import asyncio
import logging
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from openg2p_registry_core.interfaces import G2PScoreComputeFactory
from openg2p_registry_core.models import (
    G2PRegisterScore,
    G2PRegisterScoreContributingAttribute,
    G2PRegisterScoreDefinition,
    G2PRegisterScoreHistory,
    G2PScoreComputeQueue,
)
from openg2p_registry_core.models.g2p_score_compute_queue import (
    ProcessStatusEnum as ScoreComputeStatusEnum,
)

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()

# Create a new event loop for async compute_score execution.
_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_loop)


@celery_app.task(name="score_compute_worker", bind=True, max_retries=3)
def score_compute_worker(self, score_compute_queue_id: str):
    _logger.info(f"Starting score_compute_worker for score_compute_queue_id: {score_compute_queue_id}")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        score_compute_queue_item: G2PScoreComputeQueue | None = None
        try:
            score_compute_queue_item = session.get(G2PScoreComputeQueue, score_compute_queue_id)
            if not score_compute_queue_item:
                raise Exception(f"Score compute queue item not found: {score_compute_queue_id}")

            score_definition: G2PRegisterScoreDefinition | None = session.get(
                G2PRegisterScoreDefinition, score_compute_queue_item.score_definition_id
            )
            if not score_definition:
                raise Exception(
                    f"Score definition not found for score_definition_id: {score_compute_queue_item.score_definition_id}"
                )

            factory = G2PScoreComputeFactory.get_component()
            if not factory:
                factory = G2PScoreComputeFactory()

            compute_service = factory.get_compute_service(score_compute_queue_item.score_type)
            contributing_attributes = (
                session.execute(
                    select(G2PRegisterScoreContributingAttribute).where(
                        G2PRegisterScoreContributingAttribute.register_mnemonic
                        == score_definition.register_mnemonic,
                        G2PRegisterScoreContributingAttribute.score_type
                        == score_definition.score_type,
                    )
                )
                .scalars()
                .all()
            )
            contributing_attribute_values = (
                score_compute_queue_item.contributing_attribute_values
            )
            if not isinstance(contributing_attribute_values, dict):
                contributing_attribute_values = {}

            contributing_attribute_config = _contributing_attribute_config_from_contributing_attributes(
                contributing_attributes=contributing_attributes
            )

            computed_score = _loop.run_until_complete(
                compute_service.compute_score(
                    link_internal_record_id=score_compute_queue_item.link_internal_record_id,
                    contributing_attribute_config=contributing_attribute_config,
                    contributing_attribute_values=contributing_attribute_values,
                )
            )

            now = datetime.utcnow()

            # Upsert into g2p_register_scores
            _upsert_score(session, score_compute_queue_item, computed_score, now)
            
            # Append to score history
            _append_score_history(session, score_compute_queue_item, computed_score, now)

            # Mark queue item COMPLETED
            score_compute_queue_item.compute_status = ScoreComputeStatusEnum.COMPLETED.value
            score_compute_queue_item.compute_latest_timestamp = now
            score_compute_queue_item.compute_latest_error_code = None
            session.add(score_compute_queue_item)
            session.commit()

            _logger.info(f"Completed score_compute_worker for score_compute_queue_id: {score_compute_queue_id}")

        except Exception as e:
            _logger.error(
                f"Error during processing score_compute_worker for score_compute_queue_id {score_compute_queue_id}: {str(e)}"
            )
            session.rollback()

            if score_compute_queue_item:
                score_compute_queue_item.compute_no_of_attempts += 1
                score_compute_queue_item.compute_latest_timestamp = datetime.utcnow()
                score_compute_queue_item.compute_latest_error_code = str(e)

                if score_compute_queue_item.compute_no_of_attempts < _config.worker_max_attempts:
                    score_compute_queue_item.compute_status = ScoreComputeStatusEnum.PENDING.value
                else:
                    score_compute_queue_item.compute_status = ScoreComputeStatusEnum.FAILED.value

                session.add(score_compute_queue_item)
                session.commit()

            raise e

def _contributing_attribute_config_from_contributing_attributes(
    contributing_attributes: list[G2PRegisterScoreContributingAttribute],
) -> list[dict[str, Any]]:
    """Map DB contributing-attribute rows to config dicts (metadata only, no field values)."""
    return [
        {
            "attribute_name": contributing_attribute.attribute_name,
            "attribute_computation_required": contributing_attribute.attribute_computation_required,
            "attribute_computation_value": contributing_attribute.attribute_computation_value or {},
            "attribute_weightage": float(contributing_attribute.attribute_weightage),
        }
        for contributing_attribute in contributing_attributes
    ]



def _upsert_score(
    session,
    score_compute_queue_item: G2PScoreComputeQueue,
    computed_score: Any,
    now: datetime,
) -> None:
    """
    Upsert score into g2p_register_scores table.
    
    Args:
        session: Database session
        score_compute_queue_item: Score compute queue item
        computed_score: Computed score value
        now: Current timestamp
    """
    existing_score = (
        session.execute(
            select(G2PRegisterScore).where(
                G2PRegisterScore.link_internal_record_id
                == score_compute_queue_item.link_internal_record_id,
                G2PRegisterScore.score_type == score_compute_queue_item.score_type,
            )
        )
        .scalars()
        .one_or_none()
    )

    score_value = float(computed_score)

    if existing_score:
        existing_score.score_definition_id = score_compute_queue_item.score_definition_id
        existing_score.link_internal_record_id = score_compute_queue_item.link_internal_record_id
        existing_score.triggered_by_cr_id = score_compute_queue_item.change_request_id
        existing_score.triggered_by_submission_id = score_compute_queue_item.submission_id
        existing_score.computed_score = score_value
        existing_score.computed_at = now
        session.add(existing_score)
        return

    session.add(
        G2PRegisterScore(
            register_id=score_compute_queue_item.register_id,
            link_internal_record_id=score_compute_queue_item.link_internal_record_id,
            score_type=score_compute_queue_item.score_type,
            score_definition_id=score_compute_queue_item.score_definition_id,
            triggered_by_cr_id=score_compute_queue_item.change_request_id,
            triggered_by_submission_id=score_compute_queue_item.submission_id,
            computed_score=score_value,
            computed_at=now,
        )
    )


def _append_score_history(
    session,
    score_compute_queue_item: G2PScoreComputeQueue,
    computed_score: Any,
    now: datetime,
) -> None:
    """
    Append score to history table.
    
    Args:
        session: Database session
        score_compute_queue_item: Score compute queue item
        computed_score: Computed score value
        now: Current timestamp
    """
    session.add(
        G2PRegisterScoreHistory(
            register_id=score_compute_queue_item.register_id,
            link_internal_record_id=score_compute_queue_item.link_internal_record_id,
            computed_at=now,
            score_type=score_compute_queue_item.score_type,
            score_definition_id=score_compute_queue_item.score_definition_id,
            triggered_by_cr_id=score_compute_queue_item.change_request_id,
            triggered_by_submission_id=score_compute_queue_item.submission_id,
            computed_score=float(computed_score),
        )
    )

