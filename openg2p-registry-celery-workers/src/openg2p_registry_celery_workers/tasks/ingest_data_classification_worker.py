import logging
from typing import List, Tuple

from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker
from openg2p_registry_core.models import (
    ProcessStatusEnum,
    IncomingRawData, 
    IncomingRawDataPayload,
    IncomingClassifiedData,
    IncomingModelSemanticPattern
)
from openg2p_registry_core.helpers import PatternMatcher

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="ingest_data_classification_worker")
def ingest_data_classification_worker(ingest_id: str):
    _logger.info(f"Starting ingest_data_classification_worker for ingest_id: {ingest_id}")
    session_maker = sessionmaker(
        bind=_engine, expire_on_commit=False
    )

    with session_maker() as session:
        incoming_raw_data: IncomingRawData | None = None
        try:
            incoming_raw_data = session.get(IncomingRawData, ingest_id)
            incoming_raw_data_payload = session.get(IncomingRawDataPayload, ingest_id)

            # Iterate through IncomingModelSemanticPatterns for current data_model and determine incoming register_id and opertion_id
            incoming_model_semantic_patterns: List[IncomingModelSemanticPattern] = (
                session.execute(
                    select(IncomingModelSemanticPattern).where(
                        IncomingModelSemanticPattern.data_model_id == incoming_raw_data.data_model_id
                    )
                ).scalars().all()
            )
            register_id, section_id, semantic_pattern_id = _match_model_semantic_pattern(
                incoming_model_semantic_patterns, incoming_raw_data_payload
            )

            incoming_classified_data = IncomingClassifiedData(
                ingest_id=incoming_raw_data.ingest_id,
                data_model_id=incoming_raw_data.data_model_id,
                partner_id=incoming_raw_data.partner_id,
                register_id=register_id,
                section_id=section_id,
                semantic_pattern_id=semantic_pattern_id,
                classified_date_time=func.now(),
            )
            session.add(incoming_classified_data)

            # Update incoming_raw_data classification_status -> PROCESSED
            incoming_raw_data.classification_number_of_attempts += 1
            incoming_raw_data.classification_status = ProcessStatusEnum.PROCESSED.value
            incoming_raw_data.classification_date_time = func.now()
            session.commit()

        except Exception as e:
            _logger.error(
                f"Error during processing ingest_data_classification_worker for ingest_id {ingest_id}: {str(e)}"
            )
            # Rollback all sessions
            session.rollback()

            # Retry logic if maximum attempts not exhausted
            if incoming_raw_data.classification_number_of_attempts < _config.worker_max_attempts:
                incoming_raw_data.classification_number_of_attempts += 1
                incoming_raw_data.classification_status = ProcessStatusEnum.PENDING.value
            else:
                incoming_raw_data.classification_status = ProcessStatusEnum.FAILED.value

            incoming_raw_data.classification_latest_error_code = str(e)
            incoming_raw_data.classification_date_time = func.now()
            session.commit()
            # Raise exception for testing
            raise e

        _logger.info(
            f"Completed processing ingest_data_classification_worker for ingest_id: {ingest_id}"
        )


def _match_model_semantic_pattern(
    incoming_model_semantic_patterns: List[IncomingModelSemanticPattern],
    incoming_raw_data_payload: IncomingRawDataPayload
) -> Tuple[str, str, str]:
    register_id: str = None
    section_id: str = None
    semantic_pattern_id: str = None

    pattern_matcher = PatternMatcher().get_component()
    for pattern in incoming_model_semantic_patterns:
        if pattern_matcher.validate_semantic_pattern_match(
            pattern, incoming_raw_data_payload.raw_data_json
        ):
            register_id = pattern.register_id
            section_id = pattern.section_id
            semantic_pattern_id = pattern.semantic_pattern_id
    
    if not register_id or not section_id:
        raise Exception("Ingest request payload doesn't match data model semantic patterns")
    
    return register_id, section_id, semantic_pattern_id
