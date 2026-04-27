import importlib
import logging
from datetime import datetime

from sqlalchemy import inspect as sa_inspect, select
from sqlalchemy.orm import sessionmaker

from openg2p_registry_core.models import (
    G2PCompletionScoreComputationQueue,
    G2PRegisterDefinition,
    G2PRegisterSection,
    G2PRegisterSectionCompletionScore,
)
from openg2p_registry_core.models.g2p_functional_id_generation_queue import (
    ProcessStatusEnum,
)

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


_EXCLUDED_COLUMNS = {
    "internal_record_id",
    "link_internal_record_id",
    "link_foundational_id",
    "functional_record_id",
    "created_at",
    "created_by",
    "last_approved_at",
    "last_approved_by",
    "record_status",
}


def _get_register_class(register_mnemonic: str):
    module = importlib.import_module(
        "openg2p_registry_extensions.register_domain.models"
    )
    return getattr(module, f"G2PRegister{register_mnemonic}")


def _count_filled(record, columns) -> tuple[int, int]:
    total = 0
    filled = 0
    for col in columns:
        if col.key in _EXCLUDED_COLUMNS or col.primary_key:
            continue
        total += 1
        value = getattr(record, col.key, None)
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        filled += 1
    return filled, total


@celery_app.task(name="completion_score_worker")
def completion_score_worker(queue_id: str):
    _logger.info(f"Starting completion_score_worker for queue_id: {queue_id}")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        queue_item: G2PCompletionScoreComputationQueue | None = None
        try:
            queue_item = session.get(G2PCompletionScoreComputationQueue, queue_id)
            if not queue_item:
                raise Exception(
                    f"Completion score queue item not found: {queue_id}"
                )

            section: G2PRegisterSection | None = session.get(
                G2PRegisterSection, queue_item.section_id
            )
            if not section:
                raise Exception(
                    f"Section {queue_item.section_id} not found"
                )

            section_register_definition: G2PRegisterDefinition | None = session.get(
                G2PRegisterDefinition, section.section_register_id
            )
            if not section_register_definition:
                raise Exception(
                    f"Section register definition not found for "
                    f"register_id: {section.section_register_id}"
                )

            register_class = _get_register_class(
                section_register_definition.register_mnemonic
            )
            mapper = sa_inspect(register_class)
            columns = list(mapper.columns)

            total_fields = 0
            filled_fields = 0

            if section.is_list:
                children = (
                    session.execute(
                        select(register_class).where(
                            register_class.link_internal_record_id
                            == queue_item.internal_record_id
                        )
                    )
                    .scalars()
                    .all()
                )
                for child in children:
                    f, t = _count_filled(child, columns)
                    filled_fields += f
                    total_fields += t
            else:
                record = session.execute(
                    select(register_class).where(
                        register_class.internal_record_id
                        == queue_item.internal_record_id
                    )
                ).scalar()
                if record is None:
                    raise Exception(
                        f"Record {queue_item.internal_record_id} not found for "
                        f"section {section.section_id}"
                    )
                filled_fields, total_fields = _count_filled(record, columns)

            ratio = (filled_fields / total_fields) if total_fields > 0 else 0.0
            weightage = section.section_weightage or 0.0
            score = ratio * weightage

            existing = session.execute(
                select(G2PRegisterSectionCompletionScore).where(
                    G2PRegisterSectionCompletionScore.register_id == queue_item.register_id,
                    G2PRegisterSectionCompletionScore.internal_record_id == queue_item.internal_record_id,
                    G2PRegisterSectionCompletionScore.section_id == queue_item.section_id,
                )
            ).scalar()

            now = datetime.utcnow()
            if existing:
                existing.computed_section_completion_score = score
                existing.computed_timestamp = now
                session.add(existing)
            else:
                session.add(
                    G2PRegisterSectionCompletionScore(
                        register_id=queue_item.register_id,
                        internal_record_id=queue_item.internal_record_id,
                        section_id=queue_item.section_id,
                        computed_section_completion_score=score,
                        computed_timestamp=now,
                    )
                )

            queue_item.compute_status = ProcessStatusEnum.COMPLETED.value
            queue_item.compute_processed_timestamp = now
            queue_item.compute_number_of_attempts = (
                queue_item.compute_number_of_attempts or 0
            ) + 1
            queue_item.compute_latest_error_code = None
            session.add(queue_item)
            session.commit()

        except Exception as e:
            _logger.error(
                f"Error during processing completion_score_worker for queue_id {queue_id}: {str(e)}"
            )
            session.rollback()

            if queue_item:
                queue_item.compute_number_of_attempts = (
                    queue_item.compute_number_of_attempts or 0
                ) + 1
                queue_item.compute_processed_timestamp = datetime.utcnow()
                queue_item.compute_latest_error_code = str(e)

                if queue_item.compute_number_of_attempts < _config.worker_max_attempts:
                    queue_item.compute_status = ProcessStatusEnum.PENDING.value
                else:
                    queue_item.compute_status = ProcessStatusEnum.FAILED.value

                session.add(queue_item)
                session.commit()

            raise e

        _logger.info(
            f"Completed completion_score_worker for queue_id: {queue_id}"
        )
