import logging
from datetime import datetime

from sqlalchemy.orm import sessionmaker

from openg2p_registry_core.models import (
    G2PFunctionalIdGenerationQueue,
)
from openg2p_registry_core.models.g2p_functional_id_generation_queue import (
    ProcessStatusEnum as FunctionalIdGenerationStatusEnum,
)

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="functional_id_updation_worker")
def functional_id_updation_worker(queue_id: str):
    _logger.info(f"Starting functional_id_updation_worker for queue_id: {queue_id}")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        queue_item: G2PFunctionalIdGenerationQueue | None = None
        try:
            queue_item = session.get(G2PFunctionalIdGenerationQueue, queue_id)
            if not queue_item:
                raise Exception(f"Functional ID generation queue item not found: {queue_id}")

            functional_record_id = _compose_functional_record_id(
                queue_item.resolved_prefix,
                queue_item.resolved_id,
                queue_item.resolved_suffix,
            )
            if not functional_record_id:
                raise Exception(f"Resolved functional_record_id not found for queue_id: {queue_id}")

            _notify_functional_id_used(functional_record_id=functional_record_id)

            queue_item.id_updation_status = FunctionalIdGenerationStatusEnum.COMPLETED.value
            queue_item.id_updation_no_of_attempts += 1
            queue_item.id_updation_latest_timestamp = datetime.now()
            queue_item.id_updation_latest_error_code = None

            session.add(queue_item)
            session.commit()

        except Exception as e:
            _logger.error(
                f"Error during processing functional_id_updation_worker for queue_id {queue_id}: {str(e)}"
            )
            session.rollback()

            if queue_item:
                queue_item.id_updation_no_of_attempts += 1
                queue_item.id_updation_latest_timestamp = datetime.now()
                queue_item.id_updation_latest_error_code = str(e)

                if queue_item.id_updation_no_of_attempts < _config.worker_max_attempts:
                    queue_item.id_updation_status = FunctionalIdGenerationStatusEnum.PENDING.value
                else:
                    queue_item.id_updation_status = FunctionalIdGenerationStatusEnum.FAILED.value

                session.add(queue_item)
                session.commit()

            raise e

        _logger.info(f"Completed functional_id_updation_worker for queue_id: {queue_id}")


def _notify_functional_id_used(functional_record_id: str) -> None:
    updation_url = _build_functional_id_updation_url()
    _logger.info(
        "Functional ID updation placeholder invoked for "
        f"functional_record_id={functional_record_id}, url={updation_url}"
    )
    # Placeholder for actual impl


def _build_functional_id_updation_url() -> str:
    updation_path = _config.id_generation_updation_path
    return (
        f"{_config.functional_id_generation_url.rstrip('/')}/"
        f"{updation_path.lstrip('/')}"
    )


def _compose_functional_record_id(
    resolved_prefix: str | None, resolved_id: str | None, resolved_suffix: str | None
) -> str:
    return f"{resolved_prefix or ''}{resolved_id or ''}{resolved_suffix or ''}"
