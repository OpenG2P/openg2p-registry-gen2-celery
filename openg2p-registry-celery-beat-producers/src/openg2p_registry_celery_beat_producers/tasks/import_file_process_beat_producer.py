import logging
from datetime import datetime
from typing import List

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from openg2p_registry_core.models import ImportFileProcessQueue, ProcessStatusEnum

from ..app import celery_app
from ..config import Settings
from ..engine import Engine
from ..utils import Workers

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="import_file_process_beat_producer")
def import_file_process_beat_producer():
    """
    Beat producer that finds PENDING import-file queue items
    and queues them to the import-file process worker.
    """
    _logger.info("Checking for pending import-file queue items")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        pending_queue_items: List[ImportFileProcessQueue] = (
            session.execute(
                select(ImportFileProcessQueue)
                .where(
                    ImportFileProcessQueue.intake_form_ingestion_status
                    == ProcessStatusEnum.PENDING.value
                )
                .limit(_config.no_of_tasks_to_process)
            )
            .scalars()
            .all()
        )
        _logger.info(f"Found {len(pending_queue_items)} pending import-file queue items")

        for pending_queue_item in pending_queue_items:
            # Mark as PROCESSING to avoid duplicate dispatch
            pending_queue_item.intake_form_ingestion_status = ProcessStatusEnum.PROCESSING.value
            pending_queue_item.intake_form_ingestion_attempts = (pending_queue_item.intake_form_ingestion_attempts or 0) + 1
            pending_queue_item.intake_form_ingestion_timestamp = datetime.now()
            session.add(pending_queue_item)

            _logger.info(
                f"Updating status for {Workers.IMPORT_FILE_PROCESS_WORKER} to PROCESSING for import_file_id: {pending_queue_item.import_file_id}"
            )

            # Send task to appropriate celery worker
            celery_app.send_task(
                Workers.IMPORT_FILE_PROCESS_WORKER,
                args=(pending_queue_item.import_file_id,),
                queue=_config.worker_queue,
            )
            _logger.info(
                f"Sent task to {Workers.IMPORT_FILE_PROCESS_WORKER} for import_file_id: {pending_queue_item.import_file_id}"
            )

        session.commit()
    _logger.info("Completed processing pending import-file queue items")

