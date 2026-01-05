import logging
from typing import List

from openg2p_registry_core.models import IncomingRawData, ProcessStatusEnum
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import Engine
from ..utils import Workers

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="ingest_data_classification_beat_producer")
def ingest_data_classification_beat_producer():
    _logger.info("Checking for pending incoming_raw_data classification requests")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)
    
    with session_maker() as session:
        # Fetch rows with pending status
        incoming_raw_data: List[IncomingRawData] = (
            session.execute(
                select(IncomingRawData)
                .filter(
                    IncomingRawData.classification_status
                    == ProcessStatusEnum.PENDING.value
                )
                .limit(_config.no_of_tasks_to_process)
            )
            .scalars()
            .all()
        )
        _logger.info(f"Found {len(incoming_raw_data)} PENDING incoming_raw_data classification requests")

        for incoming_raw_datum in incoming_raw_data:
            _logger.info(f"Queueing incoming_raw_data with ingest_id {incoming_raw_datum.ingest_id} for classification")

            incoming_raw_datum.classification_status = ProcessStatusEnum.PROCESSING.value
            session.add(incoming_raw_datum)

            _logger.info(
                f"Updating status for {Workers.INGEST_DATA_CLASSIFICATION_WORKER} to PROCESSING for incoming_raw_data with ingest_id: {incoming_raw_datum.ingest_id}"
            )

            # Send task to appropriate celery worker
            celery_app.send_task(
                Workers.INGEST_DATA_CLASSIFICATION_WORKER,
                args=(incoming_raw_datum.ingest_id,),
                queue=_config.worker_queue,
            )
            _logger.info(
                f"Sent task to {Workers.INGEST_DATA_CLASSIFICATION_WORKER} for incoming_raw_data with ingest_id: {incoming_raw_datum.ingest_id}"
            )
        session.commit()

    _logger.info("Completed processing pending incoming_raw_data classification requests")
