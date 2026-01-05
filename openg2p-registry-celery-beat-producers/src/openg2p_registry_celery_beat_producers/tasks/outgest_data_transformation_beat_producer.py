import logging
from typing import List

from openg2p_registry_core.models import OutgoingRawData, ProcessStatusEnum
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import Engine
from ..utils import Workers

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="outgest_data_transformation_beat_producer")
def outgest_data_transformation_beat_producer():
    _logger.info("Checking for pending outgoing_raw_data tranformation requests")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)
    
    with session_maker() as session:
        # Fetch rows with pending status
        outgoing_raw_data: List[OutgoingRawData] = (
            session.execute(
                select(OutgoingRawData)
                .filter(
                    OutgoingRawData.transformation_status
                    == ProcessStatusEnum.PENDING.value
                )
                .limit(_config.no_of_tasks_to_process)
            )
            .scalars()
            .all()
        )
        _logger.info(f"Found {len(outgoing_raw_data)} pending outgoing_raw_data transformation requests")

        for outgoing_raw_datum in outgoing_raw_data:
            _logger.info(f"Queueing outgoing_raw_data with outgest_id: {outgoing_raw_datum.outgest_id} for enrichment and transformation")

            outgoing_raw_datum.transformation_status = ProcessStatusEnum.PROCESSING.value
            session.add(outgoing_raw_datum)

            _logger.info(
                f"Updating status for {Workers.OUTGEST_DATA_TRANSFORMATION_WORKER} to processing for outgoing_raw_data with outgest_id: {outgoing_raw_datum.outgest_id}"
            )

            # Send task to appropriate celery worker
            celery_app.send_task(
                Workers.OUTGEST_DATA_TRANSFORMATION_WORKER,
                args=(outgoing_raw_datum.outgest_id,),
                queue=_config.worker_queue,
            )
            _logger.info(
                f"Sent task to {Workers.OUTGEST_DATA_TRANSFORMATION_WORKER} for outgoing_raw_data with outgest_id: {outgoing_raw_datum.outgest_id}"
            )
        session.commit()

    _logger.info("Completed processing pending outgoing_raw_data transformation requests")
