import logging
from typing import List

from openg2p_registry_core.models import G2PRegisterChangeRequest, DeduplicationStatusEnum
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import Engine
from ..utils import Workers

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="deduplication_change_request_beat_producer")
def deduplication_change_request_beat_producer():
    """
    Beat producer that finds pending deduplication work for change_request records
    and queues them to the deduplication worker.
    """
    _logger.info("Checking for pending deduplication_change_request requests")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)
    
    with session_maker() as session:
        # Fetch change requests with pending change_request deduplication status
        pending_change_requests: List[G2PRegisterChangeRequest] = (
            session.execute(
                select(G2PRegisterChangeRequest)
                .filter(
                    G2PRegisterChangeRequest.deduplication_change_request_status
                    == DeduplicationStatusEnum.PENDING.value
                )
                .limit(_config.no_of_tasks_to_process)
            )
            .scalars()
            .all()
        )
        _logger.info(f"Found {len(pending_change_requests)} PENDING deduplication_change_request requests")

        for change_request in pending_change_requests:
            _logger.info(f"Queueing change_request {change_request.change_request_id} for change_request deduplication")

            # Update status to INPROGRESS
            change_request.deduplication_change_request_status = DeduplicationStatusEnum.INPROGRESS.value
            session.add(change_request)

            _logger.info(
                f"Updating status for {Workers.DEDUPLICATION_CHANGEREQUEST_WORKER} to INPROGRESS for change_request: {change_request.change_request_id}"
            )

            # Send task to celery worker
            celery_app.send_task(
                Workers.DEDUPLICATION_CHANGEREQUEST_WORKER,
                args=(change_request.change_request_id,),
                queue=_config.worker_queue,
            )
            _logger.info(
                f"Sent task to {Workers.DEDUPLICATION_CHANGEREQUEST_WORKER} for change_request: {change_request.change_request_id}"
            )
        session.commit()

    _logger.info("Completed processing pending deduplication_change_request requests")

