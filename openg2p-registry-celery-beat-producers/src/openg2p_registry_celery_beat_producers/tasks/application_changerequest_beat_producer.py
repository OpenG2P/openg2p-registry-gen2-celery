import logging
from typing import List

from openg2p_registry_core.models import (
    G2PApplication,
    ApplicationStatusEnum,
    ChangeRequestStatusEnum
)
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import Engine
from ..utils import Workers

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="application_changerequest_beat_producer")
def application_changerequest_beat_producer():
    """
    Beat producer that finds FINAL applications with PENDING change_request_submission_status
    and queues them to the application_changerequest_worker for creating change requests.
    """
    _logger.info("Checking for pending application change request submissions")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        # Fetch applications where:
        # - application_status = FINAL
        # - change_request_submission_status = PENDING
        pending_applications: List[G2PApplication] = (
            session.execute(
                select(G2PApplication)
                .filter(
                    G2PApplication.application_status == ApplicationStatusEnum.FINAL.value,
                    G2PApplication.change_request_submission_status == ChangeRequestStatusEnum.PENDING.value
                )
                .limit(_config.no_of_tasks_to_process)
            )
            .scalars()
            .all()
        )
        _logger.info(f"Found {len(pending_applications)} PENDING application change request submissions")

        for application in pending_applications:
            _logger.info(f"Queueing application {application.application_id} for change request creation")

            # Update status to PROCESSING
            application.change_request_submission_status = ChangeRequestStatusEnum.PROCESSING.value
            session.add(application)

            _logger.info(
                f"Updating change_request_submission_status to PROCESSING for application: {application.application_id}"
            )

            # Send task to celery worker
            celery_app.send_task(
                Workers.APPLICATION_CHANGEREQUEST_WORKER,
                args=(application.application_id,),
                queue=_config.worker_queue,
            )
            _logger.info(
                f"Sent task to {Workers.APPLICATION_CHANGEREQUEST_WORKER} for application: {application.application_id}"
            )
        session.commit()

    _logger.info("Completed processing pending application change request submissions")

