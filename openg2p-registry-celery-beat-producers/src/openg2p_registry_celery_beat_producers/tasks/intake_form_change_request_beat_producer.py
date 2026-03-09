import logging
from typing import List

from openg2p_registry_core.models import (
    G2PIntakeForm,
    IntakeFormStatusEnum,
    ChangeRequestStatusEnum,
    ApprovalStatusEnum
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


@celery_app.task(name="intake_form_change_request_beat_producer")
def intake_form_change_request_beat_producer():
    """
    Beat producer that finds FINAL intake_forms with PENDING change_request_submission_status
    and queues them to the intake_form_change_request_worker for creating change requests.
    """
    _logger.info("Checking for pending intake_form change request submissions")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        # Fetch intake_forms where:
        # - intake_form_status = FINAL
        # - approval_status = APPROVED
        # - change_request_submission_status = PENDING
        pending_intake_forms: List[G2PIntakeForm] = (
            session.execute(
                select(G2PIntakeForm)
                .filter(
                    G2PIntakeForm.intake_form_status == IntakeFormStatusEnum.FINAL.value,
                    G2PIntakeForm.approval_status == ApprovalStatusEnum.APPROVED.value,
                    G2PIntakeForm.change_request_submission_status == ChangeRequestStatusEnum.PENDING.value
                )
                .limit(_config.no_of_tasks_to_process)
            )
            .scalars()
            .all()
        )
        _logger.info(f"Found {len(pending_intake_forms)} PENDING intake_form change request submissions")

        for intake_form in pending_intake_forms:
            _logger.info(f"Queueing intake_form {intake_form.intake_form_id} for change request creation")

            # Update status to PROCESSING
            intake_form.change_request_submission_status = ChangeRequestStatusEnum.PROCESSING.value
            session.add(intake_form)

            _logger.info(
                f"Updating change_request_submission_status to PROCESSING for intake_form: {intake_form.intake_form_id}"
            )

            # Send task to celery worker
            celery_app.send_task(
                Workers.INTAKE_FORM_CHANGEREQUEST_WORKER,
                args=(intake_form.intake_form_id,),
                queue=_config.worker_queue,
            )
            _logger.info(
                f"Sent task to {Workers.INTAKE_FORM_CHANGEREQUEST_WORKER} for intake_form: {intake_form.intake_form_id}"
            )
        session.commit()

    _logger.info("Completed processing pending intake_form change request submissions")

