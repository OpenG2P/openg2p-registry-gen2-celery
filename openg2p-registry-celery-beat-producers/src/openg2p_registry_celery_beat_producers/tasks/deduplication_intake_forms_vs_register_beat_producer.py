import logging
from typing import List

from openg2p_registry_core.models import G2PIntakeFormSubmission, DeduplicationStatusEnum, IntakeFormStatusEnum
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import Engine
from ..utils import Workers

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="deduplication_intake_forms_vs_register_beat_producer")
def deduplication_intake_forms_vs_register_beat_producer():
    """
    Beat producer that finds intake form submissions pending deduplication against register records
    and queues them to the deduplication worker.
    """
    _logger.info("Checking for pending deduplication_intake_forms_vs_register submissions")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        pending_submissions: List[G2PIntakeFormSubmission] = (
            session.execute(
                select(G2PIntakeFormSubmission)
                .filter(
                    G2PIntakeFormSubmission.deduplication_status_vs_register
                    == DeduplicationStatusEnum.PENDING.value,
                    G2PIntakeFormSubmission.draft_status
                    == IntakeFormStatusEnum.FINAL.value,
                )
                .limit(_config.no_of_tasks_to_process)
            )
            .scalars()
            .all()
        )
        _logger.info(f"Found {len(pending_submissions)} PENDING deduplication_intake_forms_vs_register submissions")

        for submission in pending_submissions:
            _logger.info(f"Queueing submission {submission.submission_id} for intake_forms_vs_register deduplication")

            submission.deduplication_status_vs_register = DeduplicationStatusEnum.INPROGRESS.value
            session.add(submission)

            _logger.info(
                f"Updating status for {Workers.DEDUPLICATION_INTAKE_FORMS_VS_REGISTER_WORKER} to INPROGRESS for submission: {submission.submission_id}"
            )

            celery_app.send_task(
                Workers.DEDUPLICATION_INTAKE_FORMS_VS_REGISTER_WORKER,
                args=(submission.submission_id,),
                queue=_config.worker_queue,
            )
            _logger.info(
                f"Sent task to {Workers.DEDUPLICATION_INTAKE_FORMS_VS_REGISTER_WORKER} for submission: {submission.submission_id}"
            )
        session.commit()

    _logger.info("Completed processing pending deduplication_intake_forms_vs_register submissions")
