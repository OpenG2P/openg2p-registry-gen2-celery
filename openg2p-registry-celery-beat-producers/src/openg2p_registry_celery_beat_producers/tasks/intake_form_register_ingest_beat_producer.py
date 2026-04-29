import logging

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from openg2p_registry_core.models import (
    G2PIntakeFormSubmission,
    ProcessStatusEnum,
    ApprovalStatusEnum
)

from ..app import celery_app
from ..config import Settings
from ..engine import Engine
from ..utils import Workers

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="intake_form_register_ingest_beat_producer")
def intake_form_register_ingest_beat_producer():
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        submissions = (
            session.execute(
                select(G2PIntakeFormSubmission)
                .where(
                    G2PIntakeFormSubmission.approval_status == ApprovalStatusEnum.APPROVED.value,
                    G2PIntakeFormSubmission.register_ingest_process_status == ProcessStatusEnum.PENDING.value,
                )
                .limit(_config.no_of_tasks_to_process)
            )
            .scalars()
            .all()
        )

        for submission in submissions:
            celery_app.send_task(
                Workers.INTAKE_FORM_REGISTER_INGEST_WORKER,
                args=(submission.submission_id,),
                queue=_config.worker_queue,
            )

        session.commit()
