import logging
from typing import List

from openg2p_registry_core.models import OutgoingTopic, ProcessStatusEnum
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import Engine
from ..utils import Workers

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="outgest_topic_register_beat_producer")
def outgest_topic_register_beat_producer():
    _logger.info("Checking for pending outgoing_topics registration requests")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)
    
    with session_maker() as session:
        # Fetch rows with pending status
        outgoing_topics: List[OutgoingTopic] = (
            session.execute(
                select(OutgoingTopic)
                .filter(
                    OutgoingTopic.websub_register_status
                    == ProcessStatusEnum.PENDING.value
                )
                .limit(_config.no_of_tasks_to_process)
            )
            .scalars()
            .all()
        )
        _logger.info(f"Found {len(outgoing_topics)} pending outgoing_topics registration requests")

        for outgoing_topic in outgoing_topics:
            _logger.info(f"Queueing outgoing_topic with topic_id: {outgoing_topic.topic_id} for registration")

            outgoing_topic.websub_register_status = ProcessStatusEnum.PROCESSING.value
            session.add(outgoing_topic)

            _logger.info(
                f"Updating status for {Workers.OUTGEST_TOPIC_REGISTER_WORKER} to processing for outgoing_topic with topic_id: {outgoing_topic.topic_id}"
            )

            # Send task to appropriate celery worker
            celery_app.send_task(
                Workers.OUTGEST_TOPIC_REGISTER_WORKER,
                args=(outgoing_topic.topic_id,),
                queue=_config.worker_queue,
            )
            _logger.info(
                f"Sent task to {Workers.OUTGEST_TOPIC_REGISTER_WORKER} for outgoing_topic with topic_id: {outgoing_topic.topic_id}"
            )
        session.commit()

    _logger.info("Completed processing pending outgoing_topics registration requests")
