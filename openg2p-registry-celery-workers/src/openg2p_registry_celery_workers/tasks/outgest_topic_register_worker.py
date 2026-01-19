import logging
from httpx import HTTPStatusError
from datetime import datetime

from openg2p_registry_core.helpers import WebsubHelper
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from openg2p_registry_core.models import (
    ProcessStatusEnum, 
    OutgoingTopic,
)

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="outgest_topic_register_worker")
def outgest_topic_register_worker(topic_id: str):
    _logger.info(f"Starting outgest_topic_register_worker for topic_id: {topic_id}")
    session_maker = sessionmaker(
        bind=_engine, expire_on_commit=False
    )

    with session_maker() as session:
        outgoing_topic: OutgoingTopic | None = None
        try:
            outgoing_topic = session.get(OutgoingTopic, topic_id)

            _register_websub_topic(outgoing_topic)

            # Update outgoing_raw_data publish_status -> PROCESSED
            outgoing_topic.websub_register_number_of_attempts += 1
            outgoing_topic.websub_register_status = ProcessStatusEnum.PROCESSED.value
            outgoing_topic.websub_register_datetime = datetime.now()
            session.commit()

        except Exception as e:
            _logger.error(
                f"Error during processing outgest_topic_register_worker for topic_id {topic_id}: {str(e)}"
            )
            # Rollback all sessions
            session.rollback()

            # Retry logic if maximum attempts not exhausted
            if outgoing_topic.websub_register_number_of_attempts < _config.worker_max_attempts:
                outgoing_topic.websub_register_number_of_attempts += 1
                outgoing_topic.websub_register_status = ProcessStatusEnum.PENDING.value
            else:
                outgoing_topic.websub_register_status = ProcessStatusEnum.FAILED.value

            outgoing_topic.websub_register_latest_error_code = str(e)
            outgoing_topic.websub_register_datetime = datetime.now()
            session.commit()
            # Raise exception for testing
            raise e

        _logger.info(
            f"Completed processing outgest_topic_register_worker for topic_id: {topic_id}"
        )


def _register_websub_topic(
    outgoing_topic: OutgoingTopic
):
    websub_helper = WebsubHelper().get_component()
    try:
        websub_helper.register_topic(outgoing_topic.websub_topic)
    except HTTPStatusError as http_e:
        _logger.error(f"Error during registration of topic {outgoing_topic.websub_topic} to websub: {str(http_e)}")
        raise http_e