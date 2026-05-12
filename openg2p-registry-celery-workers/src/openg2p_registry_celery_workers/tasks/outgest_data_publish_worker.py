import logging
from datetime import datetime
from httpx import HTTPStatusError
from typing import Dict

from openg2p_registry_core.helpers import WebsubHelper
from sqlalchemy.orm import Session, sessionmaker
from openg2p_registry_core.models import (
    ProcessStatusEnum,
    OutgoingRawData,
    OutgoingTopic,
    OutgoingTransformedDataPayload,
)

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="outgest_data_publish_worker")
def outgest_data_publish_worker(outgest_id: str):
    _logger.info(f"Starting outgest_data_publish_worker for outgest_id: {outgest_id}")
    session_maker = sessionmaker(
        bind=_engine, expire_on_commit=False
    )

    with session_maker() as session:
        outgoing_raw_data: OutgoingRawData | None = None
        try:
            outgoing_raw_data = session.get(OutgoingRawData, outgest_id)
            outgoing_transformed_data_payload = session.get(OutgoingTransformedDataPayload, outgest_id)

            _publish_content(session, outgoing_raw_data.topic_id, outgoing_transformed_data_payload.transformed_data_json)

            # Update outgoing_raw_data publish_status -> PROCESSED
            outgoing_raw_data.publish_number_of_attempts += 1
            outgoing_raw_data.publish_status = ProcessStatusEnum.PROCESSED.value
            outgoing_raw_data.publish_datetime = datetime.now()
            session.commit()

        except Exception as e:
            _logger.error(
                f"Error during processing outgest_data_publish_worker for outgest_id {outgest_id}: {str(e)}"
            )
            # Rollback all sessions
            session.rollback()

            # Retry logic if maximum attempts not exhausted
            if outgoing_raw_data.publish_number_of_attempts < _config.worker_max_attempts:
                outgoing_raw_data.publish_number_of_attempts += 1
                outgoing_raw_data.publish_status = ProcessStatusEnum.PENDING.value
            else:
                outgoing_raw_data.publish_status = ProcessStatusEnum.FAILED.value

            outgoing_raw_data.publish_latest_error_code = str(e)
            outgoing_raw_data.publish_datetime = datetime.now()
            session.commit()
            # Raise exception for testing
            raise e

        _logger.info(
            f"Completed processing outgest_data_publish_worker for outgest_id: {outgest_id}"
        )


def _publish_content(session: Session, topic_id: str, transformed_data_json: Dict):
    outgoing_topic = session.get(OutgoingTopic, topic_id)
    if not outgoing_topic:
        raise Exception(f"OutgoingTopic not found for topic_id: {topic_id}")
    websub_helper = WebsubHelper.get_component()
    try:
        websub_helper.publish(outgoing_topic.websub_topic, transformed_data_json)
    except HTTPStatusError as http_e:
        _logger.error(f"Error during publishing content to websub topic {outgoing_topic.websub_topic}: {str(http_e)}")
        raise http_e
