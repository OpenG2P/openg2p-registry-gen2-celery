import logging
from typing import Dict, Optional
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker
from openg2p_registry_core.models import (
    ProcessStatusEnum,
    OutgoingTemplate,
    OutgoingRawData,
    OutgoingRawDataPayload,
    OutgoingTransformedDataPayload,
)
from openg2p_registry_core.helpers import TemplateHelper, MinioClient

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="outgest_data_transformation_worker")
def outgest_data_transformation_worker(outgest_id: str):
    _logger.info(f"Starting outgest_data_transformation_worker for outgest_id: {outgest_id}")
    session_maker = sessionmaker(
        bind=_engine, expire_on_commit=False
    )

    with session_maker() as session:
        outgoing_raw_data: OutgoingRawData | None = None
        try:
            outgoing_raw_data = session.get(OutgoingRawData, outgest_id)
            outgoing_raw_data_payload = session.get(OutgoingRawDataPayload, outgoing_raw_data.payload_id)

            transformed_data_json: Dict = _transform_outgoing_raw_data_json(
                outgoing_raw_data,
                outgoing_raw_data_payload,
                session
            )
            outgoing_transformed_data_payload: OutgoingTransformedDataPayload = _construct_outgoing_transformed_data_payload(
                outgoing_raw_data,
                transformed_data_json,
            )
            session.add(outgoing_transformed_data_payload)

            # Update incoming_classified_data transformation_status -> PROCESSED
            outgoing_raw_data.transformation_number_of_attempts += 1
            outgoing_raw_data.transformation_status = ProcessStatusEnum.PROCESSED.value
            outgoing_raw_data.transformation_datetime = datetime.now()

            # Update incoming_classified_data publish_status -> PENDING
            outgoing_raw_data.publish_status = ProcessStatusEnum.PENDING.value
            session.commit()

        except Exception as e:
            _logger.error(
                f"Error during processing outgoing_raw_data for outgest_id {outgest_id}: {str(e)}"
            )
            # Rollback all sessions
            session.rollback()

            # Retry logic if maximum attempts not exhausted
            if outgoing_raw_data.transformation_number_of_attempts < _config.worker_max_attempts:
                outgoing_raw_data.transformation_number_of_attempts += 1
                outgoing_raw_data.transformation_status = ProcessStatusEnum.PENDING.value
            else:
                outgoing_raw_data.transformation_status = ProcessStatusEnum.FAILED.value

            outgoing_raw_data.transformation_latest_error_code = str(e)
            outgoing_raw_data.transformation_datetime = datetime.now()
            session.commit()
            # Raise exception for testing
            raise e

        _logger.info(
            f"Completed processing outgest_data_transformation_worker for outgest_id: {outgest_id}"
        )


def _construct_outgoing_transformed_data_payload(
    outgoing_raw_data: OutgoingRawData,
    transformed_data_json: Dict,
) -> OutgoingTransformedDataPayload:
    outgoing_transformed_data_payload = OutgoingTransformedDataPayload(
        outgest_id=outgoing_raw_data.outgest_id,
        payload_id=outgoing_raw_data.payload_id,
        change_request_id=outgoing_raw_data.change_request_id,
        intake_form_submission_id=outgoing_raw_data.intake_form_submission_id,
        transformed_data_json=transformed_data_json,
    )
    return outgoing_transformed_data_payload

def _transform_outgoing_raw_data_json(
    outgoing_raw_data: OutgoingRawData,
    outgoing_raw_data_payload: OutgoingRawDataPayload,
    session: Session
) -> Dict:
    outgoing_template: OutgoingTemplate | None = session.execute(
        select(OutgoingTemplate).filter_by(
            data_model_id=outgoing_raw_data.data_model_id,
            register_id=outgoing_raw_data.register_id
        )
    ).scalar_one_or_none()
    if not outgoing_template:
        raise Exception(
            f"Template not found data_model_id {outgoing_raw_data.data_model_id}, register_id {outgoing_raw_data.register_id} combination"
        )

    minio_client = MinioClient.get_component()
    template_helper = TemplateHelper.get_component()

    transformed_data_json: Dict = template_helper.render_with_template(
        minio_client=minio_client,
        template_file_id=outgoing_template.template_file_id,
        data=outgoing_raw_data_payload.raw_data_json,
        expand_data=False,
    )
    return transformed_data_json
