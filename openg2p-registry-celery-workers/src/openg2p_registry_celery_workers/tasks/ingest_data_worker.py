import logging
import asyncio
import datetime
from asyncio import AbstractEventLoop

from openg2p_registry_core.schemas import ChangeRequestRequestPayload
from openg2p_registry_core.services import G2PRegisterService
from sqlalchemy import func
from sqlalchemy.orm import Session, sessionmaker
from openg2p_registry_core.models import (
    ProcessStatusEnum, 
    G2PRegisterDefinition,
    IncomingClassifiedData, 
    IncomingEnrichedTransformedData
)

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="ingest_data_worker")
def ingest_data_worker(ingest_id: str):
    _logger.info(f"Starting ingest_data_worker for ingest_id: {ingest_id}")
    session_maker = sessionmaker(
        bind=_engine, expire_on_commit=False
    )

    with session_maker() as session:
        incoming_classified_data: IncomingClassifiedData | None = None
        try:
            incoming_classified_data = session.get(IncomingClassifiedData, ingest_id)
            incoming_enriched_transformed_data = session.get(IncomingEnrichedTransformedData, ingest_id)
            
            change_request_request_payload: ChangeRequestRequestPayload = _construct_change_request_request_payload(
                incoming_classified_data,
                incoming_enriched_transformed_data,
                session
            )

            asyncio.run(
                _process_change_request_async(
                    change_request_request_payload,
                    incoming_classified_data.partner_id
                )
            )

            # Update incoming_classified_data ingestion_status -> PROCESSED
            incoming_classified_data.ingestion_number_of_attempts += 1
            incoming_classified_data.ingestion_status = ProcessStatusEnum.PROCESSED.value
            incoming_classified_data.ingestion_date_time = datetime.now()
            session.commit()

        except Exception as e:
            _logger.error(
                f"Error during processing ingest_data_worker for ingest_id {ingest_id}: {str(e)}"
            )
            # Rollback all sessions
            session.rollback()

            # Retry logic if maximum attempts not exhausted
            if incoming_classified_data.ingestion_number_of_attempts < _config.worker_max_attempts:
                incoming_classified_data.ingestion_number_of_attempts += 1
                incoming_classified_data.ingestion_status = ProcessStatusEnum.PENDING.value
            else:
                incoming_classified_data.ingestion_status = ProcessStatusEnum.FAILED.value

            incoming_classified_data.ingestion_latest_error_code = str(e)
            incoming_classified_data.ingestion_date_time = datetime.now()
            session.commit()
            # Raise exception for testing
            raise e

        _logger.info(
            f"Completed processing ingest_data_worker for ingest_id: {ingest_id}"
        )


def _construct_change_request_request_payload(
    incoming_classified_data: IncomingClassifiedData,
    incoming_enriched_transformed_data: IncomingEnrichedTransformedData,
    session: Session
) -> ChangeRequestRequestPayload:
    g2p_register_definition = session.get(G2PRegisterDefinition, incoming_classified_data.register_id)
    # change_payload is now a list
    return ChangeRequestRequestPayload(
        register_id=incoming_classified_data.register_id,
        register_mnemonic=g2p_register_definition.register_mnemonic,
        section_id=incoming_classified_data.section_id,
        change_payload=[incoming_enriched_transformed_data.transformed_data_json]
    )

async def _process_change_request_async(change_request_request_payload: ChangeRequestRequestPayload, partner_id: str):
    g2p_register_service = G2PRegisterService.get_component()
    await g2p_register_service.create_change_request(
        change_request_request_payload=change_request_request_payload,
        source_partner_id=partner_id
    )
