import logging
import asyncio
import uuid
from datetime import datetime

from openg2p_registry_core.schemas import ChangePayload, ChangeRequestRequestPayload
from openg2p_registry_core.services import (
    G2PChangeRequestWorkerService,
    G2PRegisterChangeRequestService,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import Session, sessionmaker
from openg2p_registry_core.models import (
    ProcessStatusEnum, 
    G2PRegisterDefinition,
    G2PRegisterSection,
    G2PRegisterUITabSection,
    IncomingClassifiedData, 
    IncomingEnrichedTransformedData,
    G2PRegisterChangeRequest
)

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()
_async_engine = Engine.get_async_engine()
_worker_loop: asyncio.AbstractEventLoop | None = None

# Register component services used by G2PChangeRequestWorkerService. Celery
# workers can import tasks before the FastAPI app initializer has run.
G2PRegisterChangeRequestService()
G2PChangeRequestWorkerService()


def _worker_event_loop() -> asyncio.AbstractEventLoop:
    """Return one persistent asyncio loop per Celery worker process.

    ``G2PRegisterService`` uses a module-global async SQLAlchemy engine
    (`dbengine.get()`). Re-creating a fresh event loop for each Celery task can
    leave that engine mid-operation on a previous loop, which then surfaces as
    ``asyncpg.InterfaceError: another operation is in progress`` on later tasks.

    Keeping one loop per worker process matches the connector worker strategy and
    avoids cross-loop reuse of the same async engine/connection state.
    """
    global _worker_loop
    if _worker_loop is None:
        _worker_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_worker_loop)
    return _worker_loop

@celery_app.task(name="ingest_data_worker")
def ingest_data_worker(ingest_id: str):
    _logger.info(f"Starting ingest_data_worker for ingest_id: {ingest_id}")
    session_maker = sessionmaker(
        bind=_engine, expire_on_commit=False
    )

    loop = _worker_event_loop()

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

            change_request_id: str = loop.run_until_complete(
                _process_change_request_async(
                    change_request_request_payload,
                    incoming_classified_data.partner_id
                )
            )

            # Update incoming_classified_data ingestion_status -> PROCESSED
            incoming_classified_data.change_request_id = change_request_id
            incoming_classified_data.ingestion_number_of_attempts += 1
            incoming_classified_data.ingestion_status = ProcessStatusEnum.PROCESSED.value
            incoming_classified_data.ingestion_date_time = datetime.now()
            session.commit()

        except Exception as e:
            _logger.error(
                f"Error during processing ingest_data_worker for ingest_id {ingest_id}: {str(e)}"
            )
            session.rollback()

            if incoming_classified_data is None:
                # We never managed to load the row (e.g. transient DB outage).
                # Nothing to update; let the retry handling happen on the next beat tick.
                raise e

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
    g2p_register_section = session.get(G2PRegisterSection, incoming_classified_data.section_id)
    tab_id = _resolve_tab_id(
        incoming_classified_data.register_id,
        incoming_classified_data.section_id,
        session,
    )

    transformed_data = incoming_enriched_transformed_data.transformed_data_json or {}
    if not isinstance(transformed_data, dict):
        transformed_data = {"value": transformed_data}

    internal_record_id = transformed_data.get("internal_record_id") or str(uuid.uuid4())
    change_payload_data = {
        key: value
        for key, value in transformed_data.items()
        if key != "edit_action"
    }
    change_payload_data["internal_record_id"] = internal_record_id
    # Master register sections currently allow UPDATE/NO_CHANGE only.
    change_payload_data["edit_action"] = "UPDATE"

    return ChangeRequestRequestPayload(
        register_id=incoming_classified_data.register_id,
        register_mnemonic=g2p_register_definition.register_mnemonic,
        tab_id=tab_id,
        section_id=incoming_classified_data.section_id,
        section_register_id=g2p_register_section.section_register_id,
        internal_record_id=internal_record_id,
        change_payload=[ChangePayload(**change_payload_data)]
    )


def _resolve_tab_id(register_id: str, section_id: str, session: Session) -> str:
    tab_id = session.execute(
        select(G2PRegisterUITabSection.tab_id)
        .where(
            G2PRegisterUITabSection.register_id == register_id,
            G2PRegisterUITabSection.section_id == section_id,
        )
        .order_by(G2PRegisterUITabSection.section_order)
        .limit(1)
    ).scalar()

    if not tab_id:
        tab_id = session.execute(
            select(G2PRegisterUITabSection.tab_id)
            .where(G2PRegisterUITabSection.section_id == section_id)
            .order_by(G2PRegisterUITabSection.section_order)
            .limit(1)
        ).scalar()

    if not tab_id:
        raise Exception(
            f"No tab mapping found for section_id={section_id}, register_id={register_id}"
        )

    return tab_id


async def _process_change_request_async(change_request_request_payload: ChangeRequestRequestPayload, partner_id: str) -> str:
    session_maker = async_sessionmaker(_async_engine, expire_on_commit=False)
    async with session_maker() as session:
        change_request_service = (
            G2PChangeRequestWorkerService.get_component()
            or G2PChangeRequestWorkerService()
        )
        g2p_register_change_request: G2PRegisterChangeRequest = await change_request_service.create_change_request(
            change_request_request_payload=change_request_request_payload,
            session=session,
            source_partner_id=partner_id,
        )
        await session.commit()
        await session.refresh(g2p_register_change_request)
        return g2p_register_change_request.change_request_id
