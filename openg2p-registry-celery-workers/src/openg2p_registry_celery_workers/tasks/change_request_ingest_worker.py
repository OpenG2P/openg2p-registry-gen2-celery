import asyncio
import logging
from copy import deepcopy
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from openg2p_registry_core.models import (
    ChangeRequestSourceEnum,
    G2PRegisterSection,
    G2PRegisterUITab,
    G2PRegisterUITabSection,
    IncomingClassifiedData,
    IncomingEnrichedTransformedData,
    PipelineActionEnum,
    ProcessStatusEnum,
)
from openg2p_registry_core.schemas.change_request import (
    ChangeActionEnum,
    ChangePayload,
    ChangeRequestRequestPayload,
)
from openg2p_registry_core.services.g2p_change_request_worker_service import (
    G2PChangeRequestWorkerService,
)

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_async_engine = Engine.get_async_engine()
_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_loop)


@celery_app.task(name="change_request_ingest_worker")
def change_request_ingest_worker(ingest_id: str) -> None:
    _logger.info("Starting change_request_ingest_worker for ingest_id: %s", ingest_id)
    _loop.run_until_complete(_process_change_request_ingest_async(ingest_id))
    _logger.info("Completed change_request_ingest_worker for ingest_id: %s", ingest_id)


async def _resolve_tab_id_for_section(session, register_id: str, section_id: str) -> str | None:
    result = await session.execute(
        select(G2PRegisterUITabSection.tab_id)
        .join(G2PRegisterUITab, G2PRegisterUITab.tab_id == G2PRegisterUITabSection.tab_id)
        .where(
            G2PRegisterUITabSection.register_id == register_id,
            G2PRegisterUITabSection.section_id == section_id,
            G2PRegisterUITab.register_id == register_id,
        )
        .order_by(
            G2PRegisterUITab.tab_order.asc(),
            G2PRegisterUITabSection.section_order.asc(),
        )
        .limit(1)
    )
    return result.scalar_one_or_none()


async def _build_change_request_payload_async(
    session,
    classified: IncomingClassifiedData,
    enriched: IncomingEnrichedTransformedData,
) -> ChangeRequestRequestPayload:
    if not classified.internal_record_id:
        raise ValueError("MISSING_INTERNAL_RECORD_ID")
    if not classified.section_id:
        raise ValueError("MISSING_SECTION_ID")

    section = await session.get(G2PRegisterSection, classified.section_id)
    if not section:
        raise ValueError("MISSING_SECTION_ID")

    tab_id = await _resolve_tab_id_for_section(session, classified.register_id, classified.section_id)
    if not tab_id:
        raise ValueError("CHANGE_REQUEST_VALIDATION_FAILED")

    full = enriched.transformed_data_json
    if not isinstance(full, dict):
        raise ValueError("EMPTY_SECTION_PAYLOAD")

    mnemonic = section.section_mnemonic
    section_data = full.get(mnemonic)
    if section_data is None:
        raise ValueError("SECTION_DATA_MISSING_IN_TRANSFORMED_PAYLOAD")
    if not isinstance(section_data, list) or len(section_data) == 0:
        raise ValueError("EMPTY_SECTION_PAYLOAD")

    row = deepcopy(section_data[0])
    row["edit_action"] = ChangeActionEnum.UPDATE.value
    row["internal_record_id"] = classified.internal_record_id

    return ChangeRequestRequestPayload(
        register_id=classified.register_id,
        tab_id=tab_id,
        section_id=classified.section_id,
        section_register_id=section.section_register_id,
        internal_record_id=classified.internal_record_id,
        change_payload=[ChangePayload.model_validate(row)],
    )


async def _process_change_request_ingest_async(ingest_id: str) -> None:
    session_maker = async_sessionmaker(bind=_async_engine, expire_on_commit=False)
    try:
        async with session_maker() as session:
            async with session.begin():
                classified = await session.get(IncomingClassifiedData, ingest_id)
                if classified is None:
                    raise ValueError(f"Incoming classified data not found for ingest_id '{ingest_id}'")
                if (
                    classified.pipeline_action or PipelineActionEnum.ADD.value
                ) != PipelineActionEnum.UPDATE.value:
                    raise ValueError("INVALID_PIPELINE_ACTION_FOR_CHANGE_REQUEST_WORKER")
                enriched = await session.get(IncomingEnrichedTransformedData, ingest_id)
                if enriched is None:
                    raise ValueError(f"Incoming transformed data not found for ingest_id '{ingest_id}'")

                payload = await _build_change_request_payload_async(session, classified, enriched)
                cr_service = G2PChangeRequestWorkerService.get_component()
                cr = await cr_service.create_change_request(
                    payload,
                    session,
                    source_partner_id=classified.partner_id,
                    created_by="system",
                    change_request_source=ChangeRequestSourceEnum.INGESTION_PIPELINE.value,
                )

                classified.change_request_id = cr.change_request_id
                classified.ingestion_number_of_attempts += 1
                classified.ingestion_status = ProcessStatusEnum.PROCESSED.value
                classified.ingestion_latest_error_code = None
                classified.ingestion_date_time = datetime.now()
                session.add(classified)
    except Exception as error:
        _logger.error(
            "change_request_ingest_worker failed for ingest_id %s: %s",
            ingest_id,
            error,
        )
        await _mark_change_request_ingest_failure(ingest_id, str(error), session_maker)
        raise


async def _mark_change_request_ingest_failure(
    ingest_id: str,
    error_message: str,
    session_maker,
) -> None:
    async with session_maker() as session:
        classified = await session.get(IncomingClassifiedData, ingest_id)
        if classified is None:
            return

        if classified.ingestion_number_of_attempts < _config.worker_max_attempts:
            classified.ingestion_number_of_attempts += 1
            classified.ingestion_status = ProcessStatusEnum.PENDING.value
        else:
            classified.ingestion_status = ProcessStatusEnum.FAILED.value

        classified.ingestion_latest_error_code = error_message
        classified.ingestion_date_time = datetime.now()
        session.add(classified)
        await session.commit()
