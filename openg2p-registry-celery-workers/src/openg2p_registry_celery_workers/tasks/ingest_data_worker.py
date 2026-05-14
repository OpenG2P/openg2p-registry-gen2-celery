import asyncio
import logging
import uuid
from copy import deepcopy
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from openg2p_registry_core.models import (
    ChangeRequestSourceEnum,
    G2PIntakeFormSubmission,
    G2PIntakeFormUITab,
    G2PIntakeFormUITabSection,
    G2PRegisterSection,
    IncomingClassifiedData,
    IncomingEnrichedTransformedData,
    IntakeFormStatusEnum,
    PipelineActionEnum,
    ProcessStatusEnum,
)
from openg2p_registry_core.services import G2PIntakeFormDataService

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_async_engine = Engine.get_async_engine()

_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_loop)
_INGESTION_CREATED_BY = "system"


async def _process_ingestion_async(ingest_id: str) -> None:
    session_maker = async_sessionmaker(bind=_async_engine, expire_on_commit=False)

    try:
        async with session_maker() as session:
            incoming_classified_data = await session.get(IncomingClassifiedData, ingest_id)
            if incoming_classified_data is None:
                raise ValueError(f"Incoming classified data not found for ingest_id '{ingest_id}'")

            if (
                incoming_classified_data.pipeline_action or PipelineActionEnum.ADD.value
            ) == PipelineActionEnum.UPDATE.value:
                raise ValueError("INVALID_PIPELINE_ACTION_FOR_INGEST_DATA_WORKER")

            incoming_enriched_transformed_data = await session.get(IncomingEnrichedTransformedData, ingest_id)
            if incoming_enriched_transformed_data is None:
                raise ValueError(f"Incoming transformed data not found for ingest_id '{ingest_id}'")

            ordered_sections: list[G2PRegisterSection] = await _get_ordered_form_sections(
                incoming_classified_data.intake_form_id,
                session,
            )
            transformed_data: dict[str, list[dict]] = _validate_transformed_data_json(
                incoming_classified_data,
                incoming_enriched_transformed_data,
                ordered_sections,
            )

            submission_id, already_processed = await _prepare_submission_for_ingestion(
                incoming_classified_data,
                session,
            )
            if already_processed:
                _logger.info(
                    "Skipping ingest_id %s because it already points to finalized submission %s",
                    ingest_id,
                    submission_id,
                )
                return

            if submission_id is None:
                submission_id = await _create_submission_async(incoming_classified_data, session)
                incoming_classified_data.intake_form_submission_id = submission_id

            await _save_sections_async(
                submission_id,
                incoming_classified_data,
                ordered_sections,
                transformed_data,
                session,
            )
            await _finalize_submission_async(submission_id, session)

            incoming_classified_data.ingestion_number_of_attempts += 1
            incoming_classified_data.ingestion_status = ProcessStatusEnum.PROCESSED.value
            incoming_classified_data.ingestion_latest_error_code = None
            incoming_classified_data.ingestion_date_time = datetime.now()
            session.add(incoming_classified_data)
            await session.commit()
    except Exception as error:
        _logger.error(
            "Error during processing ingest_data_worker for ingest_id %s: %s",
            ingest_id,
            error,
        )
        await _mark_ingestion_failure_async(ingest_id, str(error), session_maker)
        raise


@celery_app.task(name="ingest_data_worker")
def ingest_data_worker(ingest_id: str):
    _logger.info(f"Starting ingest_data_worker for ingest_id: {ingest_id}")
    _loop.run_until_complete(_process_ingestion_async(ingest_id))
    _logger.info(
        f"Completed processing ingest_data_worker for ingest_id: {ingest_id}"
    )


async def _prepare_submission_for_ingestion(
    incoming_classified_data: IncomingClassifiedData,
    session,
) -> tuple[str | None, bool]:
    submission_id = incoming_classified_data.intake_form_submission_id
    if not submission_id:
        return None, False

    existing_submission = await session.get(G2PIntakeFormSubmission, submission_id)
    if existing_submission is None:
        incoming_classified_data.intake_form_submission_id = None
        await session.flush()
        return None, False

    if (
        incoming_classified_data.ingestion_status == ProcessStatusEnum.PROCESSED.value
        and existing_submission.draft_status == IntakeFormStatusEnum.FINAL.value
    ):
        _logger.info(
            "Ingestion %s already linked to finalized submission %s",
            incoming_classified_data.ingest_id,
            submission_id,
        )
        return submission_id, True

    if existing_submission.draft_status == IntakeFormStatusEnum.DRAFT.value:
        await _delete_submission_async(submission_id, session)
        incoming_classified_data.intake_form_submission_id = None
        await session.flush()
        return None, False

    raise ValueError(
        f"Ingestion '{incoming_classified_data.ingest_id}' is already linked to finalized "
        f"submission '{submission_id}' and cannot create a duplicate."
    )


async def _get_ordered_form_sections(form_id: str, session) -> list[G2PRegisterSection]:
    sections = (
        await session.execute(
            select(G2PRegisterSection)
            .join(
                G2PIntakeFormUITabSection,
                G2PRegisterSection.section_id == G2PIntakeFormUITabSection.section_id,
            )
            .join(
                G2PIntakeFormUITab,
                G2PIntakeFormUITabSection.tab_id == G2PIntakeFormUITab.tab_id,
            )
            .where(G2PIntakeFormUITab.form_id == form_id)
            .order_by(
                G2PIntakeFormUITab.tab_order.asc(),
                G2PIntakeFormUITabSection.section_order.asc(),
                G2PRegisterSection.section_id.asc(),
            )
        )
    ).scalars().all()

    if not sections:
        raise ValueError(f"No sections found for intake form '{form_id}'")
    return sections


def _validate_transformed_data_json(
    incoming_classified_data: IncomingClassifiedData,
    incoming_enriched_transformed_data: IncomingEnrichedTransformedData,
    ordered_sections: list[G2PRegisterSection],
) -> dict[str, list[dict]]:
    transformed_data_json = incoming_enriched_transformed_data.transformed_data_json

    if not isinstance(transformed_data_json, dict):
        raise ValueError(
            "Transformed data must be a dict keyed by section_mnemonic, "
            f"got {type(transformed_data_json).__name__}"
        )
    if not transformed_data_json:
        raise ValueError("Transformed data is empty")

    sections_by_mnemonic = {
        section.section_mnemonic: section
        for section in ordered_sections
    }
    unknown_mnemonics = sorted(
        mnemonic
        for mnemonic in transformed_data_json.keys()
        if mnemonic not in sections_by_mnemonic
    )
    if unknown_mnemonics:
        raise ValueError(
            "Transformed data contains section mnemonics not present in intake form "
            f"'{incoming_classified_data.intake_form_id}': {', '.join(unknown_mnemonics)}"
        )

    normalized_transformed_data: dict[str, list[dict]] = {}
    subject_section_present = False

    for mnemonic, payload in transformed_data_json.items():
        if not isinstance(payload, list):
            raise ValueError(
                f"Section '{mnemonic}' payload must be a list, got {type(payload).__name__}"
            )
        if any(not isinstance(record, dict) for record in payload):
            raise ValueError(
                f"Section '{mnemonic}' payload records must all be objects"
            )

        section = sections_by_mnemonic[mnemonic]
        normalized_records = [deepcopy(record) for record in payload]
        normalized_transformed_data[mnemonic] = normalized_records

        if section.section_register_id == incoming_classified_data.register_id:
            if section.is_list:
                raise ValueError(
                    f"Subject register section '{mnemonic}' cannot be configured as a list section"
                )
            if len(normalized_records) != 1:
                raise ValueError(
                    f"Subject register section '{mnemonic}' must contain exactly one record"
                )
            subject_section_present = True

    if not subject_section_present:
        raise ValueError(
            "Transformed data must include at least one subject-register section for "
            f"register_id '{incoming_classified_data.register_id}'"
        )

    return normalized_transformed_data


async def _create_submission_async(incoming_classified_data: IncomingClassifiedData, session) -> str:
    submission = await G2PIntakeFormDataService().get_component().create_submission_with_session(
        form_id=incoming_classified_data.intake_form_id,
        register_id=incoming_classified_data.register_id,
        submission_source=ChangeRequestSourceEnum.PARTNER.value,
        partner_id=incoming_classified_data.partner_id,
        section_payloads=None,
        created_by=_INGESTION_CREATED_BY,
        session=session,
    )
    return submission.submission_id


async def _save_sections_async(
    submission_id: str,
    incoming_classified_data: IncomingClassifiedData,
    ordered_sections: list[G2PRegisterSection],
    transformed_data: dict[str, list[dict]],
    session,
) -> None:
    records_by_section_register_id: dict[str, list[dict]] = {}
    ids_by_section_register_id: dict[str, list[str]] = {}

    for section in ordered_sections:
        incoming_records = transformed_data.get(section.section_mnemonic)
        if incoming_records is None:
            continue

        merged_records = _merge_section_records(
            section=section,
            subject_register_id=incoming_classified_data.register_id,
            incoming_records=incoming_records,
            accumulated_records=records_by_section_register_id.get(section.section_register_id, []),
            accumulated_ids=ids_by_section_register_id.get(section.section_register_id, []),
        )
        records_by_section_register_id[section.section_register_id] = merged_records
        ids_by_section_register_id[section.section_register_id] = [
            record["internal_record_id"] for record in merged_records
        ]

        await G2PIntakeFormDataService().get_component().save_intake_form_submission_with_session(
            submission_id=submission_id,
            section_id=section.section_id,
            section_payload=deepcopy(merged_records),
            section_register_id=section.section_register_id,
            form_id=incoming_classified_data.intake_form_id,
            register_id=incoming_classified_data.register_id,
            created_by=_INGESTION_CREATED_BY,
            session=session,
        )


async def _mark_ingestion_failure_async(ingest_id: str, error_message: str, session_maker) -> None:
    async with session_maker() as session:
        incoming_classified_data = await session.get(IncomingClassifiedData, ingest_id)
        if incoming_classified_data is None:
            return

        if incoming_classified_data.ingestion_number_of_attempts < _config.worker_max_attempts:
            incoming_classified_data.ingestion_number_of_attempts += 1
            incoming_classified_data.ingestion_status = ProcessStatusEnum.PENDING.value
        else:
            incoming_classified_data.ingestion_status = ProcessStatusEnum.FAILED.value

        incoming_classified_data.ingestion_latest_error_code = error_message
        incoming_classified_data.ingestion_date_time = datetime.now()
        session.add(incoming_classified_data)
        await session.commit()


def _merge_section_records(
    section: G2PRegisterSection,
    subject_register_id: str,
    incoming_records: list[dict],
    accumulated_records: list[dict],
    accumulated_ids: list[str],
) -> list[dict]:
    normalized_records = [deepcopy(record) for record in incoming_records]

    if not accumulated_records:
        for record in normalized_records:
            record["internal_record_id"] = record.get("internal_record_id") or str(uuid.uuid4())
        return normalized_records

    merged_records = [deepcopy(record) for record in accumulated_records]
    record_index_by_id = {
        record["internal_record_id"]: index
        for index, record in enumerate(merged_records)
        if record.get("internal_record_id")
    }

    if section.section_register_id == subject_register_id:
        return _merge_subject_section_records(
            section,
            normalized_records,
            merged_records,
        )

    if not section.is_list:
        return _merge_single_record_section(
            section,
            normalized_records,
            merged_records,
            record_index_by_id,
        )

    return _merge_list_section_records(
        section,
        normalized_records,
        merged_records,
        accumulated_ids,
        record_index_by_id,
    )


def _merge_subject_section_records(
    section: G2PRegisterSection,
    incoming_records: list[dict],
    accumulated_records: list[dict],
) -> list[dict]:
    if section.is_list:
        raise ValueError(
            f"Subject register section '{section.section_mnemonic}' cannot be a list section"
        )
    if len(incoming_records) != 1:
        raise ValueError(
            f"Subject register section '{section.section_mnemonic}' must contain exactly one record"
        )

    target_id = accumulated_records[0]["internal_record_id"]
    incoming_record = incoming_records[0]
    incoming_record["internal_record_id"] = target_id
    accumulated_records[0] = _merge_record_data(accumulated_records[0], incoming_record)
    return accumulated_records


def _merge_single_record_section(
    section: G2PRegisterSection,
    incoming_records: list[dict],
    accumulated_records: list[dict],
    record_index_by_id: dict[str, int],
) -> list[dict]:
    if len(incoming_records) != 1:
        raise ValueError(
            f"Section '{section.section_mnemonic}' must contain exactly one record"
        )

    incoming_record = incoming_records[0]
    explicit_id = incoming_record.get("internal_record_id")
    if explicit_id and explicit_id not in record_index_by_id:
        raise ValueError(
            f"Section '{section.section_mnemonic}' references unknown internal_record_id '{explicit_id}'"
        )

    target_index = record_index_by_id.get(explicit_id, 0)
    target_id = accumulated_records[target_index]["internal_record_id"]
    incoming_record["internal_record_id"] = target_id
    accumulated_records[target_index] = _merge_record_data(
        accumulated_records[target_index],
        incoming_record,
    )
    return accumulated_records


def _merge_list_section_records(
    section: G2PRegisterSection,
    incoming_records: list[dict],
    accumulated_records: list[dict],
    accumulated_ids: list[str],
    record_index_by_id: dict[str, int],
) -> list[dict]:
    has_explicit_ids = any(record.get("internal_record_id") for record in incoming_records)
    if not has_explicit_ids and len(incoming_records) < len(accumulated_records):
        raise ValueError(
            f"Section '{section.section_mnemonic}' cannot be matched safely: "
            "fewer list rows were provided than were already accumulated for the same register"
        )

    claimed_ids: set[str] = set()

    for index, incoming_record in enumerate(incoming_records):
        explicit_id = incoming_record.get("internal_record_id")

        if explicit_id:
            if explicit_id in claimed_ids:
                raise ValueError(
                    f"Section '{section.section_mnemonic}' contains duplicate internal_record_id '{explicit_id}'"
                )
            target_id = explicit_id
            if target_id not in record_index_by_id:
                accumulated_records.append({"internal_record_id": target_id})
                record_index_by_id[target_id] = len(accumulated_records) - 1
        else:
            if index < len(accumulated_ids):
                target_id = accumulated_ids[index]
                if target_id in claimed_ids:
                    raise ValueError(
                        f"Section '{section.section_mnemonic}' could not be matched safely by index"
                    )
            else:
                target_id = str(uuid.uuid4())
                accumulated_records.append({"internal_record_id": target_id})
                record_index_by_id[target_id] = len(accumulated_records) - 1

        incoming_record["internal_record_id"] = target_id
        target_index = record_index_by_id[target_id]
        accumulated_records[target_index] = _merge_record_data(
            accumulated_records[target_index],
            incoming_record,
        )
        claimed_ids.add(target_id)

    return accumulated_records


def _merge_record_data(existing_record: dict, incoming_record: dict) -> dict:
    merged_record = deepcopy(existing_record)
    for key, value in incoming_record.items():
        if key == "edit_action":
            continue
        if value is not None:
            merged_record[key] = value
    return merged_record


async def _finalize_submission_async(submission_id: str, session) -> None:
    await G2PIntakeFormDataService.get_component().finalize_submission_with_session(
        submission_id,
        session,
    )


async def _delete_submission_async(submission_id: str, session) -> None:
    await G2PIntakeFormDataService.get_component().delete_submission_with_session(
        submission_id,
        session,
    )
