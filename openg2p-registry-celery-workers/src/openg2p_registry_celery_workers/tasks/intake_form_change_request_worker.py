import asyncio
import logging
from datetime import datetime
from typing import Any, List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from openg2p_registry_core.models import (
    ChangeRequestStatusEnum,
    G2PIntakeForm,
    G2PIntakeFormSectionDocuments,
    G2PIntakeFormSectionPayload,
    G2PRegisterDefinition,
    G2PRegisterSection,
)
from openg2p_registry_core.schemas import (
    ChangePayload,
    ChangeRequestDocumentPayload,
    ChangeRequestRequestPayload,
    EditActionEnum,
)
from openg2p_registry_core.services import G2PChangeRequestWorkerService

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_async_engine = Engine.get_async_engine()

# Create a new event loop for the worker to use for asynchronous operations
_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_loop)


@celery_app.task(name="intake_form_change_request_worker")
def intake_form_change_request_worker(submission_id: str) -> None:
    """
    Worker that processes a FINAL intake_form and creates+approves change requests for each section.
    All create+approve operations are executed in a single DB transaction (all-or-nothing).
    """
    _loop.run_until_complete(_process_intake_form_submission_async(submission_id))


async def _process_intake_form_submission_async(submission_id: str) -> None:
    _logger.info(f"Starting intake_form_change_request_worker for submission_id: {submission_id}")

    async_session_maker = async_sessionmaker(bind=_async_engine, expire_on_commit=False)
    worker_service = G2PChangeRequestWorkerService()
    currently_approving_change_request_id: str | None = None

    async with async_session_maker() as session:
        try:
            async with session.begin():
                intake_form: G2PIntakeForm | None = await session.get(G2PIntakeForm, submission_id)
                if not intake_form:
                    raise Exception(f"IntakeForm not found: {submission_id}")

                section_payloads: List[G2PIntakeFormSectionPayload] = (
                    (
                        await session.execute(
                            select(G2PIntakeFormSectionPayload).where(
                                G2PIntakeFormSectionPayload.submission_id == submission_id
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                if not section_payloads:
                    raise Exception(f"No section payloads found for intake_form: {submission_id}")

                register_definition: G2PRegisterDefinition | None = await session.get(
                    G2PRegisterDefinition, intake_form.register_id
                )
                if not register_definition:
                    raise Exception(f"Register definition not found for register_id: {intake_form.register_id}")

                created_change_request_ids: List[str] = []
                to_approve_change_request_ids: List[str] = []

                _logger.info(
                    f"Creating change requests for {len(section_payloads)} section payloads: submission_id={submission_id}"
                )
                for section_payload in section_payloads:
                    section: G2PRegisterSection | None = await session.get(
                        G2PRegisterSection, section_payload.section_id
                    )
                    if not section:
                        _logger.warning(
                            f"Section not found for section_id: {section_payload.section_id}, skipping"
                        )
                        continue

                    section_edit_action = (
                        EditActionEnum.ADD.value
                        if section.is_primary_section
                        else EditActionEnum.UPDATE.value
                    )

                    change_payload_items = _build_change_payload_list(
                        section_payload=section_payload,
                        default_edit_action=section_edit_action,
                    )
                    if not change_payload_items:
                        _logger.warning(
                            "Section payload is empty for intake_form submission, skipping: "
                            f"submission_id={submission_id}, section_id={section_payload.section_id}"
                        )
                        continue

                    change_request_payload = ChangeRequestRequestPayload(
                        register_id=intake_form.register_id,
                        register_mnemonic=register_definition.register_mnemonic,
                        tab_id=section.tab_id,
                        edit_action=section_edit_action,
                        internal_record_id=intake_form.internal_record_id,
                        section_id=section.section_id,
                        section_register_id=section.section_register_id,
                        change_payload=change_payload_items,
                    )

                    section_documents = (
                        (
                            await session.execute(
                                select(G2PIntakeFormSectionDocuments).where(
                                    G2PIntakeFormSectionDocuments.submission_id == submission_id,
                                    G2PIntakeFormSectionDocuments.section_id == section_payload.section_id,
                                )
                            )
                        )
                        .scalars()
                        .all()
                    )
                    if section_documents:
                        change_request_payload.documents = [
                            ChangeRequestDocumentPayload(
                                document_label=doc.document_label,
                                document_store_id=doc.document_store_id,
                            )
                            for doc in section_documents
                        ]

                    change_request = await worker_service.create_change_request(
                        change_request_request_payload=change_request_payload,
                        session=session,
                        source_partner_id="intake_form_system",
                        submission_id=submission_id,
                    )
                    created_change_request_ids.append(change_request.change_request_id)
                    to_approve_change_request_ids.append(change_request.change_request_id)

                if not created_change_request_ids:
                    raise Exception(
                        f"No change requests were created for intake_form {submission_id}. "
                        "All section payloads were invalid or missing matching sections."
                    )

                _logger.info(
                    "Classified intake-form change requests for auto-approval: "
                    f"submission_id={submission_id}, total={len(created_change_request_ids)}, "
                    f"eligible_for_auto_approve={len(to_approve_change_request_ids)}"
                )

                approved_count = 0

                if to_approve_change_request_ids:
                    (
                        _subject_internal_record_id,
                        approved_now,
                        currently_approving_change_request_id,
                    ) = await worker_service.auto_approve_change_requests_in_order(
                        to_approve_change_request_ids,
                        session,
                        submission_id=submission_id,
                        fallback_subject_internal_record_id=intake_form.internal_record_id,
                    )
                    approved_count += approved_now

                # Status update
                intake_form.change_request_submission_status = ChangeRequestStatusEnum.PROCESSED.value
                intake_form.submission_no_of_attempts += 1
                intake_form.submission_latest_datetime = datetime.now()
                intake_form.submission_latest_error_code = None
                if created_change_request_ids:
                    intake_form.change_request_id = created_change_request_ids[0]
                session.add(intake_form)

                _logger.info(
                    f"Completed intake_form_change_request_worker for submission_id: {submission_id}, "
                    f"change_requests_total={len(created_change_request_ids)}, "
                    f"eligible_for_auto_approve={len(to_approve_change_request_ids)}, "
                    f"auto_approved_now={approved_count}"
                )

        except Exception as e:
            failed_on = (
                f", failed_change_request_id={currently_approving_change_request_id}"
                if currently_approving_change_request_id
                else ""
            )
            _logger.error(
                f"Error during intake_form_change_request_worker for submission_id {submission_id}{failed_on}: {str(e)}"
            )
            await session.rollback()

            # Restore status to PENDING for beat to retry, or FAILED when attempts exhausted.
            async with async_session_maker() as failure_session:
                async with failure_session.begin():
                    intake_form: G2PIntakeForm | None = await failure_session.get(G2PIntakeForm, submission_id)
                    if intake_form:
                        intake_form.submission_no_of_attempts += 1
                        intake_form.submission_latest_datetime = datetime.now()
                        intake_form.submission_latest_error_code = str(e)

                        if intake_form.submission_no_of_attempts >= _config.worker_max_attempts:
                            intake_form.change_request_submission_status = ChangeRequestStatusEnum.FAILED.value
                        else:
                            intake_form.change_request_submission_status = ChangeRequestStatusEnum.PENDING.value

                        failure_session.add(intake_form)

            raise


def _build_change_payload_list(
    section_payload: G2PIntakeFormSectionPayload,
    default_edit_action: str,
) -> list[ChangePayload]:
    payload_items: list[dict[str, Any]] = section_payload.intake_form_section_payload or []
    normalized_payload_items: list[ChangePayload] = []

    for payload_item in payload_items:
        payload_dict = dict(payload_item or {})
        payload_dict.setdefault("edit_action", default_edit_action)
        normalized_payload_items.append(ChangePayload(**payload_dict))

    return normalized_payload_items
