import logging
import asyncio
from typing import Any, List
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from openg2p_registry_core.models import (
    G2PIntakeForm,
    G2PIntakeFormSectionPayload,
    G2PRegisterSection,
    G2PRegisterDefinition,
    ChangeRequestStatusEnum,
    G2PRegisterChangeRequest,
    ApprovalStatusEnum,
)
from openg2p_registry_core.schemas import ChangeRequestRequestPayload
from openg2p_registry_core.schemas import ChangePayload
from openg2p_registry_core.services import G2PRegisterService

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="intake_form_change_request_worker")
def intake_form_change_request_worker(submission_id: str):
    """
    Worker that processes a FINAL intake_form and creates change requests for each section.
    Creates one change request per section payload.
    """
    _logger.info(f"Starting intake_form_change_request_worker for submission_id: {submission_id}")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        intake_form: G2PIntakeForm = None
        currently_approving_change_request_id: str | None = None
        try:
            # Fetch the intake_form
            intake_form = session.get(G2PIntakeForm, submission_id)
            if not intake_form:
                raise Exception(f"IntakeForm not found: {submission_id}")

            # Fetch all section payloads for this intake_form
            section_payloads: List[G2PIntakeFormSectionPayload] = (
                session.execute(
                    select(G2PIntakeFormSectionPayload)
                    .filter(G2PIntakeFormSectionPayload.submission_id == submission_id)
                )
                .scalars()
                .all()
            )

            if not section_payloads:
                raise Exception(f"No section payloads found for intake_form: {submission_id}")

            _logger.info(f"Found {len(section_payloads)} section payloads for intake_form: {submission_id}")

            # Get register definition for register_mnemonic
            register_definition: G2PRegisterDefinition = session.get(
                G2PRegisterDefinition, intake_form.register_id
            )
            if not register_definition:
                raise Exception(f"Register definition not found for register_id: {intake_form.register_id}")

            existing_change_requests = _get_existing_change_requests_for_intake_form(submission_id, session)
            existing_change_request_ids = [cr.change_request_id for cr in existing_change_requests]
            pending_existing_change_request_ids = [
                cr.change_request_id for cr in existing_change_requests
                if cr.approval_status == ApprovalStatusEnum.PENDING.value
            ]

            created_change_request_ids: List[str] = []
            to_auto_approve_change_request_ids: List[str] = []
            left_pending_change_request_ids: List[str] = []

            if existing_change_request_ids:
                _logger.info(
                    f"Reusing {len(existing_change_request_ids)} existing change requests "
                    f"for intake_form {submission_id}. Pending approvals: {len(pending_existing_change_request_ids)}"
                )
                created_change_request_ids = existing_change_request_ids
                for pending_change_request_id in pending_existing_change_request_ids:
                    pending_change_request = session.get(G2PRegisterChangeRequest, pending_change_request_id)
                    if not pending_change_request:
                        _logger.warning(
                            "Pending change request not found while classifying auto-approval: "
                            f"change_request_id={pending_change_request_id}"
                        )
                        left_pending_change_request_ids.append(pending_change_request_id)
                        continue
                    section = session.get(G2PRegisterSection, pending_change_request.section_id)
                    if not section:
                        _logger.warning(
                            "Section not found while classifying auto-approval: "
                            f"change_request_id={pending_change_request_id}, section_id={pending_change_request.section_id}"
                        )
                        left_pending_change_request_ids.append(pending_change_request_id)
                        continue
                    if section.cr_auto_approve_for_intake_form:
                        to_auto_approve_change_request_ids.append(pending_change_request_id)
                    else:
                        left_pending_change_request_ids.append(pending_change_request_id)
            else:
                # Create change request for each section payload
                for section_payload in section_payloads:
                    # Get section details for tab_id and section_register_id
                    section: G2PRegisterSection = session.get(
                        G2PRegisterSection, section_payload.section_id
                    )
                    if not section:
                        _logger.warning(f"Section not found for section_id: {section_payload.section_id}, skipping")
                        continue

                    change_payload_items = _build_change_payload_list(section_payload)
                    if not change_payload_items:
                        _logger.warning(
                            "Section payload is empty for intake_form submission, skipping: "
                            f"submission_id={submission_id}, section_id={section_payload.section_id}"
                        )
                        continue

                    # Build change request payload - change_payload is now a list
                    change_request_payload = ChangeRequestRequestPayload(
                        register_id=intake_form.register_id,
                        register_mnemonic=register_definition.register_mnemonic,
                        tab_id=section.tab_id,
                        section_id=section.section_id,
                        section_register_id=section.section_register_id,
                        change_payload=change_payload_items,
                    )

                    # Create change request asynchronously
                    change_request: G2PRegisterChangeRequest = asyncio.run(
                        _create_change_request_async(
                            change_request_payload,
                            submission_id,
                            source_partner_id="intake_form_system"
                        )
                    )

                    created_change_request_ids.append(change_request.change_request_id)
                    _logger.info(
                        f"Created change request {change_request.change_request_id} for section {section.section_id}"
                    )
                    if section.cr_auto_approve_for_intake_form:
                        to_auto_approve_change_request_ids.append(change_request.change_request_id)
                    else:
                        left_pending_change_request_ids.append(change_request.change_request_id)

                if not created_change_request_ids:
                    raise Exception(
                        f"No change requests were created for intake_form {submission_id}. "
                        "All section payloads were invalid or missing matching sections."
                    )

            _logger.info(
                "Classified intake-form change requests for auto-approval: "
                f"submission_id={submission_id}, total={len(created_change_request_ids)}, "
                f"eligible_for_auto_approve={len(to_auto_approve_change_request_ids)}, "
                f"left_pending_by_policy={len(left_pending_change_request_ids)}"
            )

            approved_count = 0
            for change_request_id in to_auto_approve_change_request_ids:
                currently_approving_change_request_id = change_request_id
                _logger.info(
                    "Auto-approving intake-form change request: "
                    f"submission_id={submission_id}, change_request_id={change_request_id}"
                )
                asyncio.run(_auto_approve_change_request_async(change_request_id))
                approved_count += 1

            # Update intake_form status
            intake_form.change_request_submission_status = ChangeRequestStatusEnum.PROCESSED.value
            intake_form.submission_no_of_attempts += 1
            intake_form.submission_latest_datetime = datetime.now()
            intake_form.submission_latest_error_code = None
            # Store the first change request ID (or primary section's)
            if created_change_request_ids:
                intake_form.change_request_id = created_change_request_ids[0]
            session.commit()

            _logger.info(
                f"Completed intake_form_change_request_worker for submission_id: {submission_id}, "
                f"change_requests_total={len(created_change_request_ids)}, "
                f"eligible_for_auto_approve={len(to_auto_approve_change_request_ids)}, "
                f"auto_approved_now={approved_count}, "
                f"left_pending_by_policy={len(left_pending_change_request_ids)}, "
                f"reused_existing={len(existing_change_request_ids)}"
            )

        except Exception as e:
            failed_on = f", failed_change_request_id={currently_approving_change_request_id}" if currently_approving_change_request_id else ""
            _logger.error(
                f"Error during intake_form_change_request_worker for submission_id {submission_id}{failed_on}: {str(e)}"
            )
            session.rollback()

            if intake_form:
                intake_form.submission_no_of_attempts += 1
                intake_form.submission_latest_datetime = datetime.now()
                intake_form.submission_latest_error_code = str(e)

                # Check if max attempts exceeded
                if intake_form.submission_no_of_attempts >= _config.worker_max_attempts:
                    intake_form.change_request_submission_status = ChangeRequestStatusEnum.FAILED.value
                    _logger.error(f"Max attempts exceeded for intake_form: {submission_id}, marking as FAILED")
                else:
                    # Reset to PENDING for retry
                    intake_form.change_request_submission_status = ChangeRequestStatusEnum.PENDING.value
                    _logger.info(f"Resetting intake_form {submission_id} to PENDING for retry")

                session.add(intake_form)
                session.commit()

            raise e


def _build_change_payload_list(section_payload: G2PIntakeFormSectionPayload) -> list[ChangePayload]:
    payload_items: list[dict[str, Any]] = section_payload.intake_form_section_payload or []
    return [ChangePayload(**payload_item) for payload_item in payload_items]


def _get_existing_change_requests_for_intake_form(submission_id: str, session) -> list[G2PRegisterChangeRequest]:
    """Fetch existing intake_form-linked change requests ordered by creation timestamp."""
    return (
        session.execute(
            select(G2PRegisterChangeRequest)
            .filter(G2PRegisterChangeRequest.submission_id == submission_id)
            .order_by(G2PRegisterChangeRequest.created_at.asc())
        )
        .scalars()
        .all()
    )


async def _create_change_request_async(
    change_request_payload: ChangeRequestRequestPayload,
    submission_id: str,
    source_partner_id: str
) -> G2PRegisterChangeRequest:
    """Create a change request using the G2PRegisterService."""
    g2p_register_service = G2PRegisterService.get_component()
    return await g2p_register_service.create_change_request(
        change_request_request_payload=change_request_payload,
        source_partner_id=source_partner_id,
        submission_id=submission_id
    )


async def _auto_approve_change_request_async(change_request_id: str) -> G2PRegisterChangeRequest:
    """Auto-approve a single change request by ID using the G2PRegisterService."""
    g2p_register_service = G2PRegisterService.get_component()
    return await g2p_register_service.auto_approve_change_request(change_request_id)
