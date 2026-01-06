import logging
import asyncio
from typing import List

from sqlalchemy import select, func
from sqlalchemy.orm import sessionmaker
from openg2p_registry_core.models import (
    G2PApplication,
    G2PApplicationSectionPayload,
    G2PRegisterSection,
    G2PRegisterDefinition,
    ChangeRequestStatusEnum,
)
from openg2p_registry_core.schemas import ChangeRequestRequestPayload
from openg2p_registry_core.schemas.payload import ChangePayload
from openg2p_registry_core.services import G2PRegisterService

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="application_changerequest_worker")
def application_changerequest_worker(application_id: str):
    """
    Worker that processes a FINAL application and creates change requests for each section.
    Creates one change request per section payload.
    """
    _logger.info(f"Starting application_changerequest_worker for application_id: {application_id}")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        application: G2PApplication = None
        try:
            # Fetch the application
            application = session.get(G2PApplication, application_id)
            if not application:
                raise Exception(f"Application not found: {application_id}")

            # Fetch all section payloads for this application
            section_payloads: List[G2PApplicationSectionPayload] = (
                session.execute(
                    select(G2PApplicationSectionPayload)
                    .filter(G2PApplicationSectionPayload.application_id == application_id)
                )
                .scalars()
                .all()
            )

            if not section_payloads:
                raise Exception(f"No section payloads found for application: {application_id}")

            _logger.info(f"Found {len(section_payloads)} section payloads for application: {application_id}")

            # Get register definition for register_mnemonic
            register_definition: G2PRegisterDefinition = session.get(
                G2PRegisterDefinition, application.register_id
            )
            if not register_definition:
                raise Exception(f"Register definition not found for register_id: {application.register_id}")

            created_change_request_ids: List[str] = []

            # Create change request for each section payload
            for section_payload in section_payloads:
                # Get section details for tab_id and section_register_id
                section: G2PRegisterSection = session.get(
                    G2PRegisterSection, section_payload.section_id
                )
                if not section:
                    _logger.warning(f"Section not found for section_id: {section_payload.section_id}, skipping")
                    continue

                # Convert application payload dict to ChangePayload object
                change_payload_obj = ChangePayload(**section_payload.application_payload_json)

                # Build change request payload
                change_request_payload = ChangeRequestRequestPayload(
                    register_id=application.register_id,
                    register_mnemonic=register_definition.register_mnemonic,
                    tab_id=section.tab_id,
                    section_id=section.section_id,
                    section_register_id=section.section_register_id,
                    change_payload=change_payload_obj,
                )

                # Create change request asynchronously
                change_request = asyncio.run(
                    _create_change_request_async(
                        change_request_payload,
                        application_id,
                        source_partner_id="application_system"
                    )
                )
                created_change_request_ids.append(change_request.change_request_id)
                _logger.info(
                    f"Created change request {change_request.change_request_id} for section {section.section_id}"
                )

            # Update application status
            application.change_request_submission_status = ChangeRequestStatusEnum.PROCESSED.value
            application.submission_no_of_attempts += 1
            application.submission_latest_datetime = func.now()
            application.submission_latest_error_code = None
            # Store the first change request ID (or primary section's)
            if created_change_request_ids:
                application.change_request_id = created_change_request_ids[0]
            session.commit()

            _logger.info(
                f"Completed application_changerequest_worker for application_id: {application_id}, "
                f"created {len(created_change_request_ids)} change requests"
            )

        except Exception as e:
            _logger.error(
                f"Error during application_changerequest_worker for application_id {application_id}: {str(e)}"
            )
            session.rollback()

            if application:
                application.submission_no_of_attempts += 1
                application.submission_latest_datetime = func.now()
                application.submission_latest_error_code = str(e)

                # Check if max attempts exceeded
                if application.submission_no_of_attempts >= _config.worker_max_attempts:
                    application.change_request_submission_status = ChangeRequestStatusEnum.FAILED.value
                    _logger.error(f"Max attempts exceeded for application: {application_id}, marking as FAILED")
                else:
                    # Reset to PENDING for retry
                    application.change_request_submission_status = ChangeRequestStatusEnum.PENDING.value
                    _logger.info(f"Resetting application {application_id} to PENDING for retry")

                session.add(application)
                session.commit()

            raise e


async def _create_change_request_async(
    change_request_payload: ChangeRequestRequestPayload,
    application_id: str,
    source_partner_id: str
):
    """Create a change request using the G2PRegisterService."""
    g2p_register_service = G2PRegisterService.get_component()
    return await g2p_register_service.create_change_request(
        change_request_request_payload=change_request_payload,
        source_partner_id=source_partner_id,
        application_id=application_id
    )

