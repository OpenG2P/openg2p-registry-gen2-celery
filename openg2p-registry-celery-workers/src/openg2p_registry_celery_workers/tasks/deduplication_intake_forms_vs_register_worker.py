import logging
import importlib
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from openg2p_registry_core.models import (
    G2PIntakeFormSubmission,
    G2PIntakeFormSubmissionPayload,
    G2PRegisterDefinition,
    DeduplicationStatusEnum,
    DeduplicationIntakeFormRegisterResult,
)

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="deduplication_intake_forms_vs_register_worker", bind=True, max_retries=3)
def deduplication_intake_forms_vs_register_worker(self, submission_id: str):
    """
    Worker that performs deduplication check of an intake form submission against register records.
    Retries up to 3 times on failure.
    """
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        submission: G2PIntakeFormSubmission = None
        try:
            submission = session.get(G2PIntakeFormSubmission, submission_id)
            if not submission:
                raise Exception(f"Intake form submission not found: {submission_id}")

            submission_payload = session.get(G2PIntakeFormSubmissionPayload, submission_id)
            if not submission_payload:
                raise Exception(f"Intake form submission payload not found: {submission_id}")

            domain_factory_module = importlib.import_module(
                "openg2p_registry_extensions.register_domain.factory"
            )
            FactoryClass = getattr(domain_factory_module, "G2PRegisterDomainFactory")
            domain_factory = FactoryClass.get_component()
            if not domain_factory:
                domain_factory = FactoryClass()

            register_definition = session.get(G2PRegisterDefinition, submission.register_id)
            domain_service = domain_factory.get_domain_service(register_definition.register_mnemonic)

            incoming_payload = _normalize_payload(
                submission_payload.search_text,
                context=f"submission_id={submission_id}",
            )

            results = domain_service.compute_deduplication_score_for_register(
                submission_id,
                submission.register_id,
                incoming_payload,
                session,
            )

            for result in results:
                dedup_result = DeduplicationIntakeFormRegisterResult(
                    submission_id=submission_id,
                    internal_record_id=result["candidate_id"],
                    match_score=result["score"],
                    field_matches=result.get("field_matches", {}),
                )
                session.add(dedup_result)

            submission.deduplication_status_vs_register = DeduplicationStatusEnum.COMPLETED.value
            submission.deduplication_register_error = None
            submission.deduplication_register_process_timestamp = datetime.utcnow()
            submission.deduplication_register_forms_attempts += 1
            session.commit()

            _logger.info(f"Completed deduplication_intake_forms_vs_register for submission: {submission_id}")

        except Exception as e:
            _logger.error(f"Error in deduplication_intake_forms_vs_register_worker for submission {submission_id}: {str(e)}")
            session.rollback()

            if submission:
                submission.deduplication_register_forms_attempts += 1
                submission.deduplication_register_process_timestamp = datetime.utcnow()
                if self.request.retries < self.max_retries:
                    submission.deduplication_status_vs_register = DeduplicationStatusEnum.PENDING.value
                    _logger.info(f"Retrying deduplication_intake_forms_vs_register for submission: {submission_id}")
                else:
                    submission.deduplication_status_vs_register = DeduplicationStatusEnum.FAILED.value
                    submission.deduplication_register_error = str(e)
                    _logger.error(f"Max retries exceeded for submission: {submission_id}")

                session.add(submission)
                session.commit()

            raise e


def _normalize_payload(payload, *, context: str) -> dict:
    if isinstance(payload, dict):
        return payload

    if isinstance(payload, list):
        if not payload:
            _logger.info(f"Empty payload list for {context}; dedup will produce no matches.")
            return {}
        first_item = payload[0]
        if isinstance(first_item, dict):
            _logger.info(f"Normalized list payload to first item for {context}.")
            return first_item
        _logger.warning(
            f"Unsupported first payload item type for {context}: {type(first_item).__name__}; dedup will produce no matches."
        )
        return {}

    if isinstance(payload, str):
        import json
        try:
            parsed = json.loads(payload)
            return _normalize_payload(parsed, context=context)
        except Exception:
            _logger.warning(f"Could not parse string payload as JSON for {context}; dedup will produce no matches.")
            return {}

    if payload is None:
        _logger.info(f"Missing payload for {context}; dedup will produce no matches.")
        return {}

    _logger.warning(
        f"Unsupported payload type for {context}: {type(payload).__name__}; dedup will produce no matches."
    )
    return {}
