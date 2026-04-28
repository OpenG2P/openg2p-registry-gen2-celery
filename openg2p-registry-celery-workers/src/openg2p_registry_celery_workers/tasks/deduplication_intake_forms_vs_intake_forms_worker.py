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
    DeduplicationIntakeFormIntakeFormResult,
)

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="deduplication_intake_forms_vs_intake_forms_worker", bind=True, max_retries=3)
def deduplication_intake_forms_vs_intake_forms_worker(self, submission_id: str):
    """
    Worker that performs deduplication check of an intake form submission against other PENDING intake form submissions.
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

            # Only compare against other PENDING intake form submissions for the same register
            other_submissions_records = (
                session.execute(
                    select(G2PIntakeFormSubmission).where(
                        (G2PIntakeFormSubmission.register_id == submission.register_id) &
                        (G2PIntakeFormSubmission.submission_id != submission_id) &
                        (G2PIntakeFormSubmission.deduplication_status_vs_intake_forms == DeduplicationStatusEnum.PENDING.value)
                    )
                )
            ).scalars().all()

            other_submissions = []
            for other in other_submissions_records:
                other_payload = session.get(G2PIntakeFormSubmissionPayload, other.submission_id)
                if other_payload:
                    other_submissions.append({
                        "change_request_id": other.submission_id,
                        "change_payload": _normalize_payload(
                            other_payload.search_text,
                            context=(
                                f"submission_id={submission_id}, "
                                f"candidate_submission_id={other.submission_id}"
                            ),
                        ),
                    })

            results = domain_service.compute_deduplication_score_for_change_request(
                submission_id,
                submission.register_id,
                incoming_payload,
                other_submissions,
                session,
            )

            for result in results:
                dedup_result = DeduplicationIntakeFormIntakeFormResult(
                    submission_id=submission_id,
                    candidate_submission_id=result["candidate_id"],
                    match_score=result["score"],
                    field_matches=result.get("field_matches", {}),
                )
                session.add(dedup_result)

            submission.deduplication_status_vs_intake_forms = DeduplicationStatusEnum.COMPLETED.value
            submission.deduplication_intake_forms_error = None
            submission.deduplication_intake_forms_process_timestamp = datetime.utcnow()
            submission.deduplication_intake_forms_attempts += 1
            session.commit()

            _logger.info(f"Completed deduplication_intake_forms_vs_intake_forms for submission: {submission_id}")

        except Exception as e:
            _logger.error(f"Error in deduplication_intake_forms_vs_intake_forms_worker for submission {submission_id}: {str(e)}")
            session.rollback()

            if submission:
                submission.deduplication_intake_forms_attempts += 1
                submission.deduplication_intake_forms_process_timestamp = datetime.utcnow()
                if self.request.retries < self.max_retries:
                    submission.deduplication_status_vs_intake_forms = DeduplicationStatusEnum.PENDING.value
                    _logger.info(f"Retrying deduplication_intake_forms_vs_intake_forms for submission: {submission_id}")
                else:
                    submission.deduplication_status_vs_intake_forms = DeduplicationStatusEnum.FAILED.value
                    submission.deduplication_intake_forms_error = str(e)
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
