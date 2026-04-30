import logging
import importlib
from datetime import datetime

from sqlalchemy import select, inspect
from sqlalchemy.orm import sessionmaker
from openg2p_registry_core.models import (
    G2PIntakeFormSubmission,
    G2PRegisterDefinition,
    G2PRegisterSection,
    RegisterPurposeEnum,
    ApprovalStatusEnum,
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
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        submission: G2PIntakeFormSubmission = None
        try:
            _logger.info(
                f"Starting deduplication_intake_forms_vs_intake_forms for submission: {submission_id} "
                f"(attempt {self.request.retries + 1}/{self.max_retries + 1})"
            )

            submission = session.get(G2PIntakeFormSubmission, submission_id)
            if not submission:
                raise Exception(f"Intake form submission not found: {submission_id}")

            _logger.info(
                f"Loaded submission {submission_id}: register_id={submission.register_id}, "
                f"approval_status={submission.approval_status}, "
                f"deduplication_status_vs_intake_forms={submission.deduplication_status_vs_intake_forms}"
            )

            # Find all sections for this form's register
            sections = session.execute(
                select(G2PRegisterSection).where(
                    G2PRegisterSection.register_id == str(submission.register_id)
                )
            ).scalars().all()

            _logger.info(
                f"Found {len(sections)} section(s) for register_id={submission.register_id}: "
                f"{[s.section_mnemonic for s in sections]}"
            )

            # Keep only sections whose section_register has purpose == REGISTER
            register_sections = []
            for section in sections:
                sec_reg_def = session.get(G2PRegisterDefinition, section.section_register_id)
                if sec_reg_def and sec_reg_def.register_purpose == RegisterPurposeEnum.REGISTER.value:
                    register_sections.append((section, sec_reg_def))
                else:
                    _logger.info(
                        f"Skipping section {section.section_mnemonic} "
                        f"(section_register_id={section.section_register_id}): "
                        f"purpose={sec_reg_def.register_purpose if sec_reg_def else 'NOT_FOUND'}"
                    )

            _logger.info(
                f"Filtered to {len(register_sections)} REGISTER-purpose section(s): "
                f"{[s.section_mnemonic for s, _ in register_sections]}"
            )

            if not register_sections:
                _logger.info(
                    f"No REGISTER-purpose sections for submission {submission_id}; nothing to deduplicate."
                )
                submission.deduplication_status_vs_intake_forms = DeduplicationStatusEnum.COMPLETED.value
                submission.deduplication_intake_forms_error = None
                submission.deduplication_intake_forms_process_timestamp = datetime.utcnow()
                submission.deduplication_intake_forms_attempts += 1
                session.commit()
                return

            # Delete any existing results for this submission (idempotent on retry)
            existing = session.execute(
                select(DeduplicationIntakeFormIntakeFormResult).where(
                    DeduplicationIntakeFormIntakeFormResult.submission_id == submission_id
                )
            ).scalars().all()
            if existing:
                _logger.info(f"Deleting {len(existing)} existing dedup result(s) for submission {submission_id} before recompute.")
            for row in existing:
                session.delete(row)
            session.flush()

            # Load domain factory once
            domain_factory_module = importlib.import_module(
                "openg2p_registry_extensions.register_domain.factory"
            )
            FactoryClass = getattr(domain_factory_module, "G2PRegisterDomainFactory")
            domain_factory = FactoryClass.get_component()
            if not domain_factory:
                domain_factory = FactoryClass()

            model_module = importlib.import_module(
                "openg2p_registry_extensions.register_domain.models"
            )

            # Other non-approved submissions for the same register (fetched once, reused per section)
            other_submissions = session.execute(
                select(G2PIntakeFormSubmission).where(
                    (G2PIntakeFormSubmission.register_id == submission.register_id) &
                    (G2PIntakeFormSubmission.submission_id != submission_id) &
                    (G2PIntakeFormSubmission.approval_status != ApprovalStatusEnum.APPROVED.value)
                )
            ).scalars().all()

            _logger.info(
                f"Found {len(other_submissions)} other non-approved submission(s) for register_id={submission.register_id} "
                f"to compare against submission {submission_id}."
            )

            for section, sec_reg_def in register_sections:
                _logger.info(
                    f"Processing section: {section.section_mnemonic} "
                    f"(section_register_id={section.section_register_id}, mnemonic={sec_reg_def.register_mnemonic})"
                )

                intake_class = getattr(
                    model_module, f"G2PIntakeForm{sec_reg_def.register_mnemonic}", None
                )
                if intake_class is None:
                    _logger.warning(
                        f"No intake form model for {sec_reg_def.register_mnemonic}, skipping section."
                    )
                    continue

                # Records for the current submission (list sections → multiple rows)
                intake_records = session.execute(
                    select(intake_class).where(intake_class.submission_id == submission_id)
                ).scalars().all()

                if not intake_records:
                    _logger.info(
                        f"No intake records for section {section.section_mnemonic} "
                        f"(register {sec_reg_def.register_mnemonic}), submission {submission_id}."
                    )
                    continue

                _logger.info(
                    f"Found {len(intake_records)} intake record(s) for submission {submission_id} "
                    f"in section {section.section_mnemonic}."
                )

                domain_service = domain_factory.get_domain_service(sec_reg_def.register_mnemonic)
                if not domain_service:
                    _logger.warning(
                        f"No domain service for {sec_reg_def.register_mnemonic}, skipping section."
                    )
                    continue

                _logger.info(f"Resolved domain service for mnemonic={sec_reg_def.register_mnemonic}: {type(domain_service).__name__}")

                # Build candidate list from other submissions' records for this section.
                # Multiple records per other submission (list sections) are each added separately;
                # we later keep the best score per candidate_submission_id.
                other_change_requests = []
                for other in other_submissions:
                    other_records = session.execute(
                        select(intake_class).where(
                            intake_class.submission_id == str(other.submission_id)
                        )
                    ).scalars().all()
                    for other_record in other_records:
                        other_change_requests.append({
                            "change_request_id": str(other.submission_id),
                            "change_payload": {
                                col.name: getattr(other_record, col.name)
                                for col in inspect(intake_class).columns
                                if col.name not in {"submission_id"}
                            },
                        })

                _logger.info(
                    f"Built {len(other_change_requests)} candidate change request record(s) "
                    f"from {len(other_submissions)} other submission(s) for section {section.section_mnemonic}."
                )

                if not other_change_requests:
                    _logger.info(
                        f"No candidate records for section {section.section_mnemonic}; skipping score computation."
                    )
                    continue

                for idx, intake_record in enumerate(intake_records):
                    incoming_dict = {
                        col.name: getattr(intake_record, col.name)
                        for col in inspect(intake_class).columns
                        if col.name not in {"submission_id"}
                    }

                    _logger.info(
                        f"Computing deduplication scores for intake record {idx + 1}/{len(intake_records)} "
                        f"of submission {submission_id} in section {section.section_mnemonic} "
                        f"against {len(other_change_requests)} candidate(s)."
                    )

                    results = domain_service.compute_deduplication_score_for_change_request(
                        submission_id,
                        str(section.section_register_id),
                        incoming_dict,
                        other_change_requests,
                        session,
                    )

                    _logger.info(
                        f"Score computation returned {len(results)} result(s) for intake record "
                        f"{idx + 1} of submission {submission_id} in section {section.section_mnemonic}."
                    )

                    # Deduplicate by candidate_submission_id — keep best score when a
                    # candidate has multiple section records.
                    best: dict[str, dict] = {}
                    for result in results:
                        cid = result["candidate_id"]
                        if cid not in best or result["score"] > best[cid]["score"]:
                            best[cid] = result

                    _logger.info(
                        f"After deduplication by candidate_id: {len(best)} unique candidate(s) with scores: "
                        f"{[(cid, r['score']) for cid, r in best.items()]}"
                    )

                    for result in best.values():
                        session.add(DeduplicationIntakeFormIntakeFormResult(
                            submission_id=submission_id,
                            section_register_id=str(section.section_register_id),
                            candidate_submission_id=result["candidate_id"],
                            match_score=result["score"],
                            field_matches=result.get("field_matches", {}),
                        ))

            submission.deduplication_status_vs_intake_forms = DeduplicationStatusEnum.COMPLETED.value
            submission.deduplication_intake_forms_error = None
            submission.deduplication_intake_forms_process_timestamp = datetime.utcnow()
            submission.deduplication_intake_forms_attempts += 1
            session.commit()

            _logger.info(
                f"Completed deduplication_intake_forms_vs_intake_forms for submission: {submission_id}"
            )

        except Exception as e:
            _logger.error(
                f"Error in deduplication_intake_forms_vs_intake_forms_worker for submission "
                f"{submission_id}: {str(e)}"
            )
            session.rollback()

            if submission:
                submission.deduplication_intake_forms_attempts += 1
                submission.deduplication_intake_forms_process_timestamp = datetime.utcnow()
                if self.request.retries < self.max_retries:
                    submission.deduplication_status_vs_intake_forms = DeduplicationStatusEnum.PENDING.value
                    _logger.info(
                        f"Retrying deduplication_intake_forms_vs_intake_forms for submission: {submission_id}"
                    )
                else:
                    submission.deduplication_status_vs_intake_forms = DeduplicationStatusEnum.FAILED.value
                    submission.deduplication_intake_forms_error = str(e)
                    _logger.error(f"Max retries exceeded for submission: {submission_id}")

                session.add(submission)
                session.commit()

            raise e
