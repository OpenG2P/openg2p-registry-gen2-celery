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
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        submission: G2PIntakeFormSubmission = None
        try:
            _logger.info(
                f"Starting deduplication_intake_forms_vs_register for submission: {submission_id} "
                f"(attempt {self.request.retries + 1}/{self.max_retries + 1})"
            )

            submission = session.get(G2PIntakeFormSubmission, submission_id)
            if not submission:
                raise Exception(f"Intake form submission not found: {submission_id}")

            _logger.info(
                f"Loaded submission {submission_id}: register_id={submission.register_id}, "
                f"approval_status={submission.approval_status}, "
                f"deduplication_status_vs_register={submission.deduplication_status_vs_register}"
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
                submission.deduplication_status_vs_register = DeduplicationStatusEnum.COMPLETED.value
                submission.deduplication_register_error = None
                submission.deduplication_register_process_timestamp = datetime.utcnow()
                submission.deduplication_register_forms_attempts += 1
                session.commit()
                return

            # Delete any existing results for this submission (idempotent on retry)
            existing = session.execute(
                select(DeduplicationIntakeFormRegisterResult).where(
                    DeduplicationIntakeFormRegisterResult.submission_id == submission_id
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

                # List sections produce multiple rows per submission
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

                for idx, intake_record in enumerate(intake_records):
                    record_dict = {
                        col.name: getattr(intake_record, col.name)
                        for col in inspect(intake_class).columns
                        if col.name not in {"submission_id"}
                    }

                    _logger.info(
                        f"Computing register deduplication scores for intake record {idx + 1}/{len(intake_records)} "
                        f"of submission {submission_id} in section {section.section_mnemonic}."
                    )

                    results = domain_service.compute_deduplication_score_for_register(
                        submission_id,
                        str(section.section_register_id),
                        record_dict,
                        session,
                    )

                    _logger.info(
                        f"Score computation returned {len(results)} result(s) for intake record "
                        f"{idx + 1} of submission {submission_id} in section {section.section_mnemonic}: "
                        f"{[(r['candidate_id'], r['score']) for r in results]}"
                    )

                    for result in results:
                        session.add(DeduplicationIntakeFormRegisterResult(
                            submission_id=submission_id,
                            section_register_id=str(section.section_register_id),
                            internal_record_id=result["candidate_id"],
                            match_score=result["score"],
                            field_matches=result.get("field_matches", {}),
                        ))

            submission.deduplication_status_vs_register = DeduplicationStatusEnum.COMPLETED.value
            submission.deduplication_register_error = None
            submission.deduplication_register_process_timestamp = datetime.utcnow()
            submission.deduplication_register_forms_attempts += 1
            session.commit()

            _logger.info(
                f"Completed deduplication_intake_forms_vs_register for submission: {submission_id}"
            )

        except Exception as e:
            _logger.error(
                f"Error in deduplication_intake_forms_vs_register_worker for submission "
                f"{submission_id}: {str(e)}"
            )
            session.rollback()

            if submission:
                submission.deduplication_register_forms_attempts += 1
                submission.deduplication_register_process_timestamp = datetime.utcnow()
                if self.request.retries < self.max_retries:
                    submission.deduplication_status_vs_register = DeduplicationStatusEnum.PENDING.value
                    _logger.info(
                        f"Retrying deduplication_intake_forms_vs_register for submission: {submission_id}"
                    )
                else:
                    submission.deduplication_status_vs_register = DeduplicationStatusEnum.FAILED.value
                    submission.deduplication_register_error = str(e)
                    _logger.error(f"Max retries exceeded for submission: {submission_id}")

                session.add(submission)
                session.commit()

            raise e
