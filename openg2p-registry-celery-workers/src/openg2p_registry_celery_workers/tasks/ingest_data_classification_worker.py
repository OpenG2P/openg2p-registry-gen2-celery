import logging
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from openg2p_registry_core.helpers import PatternMatcher
from openg2p_registry_core.models import (
    ProcessStatusEnum,
    IncomingRawData,
    IncomingRawDataPayload,
    IncomingClassifiedData,
    IncomingModelSemanticPattern,
    PipelineActionEnum,
)

from .ingestion_classification_support import (
    load_register_patterns,
    match_register_pattern_and_identifier,
    resolve_pipeline_action_and_internal_id,
    load_semantic_patterns_for_register,
    match_semantic_for_add,
    match_semantic_for_update,
    match_legacy_semantic,
)
from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="ingest_data_classification_worker")
def ingest_data_classification_worker(ingest_id: str):
    _logger.info("Starting ingest_data_classification_worker for ingest_id: %s", ingest_id)
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        incoming_raw_data: IncomingRawData | None = None
        try:
            incoming_raw_data = session.get(IncomingRawData, ingest_id)
            if incoming_raw_data is None:
                raise ValueError(f"Incoming raw data not found for ingest_id '{ingest_id}'")
            incoming_raw_data_payload = session.get(IncomingRawDataPayload, ingest_id)
            if incoming_raw_data_payload is None:
                raise ValueError(f"Incoming raw data payload not found for ingest_id '{ingest_id}'")
            pattern_matcher = PatternMatcher().get_component()

            register_patterns = load_register_patterns(session, incoming_raw_data.data_model_id)
            if register_patterns:
                fields = _classify_with_register_patterns(
                    session,
                    incoming_raw_data,
                    incoming_raw_data_payload,
                    register_patterns,
                    pattern_matcher,
                )
            else:
                fields = _classify_legacy(
                    session,
                    incoming_raw_data,
                    incoming_raw_data_payload,
                    pattern_matcher,
                )

            incoming_classified_data = IncomingClassifiedData(
                ingest_id=incoming_raw_data.ingest_id,
                data_model_id=incoming_raw_data.data_model_id,
                partner_id=incoming_raw_data.partner_id,
                register_id=fields["register_id"],
                pipeline_action=fields["pipeline_action"],
                section_id=fields.get("section_id"),
                internal_record_id=fields.get("internal_record_id"),
                intake_form_id=fields.get("intake_form_id"),
                semantic_pattern_id=fields["semantic_pattern_id"],
                classified_date_time=datetime.now(),
            )
            session.add(incoming_classified_data)

            incoming_raw_data.classification_number_of_attempts += 1
            incoming_raw_data.classification_status = ProcessStatusEnum.PROCESSED.value
            incoming_raw_data.classification_date_time = datetime.now()
            session.commit()

        except Exception as e:
            _logger.error(
                "Error during ingest_data_classification_worker for ingest_id %s: %s",
                ingest_id,
                str(e),
            )
            session.rollback()

            if incoming_raw_data is None:
                raise e

            if incoming_raw_data.classification_number_of_attempts < _config.worker_max_attempts:
                incoming_raw_data.classification_number_of_attempts += 1
                incoming_raw_data.classification_status = ProcessStatusEnum.PENDING.value
            else:
                incoming_raw_data.classification_status = ProcessStatusEnum.FAILED.value

            incoming_raw_data.classification_latest_error_code = str(e)
            incoming_raw_data.classification_date_time = datetime.now()
            session.commit()
            raise e

        _logger.info("Completed ingest_data_classification_worker for ingest_id: %s", ingest_id)


def _classify_legacy(session, incoming_raw_data, payload, pattern_matcher):
    semantic_patterns = list(
        session.execute(
            select(IncomingModelSemanticPattern).where(
                IncomingModelSemanticPattern.data_model_id == incoming_raw_data.data_model_id
            )
        ).scalars().all()
    )
    matched = match_legacy_semantic(semantic_patterns, payload, pattern_matcher)
    if not matched.intake_form_id:
        raise ValueError("INTAKE_FORM_PATTERN_NOT_MATCHED")
    return {
        "register_id": matched.register_id,
        "pipeline_action": PipelineActionEnum.ADD.value,
        "section_id": None,
        "internal_record_id": None,
        "intake_form_id": matched.intake_form_id,
        "semantic_pattern_id": matched.semantic_pattern_id,
    }


def _classify_with_register_patterns(
    session, incoming_raw_data, payload, register_patterns, pattern_matcher
):
    reg_pat, record_identifier = match_register_pattern_and_identifier(
        register_patterns, payload, pattern_matcher
    )
    pipeline_action, internal_record_id = resolve_pipeline_action_and_internal_id(
        session, reg_pat.register_id, record_identifier
    )
    semantic_patterns = load_semantic_patterns_for_register(
        session, incoming_raw_data.data_model_id, reg_pat.register_id
    )
    if pipeline_action == PipelineActionEnum.ADD.value:
        matched = match_semantic_for_add(semantic_patterns, payload, pattern_matcher)
        return {
            "register_id": reg_pat.register_id,
            "pipeline_action": pipeline_action,
            "section_id": None,
            "internal_record_id": None,
            "intake_form_id": matched.intake_form_id,
            "semantic_pattern_id": matched.semantic_pattern_id,
        }

    matched = match_semantic_for_update(semantic_patterns, payload, pattern_matcher)
    return {
        "register_id": reg_pat.register_id,
        "pipeline_action": pipeline_action,
        "section_id": matched.section_id,
        "internal_record_id": internal_record_id,
        "intake_form_id": None,
        "semantic_pattern_id": matched.semantic_pattern_id,
    }
