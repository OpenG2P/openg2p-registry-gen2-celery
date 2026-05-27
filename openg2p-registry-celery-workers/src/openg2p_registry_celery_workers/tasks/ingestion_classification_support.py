"""Helpers for ingest_data_classification_worker (two-pass vs legacy)."""

from typing import List, Tuple

from sqlalchemy import select
from sqlalchemy.orm import Session

from openg2p_registry_core.models import (
    IncomingModelRegisterSemanticPattern,
    IncomingModelSemanticPattern,
    IncomingRawDataPayload,
    PipelineActionEnum,
)
from .ingestion_register_lookup import find_register_row_by_identifier


def load_register_patterns(session: Session, data_model_id: str) -> List[IncomingModelRegisterSemanticPattern]:
    return list(
        session.execute(
            select(IncomingModelRegisterSemanticPattern).where(
                IncomingModelRegisterSemanticPattern.data_model_id == data_model_id
            )
        ).scalars().all()
    )


def match_register_pattern_and_identifier(
    patterns: List[IncomingModelRegisterSemanticPattern],
    payload: IncomingRawDataPayload,
    pattern_matcher,
) -> Tuple[IncomingModelRegisterSemanticPattern, str]:
    data = payload.raw_data_json
    for reg_pat in patterns:
        if not pattern_matcher.validate_register_semantic_pattern_match(reg_pat, data):
            continue
        record_id = pattern_matcher.extract_record_identifier_value(
            data, reg_pat.key_path_for_record_identifier
        )
        if not record_id:
            raise ValueError("RECORD_IDENTIFIER_EMPTY")
        return reg_pat, record_id
    raise ValueError("REGISTER_PATTERN_NOT_MATCHED")


def resolve_pipeline_action_and_internal_id(
    session: Session,
    register_id: str,
    record_identifier: str,
) -> Tuple[str, str | None]:
    row = find_register_row_by_identifier(session, register_id, record_identifier)
    if row is None:
        return PipelineActionEnum.ADD.value, None
    internal_id = getattr(row, "internal_record_id", None)
    if not internal_id:
        raise ValueError("INVALID_RECORD_STATE")
    return PipelineActionEnum.UPDATE.value, internal_id


def load_semantic_patterns_for_register(
    session: Session, data_model_id: str, register_id: str
) -> List[IncomingModelSemanticPattern]:
    return list(
        session.execute(
            select(IncomingModelSemanticPattern).where(
                IncomingModelSemanticPattern.data_model_id == data_model_id,
                IncomingModelSemanticPattern.register_id == register_id,
            )
        ).scalars().all()
    )


def match_semantic_for_add(
    patterns: List[IncomingModelSemanticPattern],
    payload: IncomingRawDataPayload,
    pattern_matcher,
) -> IncomingModelSemanticPattern:
    data = payload.raw_data_json
    for p in patterns:
        if pattern_matcher.validate_intake_form_pattern_only(p, data):
            return p
    raise ValueError("INTAKE_FORM_PATTERN_NOT_MATCHED")


def match_semantic_for_update(
    patterns: List[IncomingModelSemanticPattern],
    payload: IncomingRawDataPayload,
    pattern_matcher,
) -> IncomingModelSemanticPattern:
    data = payload.raw_data_json
    for p in patterns:
        if not p.section_id or not p.pattern_for_section:
            continue
        if pattern_matcher.validate_section_pattern_only(p, data):
            return p
    raise ValueError("SECTION_PATTERN_NOT_MATCHED")


def match_legacy_semantic(
    patterns: List[IncomingModelSemanticPattern],
    payload: IncomingRawDataPayload,
    pattern_matcher,
) -> IncomingModelSemanticPattern:
    data = payload.raw_data_json
    for p in patterns:
        if pattern_matcher.validate_semantic_pattern_match(p, data):
            return p
    raise ValueError("INTAKE_FORM_PATTERN_NOT_MATCHED")
