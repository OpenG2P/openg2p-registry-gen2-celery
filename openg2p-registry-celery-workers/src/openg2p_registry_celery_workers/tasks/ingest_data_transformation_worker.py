import logging
from datetime import datetime
from typing import Dict, Optional
from jinja2 import Template

from openg2p_registry_core.models.g2p_intake_form_metadata import G2PIntakeFormDefinition, G2PIntakeFormUITab, G2PIntakeFormUITabSection
from openg2p_registry_core.models.g2p_register_sections import G2PRegisterSection
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker
from openg2p_registry_core.models import (
    ProcessStatusEnum,
    IncomingTemplate,
    IncomingClassifiedData,
    IncomingRawDataPayload,
    IncomingEnrichedTransformedData,
    IncomingModelSemanticPattern,
    PipelineActionEnum,
)
from openg2p_registry_core.interfaces import (
    G2PPayloadEnricherFactory,
    G2PPayloadEnricherInterface
)
from openg2p_registry_core.helpers import TemplateHelper, PatternMatcher, MinioClient

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="ingest_data_transformation_worker")
def ingest_data_transformation_worker(ingest_id: str):
    _logger.info(f"Starting ingest_data_transformation_worker for ingest_id: {ingest_id}")
    session_maker = sessionmaker(
        bind=_engine, expire_on_commit=False
    )

    with session_maker() as session:
        incoming_classified_data: IncomingClassifiedData | None = None
        try:
            incoming_classified_data = session.get(IncomingClassifiedData, ingest_id)
            incoming_raw_data_payload = session.get(IncomingRawDataPayload, ingest_id)
            
            enriched_data_json: Dict = _enrich_raw_data_json(
                incoming_classified_data,
                incoming_raw_data_payload,
                session
            )
            transformed_data_json: Dict = _transform_enriched_data_json(
                incoming_classified_data,
                enriched_data_json,
                session
            )
            _validate_transformed_data_json(
                incoming_classified_data,
                transformed_data_json,
                session
            )
            incoming_enriched_transformed_data: IncomingEnrichedTransformedData = _construct_incoming_enriched_transformed_data(
                incoming_classified_data,
                enriched_data_json,
                transformed_data_json,
            )
            session.add(incoming_enriched_transformed_data)

            # Update incoming_classified_data transformation_status -> PROCESSED
            incoming_classified_data.transformation_number_of_attempts += 1
            incoming_classified_data.transformation_status = ProcessStatusEnum.PROCESSED.value
            incoming_classified_data.transformation_date_time = datetime.now()

            # Update incoming_classified_data ingestion_status -> PENDING
            incoming_classified_data.ingestion_status = ProcessStatusEnum.PENDING.value
            session.commit()

        except Exception as e:
            _logger.error(
                f"Error during processing ingest_data_transformation_worker for ingest_id {ingest_id}: {str(e)}"
            )
            # Rollback all sessions
            session.rollback()

            # Retry logic if maximum attempts not exhausted
            if incoming_classified_data.transformation_number_of_attempts < _config.worker_max_attempts:
                incoming_classified_data.transformation_number_of_attempts += 1
                incoming_classified_data.transformation_status = ProcessStatusEnum.PENDING.value
            else:
                incoming_classified_data.transformation_status = ProcessStatusEnum.FAILED.value

            incoming_classified_data.transformation_latest_error_code = str(e)
            incoming_classified_data.transformation_date_time = datetime.now()
            session.commit()
            # Raise exception for testing
            raise e

        _logger.info(
            f"Completed processing ingest_data_transformation_worker for ingest_id: {ingest_id}"
        )


def _get_business_payload(
    incoming_raw_data_payload: IncomingRawDataPayload,
    incoming_classified_data: IncomingClassifiedData,
    session: Session
) -> Dict:
    incoming_model_semantic_pattern: IncomingModelSemanticPattern | None = session.execute(
        select(IncomingModelSemanticPattern).filter_by(
            semantic_pattern_id=incoming_classified_data.semantic_pattern_id
        )
    ).scalar_one_or_none()
    if not incoming_model_semantic_pattern:
        raise Exception(
            f"Model semantic pattern not found semantic_pattern_id {incoming_classified_data.semantic_pattern_id}"
        )
    pattern_matcher = PatternMatcher().get_component()
    business_payload: Dict | None = pattern_matcher.get_business_payload(
        incoming_model_semantic_pattern,
        incoming_raw_data_payload.raw_data_json
    )
    if not business_payload:
        raise Exception(
            f"Business payload not found using key_path_for_business_payload {incoming_model_semantic_pattern.key_path_for_business_payload}"
        )
    return business_payload
    
def _construct_incoming_enriched_transformed_data(
    incoming_classified_data: IncomingClassifiedData,
    enriched_data_json: Dict,
    transformed_data_json: Dict,
) -> IncomingEnrichedTransformedData:
    incoming_enriched_transformed_data = IncomingEnrichedTransformedData(
        ingest_id=incoming_classified_data.ingest_id,
    )
    incoming_enriched_transformed_data.enriched_data_json = enriched_data_json
    incoming_enriched_transformed_data.transformed_data_json = transformed_data_json
    return incoming_enriched_transformed_data

def _validate_transformed_data_json(
    incoming_classified_data: IncomingClassifiedData,
    transformed_data_json: Dict,
    session: Session
) -> None:
    if not isinstance(transformed_data_json, dict):
        raise ValueError(
            f"Transformed data must be a dict keyed by section_mnemonic, got {type(transformed_data_json).__name__}"
        )

    if not transformed_data_json:
        raise ValueError("Transformed data is empty")

    if (incoming_classified_data.pipeline_action or PipelineActionEnum.ADD.value) == PipelineActionEnum.UPDATE.value:
        _validate_update_transformed_payload(incoming_classified_data, transformed_data_json, session)
        return

    _validate_add_transformed_payload(incoming_classified_data, transformed_data_json, session)


def _validate_update_transformed_payload(
    classified: IncomingClassifiedData,
    transformed_data_json: Dict,
    session: Session,
) -> None:
    if not classified.section_id:
        raise ValueError("SECTION_DATA_MISSING_IN_TRANSFORMED_PAYLOAD")
    section = session.get(G2PRegisterSection, classified.section_id)
    if not section:
        raise ValueError("SECTION_DATA_MISSING_IN_TRANSFORMED_PAYLOAD")
    key = section.section_mnemonic
    if key not in transformed_data_json:
        raise ValueError("SECTION_DATA_MISSING_IN_TRANSFORMED_PAYLOAD")
    payload = transformed_data_json[key]
    if payload is None or (isinstance(payload, list) and len(payload) == 0):
        raise ValueError("SECTION_DATA_MISSING_IN_TRANSFORMED_PAYLOAD")


def _validate_add_transformed_payload(
    classified: IncomingClassifiedData,
    transformed_data_json: Dict,
    session: Session,
) -> None:
    if not classified.intake_form_id:
        raise ValueError("INTAKE_FORM_REQUIRED_FOR_ADD_TRANSFORMATION")

    sections = (
        session.execute(
            select(G2PRegisterSection)
            .join(
                G2PIntakeFormUITabSection,
                G2PRegisterSection.section_id == G2PIntakeFormUITabSection.section_id,
            )
            .join(
                G2PIntakeFormUITab,
                G2PIntakeFormUITabSection.tab_id == G2PIntakeFormUITab.tab_id,
            )
            .where(G2PIntakeFormUITab.form_id == classified.intake_form_id)
            .order_by(
                G2PIntakeFormUITab.tab_order.asc(),
                G2PIntakeFormUITabSection.section_order.asc(),
                G2PRegisterSection.section_id.asc(),
            )
        )
    ).scalars().all()

    if not sections:
        raise ValueError(
            f"No sections found for intake form '{classified.intake_form_id}'"
        )

    section_mnemonics = {section.section_mnemonic: section for section in sections}

    for mnemonic in transformed_data_json.keys():
        if mnemonic not in section_mnemonics:
            _logger.warning(
                "Transformed data contains unrecognized section_mnemonic '%s' for form_id '%s'",
                mnemonic,
                classified.intake_form_id,
            )

    primary_section_found = False
    for mnemonic, section in section_mnemonics.items():
        if mnemonic in transformed_data_json and section.section_register_id == classified.register_id:
            primary_section_found = True
            break

    if not primary_section_found:
        raise ValueError(
            f"Transformed data must contain at least one section with section_register_id "
            f"matching register_id '{classified.register_id}'"
        )

def _enrich_raw_data_json(
    incoming_classified_data: IncomingClassifiedData, 
    incoming_raw_data_payload: IncomingRawDataPayload,
    session: Session
) -> Dict:
    incoming_model_semantic_pattern: IncomingModelSemanticPattern | None = session.execute(
        select(IncomingModelSemanticPattern).filter_by(
           semantic_pattern_id=incoming_classified_data.semantic_pattern_id
        )
    ).scalar_one_or_none()

    enriched_data_json = _get_business_payload(
        incoming_raw_data_payload, 
        incoming_classified_data, 
        session
    )
    # Enrich and store in IncomingEnrichedTransformedData as enriched_data_json or enriched_data_xml
    if incoming_model_semantic_pattern:
        raw_payload_enricher_class: str = incoming_model_semantic_pattern.raw_payload_enricher_class
        g2p_payload_enricher_service: G2PPayloadEnricherInterface = G2PPayloadEnricherFactory().get_enricher_service(raw_payload_enricher_class)
        enriched_data_json = g2p_payload_enricher_service.enrich(enriched_data_json, session)
    
    return enriched_data_json

def _transform_enriched_data_json(
    incoming_classified_data: IncomingClassifiedData,
    enriched_data_json: Dict,
    session: Session
) -> Dict:
    register_id = incoming_classified_data.register_id
    _pa = incoming_classified_data.pipeline_action or PipelineActionEnum.ADD.value
    if _pa == PipelineActionEnum.ADD.value:
        g2p_intake_form: G2PIntakeFormDefinition | None = session.get(
            G2PIntakeFormDefinition, incoming_classified_data.intake_form_id
        )
        if g2p_intake_form:
            register_id = g2p_intake_form.register_id

    incoming_template: IncomingTemplate | None = session.execute(
        select(IncomingTemplate).filter_by(
            data_model_id=incoming_classified_data.data_model_id,
            register_id=register_id,
        )
    ).scalar_one_or_none()
    if not incoming_template:
        raise Exception(
            f"Template not found data_model_id {incoming_classified_data.data_model_id}, "
            f"register_id {register_id}"
        )

    minio_client = MinioClient.get_component()
    template_helper = TemplateHelper.get_component()

    transformed_data_json: Dict = template_helper.render_with_template(
        minio_client=minio_client,
        template_file_id=incoming_template.template_file_id,
        data=enriched_data_json,
        expand_data=incoming_template.jsonld_expansion_required
    )
    return transformed_data_json
