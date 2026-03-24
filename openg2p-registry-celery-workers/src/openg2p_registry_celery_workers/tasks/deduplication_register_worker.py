import logging
import importlib

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from openg2p_registry_core.models import (
    G2PRegisterChangeRequest,
    G2PRegisterChangeRequestPayload,
    G2PRegisterDefinition,
    DeduplicationStatusEnum,
    DeduplicationRegisterResult,
)


from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="deduplication_register_worker", bind=True, max_retries=3)
def deduplication_register_worker(self, change_request_id: str):
    """
    Worker that performs deduplication check against register records.
    Retries up to 3 times on failure.
    """
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)
    
    with session_maker() as session:
        change_request: G2PRegisterChangeRequest = None
        try:
            # Fetch change request and payload
            change_request = session.get(G2PRegisterChangeRequest, change_request_id)
            if not change_request:
                raise Exception(f"Change request not found: {change_request_id}")
            
            change_request_payload = session.get(G2PRegisterChangeRequestPayload, change_request_id)
            if not change_request_payload:
                raise Exception(f"Change request payload not found: {change_request_id}")
            
            # Get domain service
            domain_factory_module = importlib.import_module(
                "openg2p_registry_extensions.register_domain.factory"
            )
            FactoryClass = getattr(domain_factory_module, "G2PRegisterDomainFactory")
            domain_factory = FactoryClass.get_component()
            if not domain_factory:
                domain_factory = FactoryClass()
            
            register_definition = session.get(G2PRegisterDefinition, change_request.register_id)
            domain_service = domain_factory.get_domain_service(register_definition.register_mnemonic)

            incoming_payload = _normalize_change_payload(
                change_request_payload.change_payload,
                context=f"change_request_id={change_request_id}",
            )

            # Compute dedup scores using public service method
            results = domain_service.compute_deduplication_score_for_register(
                change_request_id,
                change_request.register_id,
                incoming_payload,
                session
            )

            # Save results
            for result in results:
                dedup_result = DeduplicationRegisterResult(
                    change_request_id=change_request_id,
                    internal_record_id=result["candidate_id"],
                    match_score=result["score"],
                    field_matches=result.get("field_matches", {})
                )
                session.add(dedup_result)
            
            # Update status to COMPLETED
            change_request.deduplication_register_status = DeduplicationStatusEnum.COMPLETED.value
            change_request.deduplication_register_failure_reason = None
            session.commit()
            
            _logger.info(f"Completed deduplication_register for change_request: {change_request_id}")
            
        except Exception as e:
            _logger.error(f"Error in deduplication_register_worker for change_request {change_request_id}: {str(e)}")
            session.rollback()
            
            if change_request:
                # Retry logic with max_retries
                if self.request.retries < self.max_retries:
                    change_request.deduplication_register_status = DeduplicationStatusEnum.PENDING.value
                    _logger.info(f"Retrying deduplication_register for change_request: {change_request_id}")
                else:
                    change_request.deduplication_register_status = DeduplicationStatusEnum.FAILED.value
                    change_request.deduplication_register_failure_reason = str(e)
                    _logger.error(f"Max retries exceeded for change_request: {change_request_id}")
                
                session.add(change_request)
                session.commit()
            
            raise e

def _normalize_change_payload(change_payload, *, context: str) -> dict:
    """Normalize modern payload variants to legacy dict shape for scoring."""
    if isinstance(change_payload, dict):
        return change_payload

    if isinstance(change_payload, list):
        if not change_payload:
            _logger.info(f"Empty change_payload list for {context}; dedup will produce no matches.")
            return {}
        first_item = change_payload[0]
        if isinstance(first_item, dict):
            _logger.info(f"Normalized list change_payload to first item for {context}.")
            return first_item
        _logger.warning(
            f"Unsupported first payload item type for {context}: {type(first_item).__name__}; dedup will produce no matches."
        )
        return {}

    if change_payload is None:
        _logger.info(f"Missing change_payload for {context}; dedup will produce no matches.")
        return {}

    _logger.warning(
        f"Unsupported change_payload type for {context}: {type(change_payload).__name__}; dedup will produce no matches."
    )
    return {}