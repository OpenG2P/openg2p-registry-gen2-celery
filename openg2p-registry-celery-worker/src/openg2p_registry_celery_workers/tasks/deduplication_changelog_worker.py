import logging
import importlib

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from openg2p_registry_core.models import (
    G2PRegisterChangeRequest,
    G2PRegisterChangeRequestPayload,
    G2PRegisterDefinition,
    DeduplicationStatusEnum,
    DeduplicationChangerequestResult
)


from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="deduplication_changerequest_worker", bind=True, max_retries=3)
def deduplication_changerequest_worker(self, change_request_id: str):
    """
    Worker that performs deduplication check against pending changerequest records.
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
            domain_factory = getattr(domain_factory_module, "G2PRegisterDomainFactory").get_component()
            
            register_definition = session.get(G2PRegisterDefinition, change_request.register_id)
            domain_service = domain_factory.get_domain_service(register_definition.register_mnemonic)

            # Find other pending changerequests for the same register
            other_changerequests_records = (
                session.execute(
                    select(G2PRegisterChangeRequest).where(
                        (G2PRegisterChangeRequest.register_id == change_request.register_id) &
                        (G2PRegisterChangeRequest.change_request_id != change_request_id)
                    )
                )
            ).scalars().all()

            # Build list of other changerequest data
            other_changerequests = []
            for other_changerequest in other_changerequests_records:
                other_payload = session.get(G2PRegisterChangeRequestPayload, other_changerequest.change_request_id)
                if other_payload:
                    other_changerequests.append({
                        'change_request_id': other_changerequest.change_request_id,
                        'change_payload': other_payload.change_payload
                    })

            # Compute dedup scores using public service method
            results = domain_service.compute_deduplication_score_for_changerequest(
                change_request_id,
                change_request.register_id,
                change_request_payload.change_payload,
                other_changerequests,
                session
            )

            # Save results
            for result in results:
                dedup_result = DeduplicationChangerequestResult(
                    change_request_id=change_request_id,
                    candidate_change_request_id=result["candidate_id"],
                    match_score=result["score"],
                    field_matches=result.get("field_matches", {})
                )
                session.add(dedup_result)
            
            # Update status to COMPLETED
            change_request.deduplication_changerequest_status = DeduplicationStatusEnum.COMPLETED.value
            change_request.deduplication_changerequest_failure_reason = None
            session.commit()
            
            _logger.info(f"Completed deduplication_changerequest for change_request: {change_request_id}")
            
        except Exception as e:
            _logger.error(f"Error in deduplication_changerequest_worker for change_request {change_request_id}: {str(e)}")
            session.rollback()
            
            if change_request:
                # Retry logic with max_retries
                if self.request.retries < self.max_retries:
                    change_request.deduplication_changerequest_status = DeduplicationStatusEnum.PENDING.value
                    _logger.info(f"Retrying deduplication_changerequest for change_request: {change_request_id}")
                else:
                    change_request.deduplication_changerequest_status = DeduplicationStatusEnum.FAILED.value
                    change_request.deduplication_changerequest_failure_reason = str(e)
                    _logger.error(f"Max retries exceeded for change_request: {change_request_id}")
                
                session.add(change_request)
                session.commit()
            
            raise e

