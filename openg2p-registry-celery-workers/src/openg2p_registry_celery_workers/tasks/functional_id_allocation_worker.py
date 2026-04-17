import importlib
import logging
from datetime import datetime

import httpx
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from openg2p_registry_core.models import (
    G2PFunctionalIdGenerationQueue,
    G2PRegisterDefinition,
)
from openg2p_registry_core.models.g2p_functional_id_generation_queue import (
    ProcessStatusEnum as FunctionalIdGenerationStatusEnum,
)

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="functional_id_allocation_worker")
def functional_id_allocation_worker(queue_id: str):
    _logger.info(f"Starting functional_id_allocation_worker for queue_id: {queue_id}")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        queue_item: G2PFunctionalIdGenerationQueue | None = None
        try:
            queue_item = session.get(G2PFunctionalIdGenerationQueue, queue_id)
            if not queue_item:
                raise Exception(f"Functional ID generation queue item not found: {queue_id}")

            register_definition = session.get(G2PRegisterDefinition, queue_item.register_id)
            if not register_definition:
                raise Exception(
                    f"Register definition not found for register_id: {queue_item.register_id}"
                )

            register_class = _get_register_class(register_definition.register_mnemonic)
            register_record = (
                session.execute(
                    select(register_class).where(
                        register_class.internal_record_id == queue_item.internal_record_id
                    )
                )
            ).scalar_one_or_none()
            if not register_record:
                raise Exception(
                    "Register record not found for "
                    f"internal_record_id: {queue_item.internal_record_id}"
                )

            resolved_affix = _resolve_prefix_suffix(
                register_record, register_definition.register_mnemonic
            )
            resolved_id = _allocate_functional_record_id(register_definition.register_mnemonic)
            functional_record_id = _compose_functional_record_id(
                resolved_affix.prefix,
                resolved_id,
                resolved_affix.suffix,
            )

            register_record.functional_record_id = functional_record_id
            queue_item.resolved_id = resolved_id
            queue_item.resolved_prefix = resolved_affix.prefix
            queue_item.resolved_suffix = resolved_affix.suffix
            queue_item.id_allocation_status = FunctionalIdGenerationStatusEnum.COMPLETED.value
            queue_item.id_updation_status = FunctionalIdGenerationStatusEnum.PENDING.value
            queue_item.id_allocation_no_of_attempts += 1
            queue_item.id_allocation_latest_timestamp = datetime.now()
            queue_item.id_allocation_latest_error_code = None

            session.add(register_record)
            session.add(queue_item)
            session.commit()

        except Exception as e:
            _logger.error(
                f"Error during processing functional_id_allocation_worker for queue_id {queue_id}: {str(e)}"
            )
            session.rollback()

            if queue_item:
                queue_item.id_allocation_no_of_attempts += 1
                queue_item.id_allocation_latest_timestamp = datetime.now()
                queue_item.id_allocation_latest_error_code = str(e)

                if queue_item.id_allocation_no_of_attempts < _config.worker_max_attempts:
                    queue_item.id_allocation_status = FunctionalIdGenerationStatusEnum.PENDING.value
                else:
                    queue_item.id_allocation_status = FunctionalIdGenerationStatusEnum.FAILED.value

                session.add(queue_item)
                session.commit()

            raise e

        _logger.info(
            f"Completed functional_id_allocation_worker for queue_id: {queue_id}"
        )


def _get_register_class(register_mnemonic: str):
    module = importlib.import_module("openg2p_registry_extensions.register_domain.models")
    implementation_class_name = f"G2PRegister{register_mnemonic}"
    return getattr(module, implementation_class_name)


def _resolve_prefix_suffix(register_record, register_mnemonic: str):
    factory_module = importlib.import_module("openg2p_registry_extensions.register_domain.factory")
    factory_class = getattr(factory_module, "G2PIdGeneratorFactory")
    id_generator_factory = factory_class.get_component()
    if not id_generator_factory:
        id_generator_factory = factory_class()

    id_generator_service = id_generator_factory.get_id_generator()
    if not id_generator_service:
        raise Exception(
            f"No ID generator service found for register mnemonic: {register_mnemonic}"
        )

    return id_generator_service.generate_prefix_suffix(register_record, register_mnemonic)


def _allocate_functional_record_id(register_mnemonic: str) -> str:
    allocation_url = _build_functional_id_generation_url(register_mnemonic.lower())
    try:
        response = httpx.post(allocation_url, timeout=30.0)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise Exception(
            _build_http_error_message(exc.response, allocation_url)
        ) from exc
    except httpx.HTTPError as exc:
        raise Exception(
            f"Functional ID allocation request failed for url: {allocation_url}: {str(exc)}"
        ) from exc

    response_json = response.json()
    functional_record_id = response_json.get("response", {}).get("id")
    if not functional_record_id:
        raise Exception(
            "Functional ID generation response did not include response.id "
            f"for url: {allocation_url}"
        )

    return functional_record_id


def _build_functional_id_generation_url(register_mnemonic_lower: str) -> str:
    allocation_path = _config.id_generation_allocation_path.format(
        id_type=register_mnemonic_lower
    )
    return (
        f"{_config.functional_id_generation_url.rstrip('/')}/"
        f"{allocation_path.lstrip('/')}"
    )


def _compose_functional_record_id(
    resolved_prefix: str | None, resolved_id: str, resolved_suffix: str | None
) -> str:
    return f"{resolved_prefix or ''}{resolved_id}{resolved_suffix or ''}"


def _build_http_error_message(response: httpx.Response, allocation_url: str) -> str:
    error_code = None
    try:
        error_payload = response.json()
        errors = error_payload.get("errors") or []
        if errors and isinstance(errors, list):
            first_error = errors[0]
            if isinstance(first_error, dict):
                error_code = first_error.get("code") or first_error.get("errorCode")
            elif isinstance(first_error, str):
                error_code = first_error
    except ValueError:
        error_payload = None

    if error_code:
        return (
            f"Functional ID allocation failed with status {response.status_code} "
            f"and error code {error_code} for url: {allocation_url}"
        )

    return (
        f"Functional ID allocation failed with status {response.status_code} "
        f"for url: {allocation_url}: {response.text}"
    )
