import importlib
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from openg2p_registry_core.models import G2PRegisterDefinition

_DOMAIN_MODELS_MODULE = "openg2p_registry_extensions.register_domain.models"


def find_register_row_by_identifier(
    session: Session, register_id: str, record_identifier: str
) -> Any | None:
    register_definition = session.get(G2PRegisterDefinition, register_id)
    if not register_definition:
        return None
    model_module = importlib.import_module(_DOMAIN_MODELS_MODULE)
    register_class = getattr(
        model_module, f"G2PRegister{register_definition.register_mnemonic}"
    )
    row = session.execute(
        select(register_class).where(
            register_class.functional_record_id == record_identifier
        )
    ).scalar_one_or_none()
    if row:
        return row
    if hasattr(register_class, "foundational_id"):
        return (
            session.execute(
                select(register_class).where(
                    register_class.foundational_id == record_identifier
                )
            ).scalar_one_or_none()
        )
    return None
