# ruff: noqa: E402

import logging

from .config import Settings

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)

from celery import Celery
from openg2p_registry_core.helpers import MinioClient, TemplateHelper, WebsubHelper
from openg2p_fastapi_common.app import Initializer as BaseInitializer
from openg2p_fastapi_common.exception import BaseExceptionHandler        
from openg2p_registry_core.services import G2PRegisterService
from openg2p_registry_core.services.g2p_register_change_request_service import (
    G2PRegisterChangeRequestService,
)
from openg2p_registry_core.services.g2p_change_request_worker_service import (
    G2PChangeRequestWorkerService,
)
from openg2p_registry_extensions.register_domain.factory import G2PRegisterDomainFactory

class Initializer(BaseInitializer):
    def initialize(self, **kwargs):
        super().initialize()
        BaseExceptionHandler()

        # Services
        G2PRegisterService()
        G2PRegisterChangeRequestService()
        G2PChangeRequestWorkerService()

        # Domain factory (needed for dynamic domain resolution during approvals)
        G2PRegisterDomainFactory()

        # Helpers
        MinioClient(
            _config.minio_endpoint,
            _config.minio_access_key,
            _config.minio_secret_key,
            _config.minio_secure,
            _config.minio_bucket_name,
        )
        TemplateHelper(_config.template_bucket_name)
        WebsubHelper()


celery_app = Celery(
    "g2p_registry_celery_worker",
    broker=_config.celery_broker_url,
    backend=_config.celery_backend_url,
    include=["openg2p_registry_celery_workers.tasks"],
)

celery_app.conf.timezone = "UTC"
