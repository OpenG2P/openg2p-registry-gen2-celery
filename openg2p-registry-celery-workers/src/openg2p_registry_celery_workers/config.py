from openg2p_registry_extensions.config import Settings as ExtSettings
from pydantic_settings import SettingsConfigDict

from . import __version__


class Settings(ExtSettings):
    model_config = SettingsConfigDict(
        env_prefix="registry_celery_workers_", env_file=".env", extra="allow"
    )
    openapi_title: str = "OpenG2P Registry Celery Workers"
    openapi_description: str = """
        Celery workers for OpenG2P Registry
        ***********************************
        Further details goes here
        ***********************************
        """
    openapi_version: str = __version__

    # Registry Database
    db_username: str = "postgres"
    db_password: str = "postgres"
    db_hostname: str = "localhost"
    db_port: int = 5432
    db_dbname: str = "registrydb"

    # Celery Configuration
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_backend_url: str = "redis://localhost:6379/0"
    worker_queue: str = "celery_jobs_worker_queue"

    batch_size: int = 2000
    worker_max_attempts: int = 5

    functional_id_generation_url: str = "http://functional-id-generation-service-url/v1"
    id_generation_allocation_path: str = "/idgenerator/{id_type}/id"
    id_generation_updation_path: str = ""
