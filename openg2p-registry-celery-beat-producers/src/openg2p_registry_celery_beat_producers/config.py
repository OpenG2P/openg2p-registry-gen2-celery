from openg2p_registry_extensions.config import Settings as ExtSettings
from pydantic_settings import SettingsConfigDict

from typing import Optional

from . import __version__


class Settings(ExtSettings):
    model_config = SettingsConfigDict(
        env_prefix="registry_celery_beat_", env_file=".env", extra="allow"
    )
    openapi_title: str = "OpenG2P Registry Celery Beat Producers"
    openapi_description: str = """
        Celery Beat Producers for OpenG2P Registry
        ***********************************
        Further details goes here
        ***********************************
        """
    openapi_version: str = __version__

    # Registry Database
    db_driver: str = "postgresql"
    db_username: str = "postgres"
    db_password: str = "password"
    db_hostname: str = "localhost"
    db_port: int = 5432
    db_dbname: str = "registrydb"

    # Celery Configuration
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_backend_url: str = "redis://localhost:6379/0"
    worker_queue: str = "registry_worker_queue"

    batch_size: int = 2000
    no_of_tasks_to_process: int = 4
    default_beat_producer_frequency: int = 20

    data_transformation_beat_producer_frequency: Optional[int] = None           # ingest & outgest

    ingest_data_beat_producer_frequency: Optional[int] = None                   # ingest
    ingest_data_classification_beat_producer_frequency: Optional[int] = None    # ingest

    outgest_data_publish_beat_producer_frequency: Optional[int] = None          # outgest
    outgest_topic_register_beat_producer_frequency: Optional[int] = None        # outgest

    deduplication_beat_producer_frequency: Optional[int] = None                 # deduplication

    intake_form_register_ingest_beat_producer_frequency: Optional[int] = None     # intake_form register ingest
    functional_id_allocation_beat_producer_frequency: Optional[int] = None       # functional id allocation
    functional_id_updation_beat_producer_frequency: Optional[int] = None         # functional id updation
    score_compute_beat_producer_frequency: Optional[int] = None                 # score computation
    completion_score_beat_producer_frequency: Optional[int] = None               # completion score
    import_file_process_beat_producer_frequency: Optional[int] = None           # import file processing
