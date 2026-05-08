# ruff: noqa: E402

import logging

from .config import Settings
_config = Settings.get_config()

from celery import Celery
from openg2p_fastapi_common.app import Initializer as BaseInitializer
from openg2p_fastapi_common.exception import BaseExceptionHandler

_logger = logging.getLogger(_config.logging_default_logger_name)


class Initializer(BaseInitializer):
    def initialize(self, **kwargs):
        super().init_logger()
        super().init_app()
        BaseExceptionHandler()


celery_app = Celery(
    "g2p_registry_celery_beat_producer",
    broker=_config.celery_broker_url,
    backend=_config.celery_backend_url,
    include=["openg2p_registry_celery_beat_producers.tasks"],
)

celery_app.conf.beat_schedule = {
    "ingest_data_classification_beat_producer": {
        "task": "ingest_data_classification_beat_producer",
        "schedule": (
            _config.ingest_data_classification_beat_producer_frequency or
            _config.default_beat_producer_frequency
        ),
    },
    "ingest_data_transformation_beat_producer": {
        "task": "ingest_data_transformation_beat_producer",
        "schedule": (
            _config.data_transformation_beat_producer_frequency or
            _config.default_beat_producer_frequency
        ),
    },
    "ingest_data_beat_producer": {
        "task": "ingest_data_beat_producer",
        "schedule": (
            _config.ingest_data_beat_producer_frequency or
            _config.default_beat_producer_frequency
        ),
    },
    "outgest_topic_register_beat_producer": {
        "task": "outgest_topic_register_beat_producer",
        "schedule": (
            _config.outgest_topic_register_beat_producer_frequency or
            _config.default_beat_producer_frequency
        ),
    },
    "outgest_data_transformation_beat_producer": {
        "task": "outgest_data_transformation_beat_producer",
        "schedule": (
            _config.data_transformation_beat_producer_frequency or
            _config.default_beat_producer_frequency
        ),
    },
    "outgest_data_publish_beat_producer": {
        "task": "outgest_data_publish_beat_producer",
        "schedule": (
            _config.outgest_data_publish_beat_producer_frequency or
            _config.default_beat_producer_frequency
        ),
    },
    "deduplication_register_beat_producer": {
        "task": "deduplication_register_beat_producer",
        "schedule": (
            _config.deduplication_beat_producer_frequency or
            _config.default_beat_producer_frequency
        ),
    },
    "deduplication_change_request_beat_producer": {
        "task": "deduplication_change_request_beat_producer",
        "schedule": (
            _config.deduplication_beat_producer_frequency or
            _config.default_beat_producer_frequency
        ),
    },
    "intake_form_register_ingest_beat_producer": {
        "task": "intake_form_register_ingest_beat_producer",
        "schedule": (
            _config.intake_form_register_ingest_beat_producer_frequency or
            _config.default_beat_producer_frequency
        ),
    },
    "functional_id_allocation_beat_producer": {
        "task": "functional_id_allocation_beat_producer",
        "schedule": (
            _config.functional_id_allocation_beat_producer_frequency or
            _config.default_beat_producer_frequency
        ),
    },
    "functional_id_updation_beat_producer": {
        "task": "functional_id_updation_beat_producer",
        "schedule": (
            _config.functional_id_updation_beat_producer_frequency or
            _config.default_beat_producer_frequency
        ),
    },
    "completion_score_beat_producer": {
        "task": "completion_score_beat_producer",
        "schedule": (
            _config.completion_score_beat_producer_frequency or
            _config.default_beat_producer_frequency
        ),
    },
    "score_compute_beat_producer": {
        "task": "score_compute_beat_producer",
        "schedule": (
            _config.score_compute_beat_producer_frequency or
            _config.default_beat_producer_frequency
        ),
    },
    "deduplication_intake_forms_vs_register_beat_producer": {
        "task": "deduplication_intake_forms_vs_register_beat_producer",
        "schedule": (
            _config.deduplication_beat_producer_frequency or
            _config.default_beat_producer_frequency
        ),
    },
    "deduplication_intake_forms_vs_intake_forms_beat_producer": {
        "task": "deduplication_intake_forms_vs_intake_forms_beat_producer",
        "schedule": (
            _config.deduplication_beat_producer_frequency or
            _config.default_beat_producer_frequency
        ),
    },
    "import_file_process_beat_producer": {
        "task": "import_file_process_beat_producer",
        "schedule": (
            _config.import_file_process_beat_producer_frequency
            or _config.default_beat_producer_frequency
        ),
    },
}
celery_app.conf.timezone = "UTC"
