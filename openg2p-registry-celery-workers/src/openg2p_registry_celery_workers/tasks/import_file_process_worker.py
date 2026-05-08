import csv
import io
import logging
import asyncio
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from openg2p_registry_core.helpers import MinioBucketEnum, MinioClient
from openg2p_registry_core.models import (
    DataModel,
    ImportFileProcessLog,
    ImportFileProcessQueue,
    ProcessStatusEnum,
)
from openg2p_registry_core.services import G2PIngestService

from ..app import celery_app
from ..config import Settings
from ..engine import Engine

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()
_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_loop)


@celery_app.task(name="import_file_process_worker", bind=True, max_retries=3)
def import_file_process_worker(self, import_file_id: str):
    """
    Worker that processes a CSV import file record-by-record:
    - Reads file from Minio using document_store_id
    - For each record, checks import_file_process_log (idempotency)
    - Calls ingest pipeline (core ingest service)
    - Writes import_file_process_log per record (commit per record)
    """
    _logger.info("Import file process worker started for import_file_id=%s", import_file_id)

    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        queue_row: ImportFileProcessQueue | None = None
        try:
            queue_row = session.get(ImportFileProcessQueue, import_file_id)
            if not queue_row:
                raise ValueError(f"Import file '{import_file_id}' not found in queue")

            # Resolve data_model mnemonic (ingest service uses mnemonic)
            data_model: DataModel | None = session.get(DataModel, queue_row.data_model_id)
            if not data_model:
                raise ValueError(f"DataModel '{queue_row.data_model_id}' not found")

            # Download CSV bytes from Minio (document_store_id is object name)

            # TODO: Use bucket name
            minio_client = MinioClient.get_component()
            csv_bytes = minio_client.get_object(
                queue_row.document_store_id
            )
            csv_text = csv_bytes.decode("utf-8-sig")

            reader = csv.DictReader(io.StringIO(csv_text))
            total_present = 0
            total_ingested = 0

            ingest_service = G2PIngestService.get_component()

            for record_number, row in enumerate(reader, start=1):
                total_present += 1

                # Idempotency: skip if already logged
                existing = (
                    session.execute(
                        select(ImportFileProcessLog).where(
                            ImportFileProcessLog.document_store_id == queue_row.document_store_id,
                            ImportFileProcessLog.record_number == record_number,
                        )
                    )
                    .scalars()
                    .first()
                )
                if existing:
                    continue

                # Build ingest payload (minimal: row as JSON body)
                ingest_data = {"headers": {}, "body": row}

                # Call ingest pipeline (bypasses classification when register/intake ids are provided)
                correlation_id, _ = _loop.run_until_complete(
                    ingest_service.ingest_data(
                        data_model.data_model_mnemonic,
                        ingest_data,
                        register_id=queue_row.register_id,
                        intake_form_id=queue_row.intake_form_id,
                    )
                )
                _logger.debug(
                    "Ingested record_number=%s import_file_id=%s correlation_id=%s",
                    record_number,
                    import_file_id,
                    correlation_id,
                )

                session.add(
                    ImportFileProcessLog(
                        import_file_id=import_file_id,
                        document_store_id=queue_row.document_store_id,
                        record_number=record_number,
                        ingestion_timestamp=datetime.now(),
                    )
                )
                session.commit()  # commit record by record
                total_ingested += 1

            queue_row.number_of_records_present = total_present
            queue_row.number_of_records_ingested = total_ingested
            queue_row.intake_form_ingestion_status = ProcessStatusEnum.PROCESSED.value
            queue_row.intake_form_ingestion_timestamp = datetime.now()
            queue_row.intake_form_ingestion_error = None
            session.add(queue_row)
            session.commit()

            _logger.info(
                "Completed import_file_id=%s present=%s ingested=%s",
                import_file_id,
                total_present,
                total_ingested,
            )
        except Exception as e:
            _logger.error(
                "Error during processing import_file_process_worker for import_file_id %s: %s",
                import_file_id,
                str(e),
                exc_info=True,
            )
            session.rollback()

            if queue_row:
                queue_row.intake_form_ingestion_timestamp = datetime.now()
                queue_row.intake_form_ingestion_error = str(e)
                # retries handled by beat attempts; mark pending unless max attempts exceeded
                if (queue_row.intake_form_ingestion_attempts or 0) >= _config.worker_max_attempts:
                    queue_row.intake_form_ingestion_status = ProcessStatusEnum.FAILED.value
                else:
                    queue_row.intake_form_ingestion_status = ProcessStatusEnum.PENDING.value
                session.add(queue_row)
                session.commit()

            raise

    _logger.info("Import file process worker completed for import_file_id=%s", import_file_id)