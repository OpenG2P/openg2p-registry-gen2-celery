import csv
import io
import json
import uuid
import logging
import asyncio
from datetime import datetime
from typing import Any
import openpyxl
import xmltodict

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

SUPPORTED_FORMATS = [".csv", ".tsv", ".xlsx", ".xls", ".json", ".jsonl", ".xml"]

@celery_app.task(name="import_file_process_worker", bind=True, max_retries=3)
def import_file_process_worker(self, import_file_id: str):
    """
    Worker that processes an import file record-by-record.

    Steps:
        1. Fetch queue item and resolve data model.
        2. Download file from Minio.
        3. Parse file into records (CSV / TSV / Excel / JSON / JSONL / XML).
        4. For each record, check idempotency log and call ingest pipeline.
        5. Write per-record log and commit.
        6. Update queue item status on success or failure.
    """
    _logger.info(
        "Import file process worker started for import_file_id=%s", import_file_id
    )

    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        queue_item: ImportFileProcessQueue | None = None
        try:
            queue_item = session.get(ImportFileProcessQueue, import_file_id)
            if not queue_item:
                raise Exception(
                    f"Import file queue item not found for import_file_id: {import_file_id}"
                )

            data_model: DataModel | None = session.get(DataModel, queue_item.data_model_id)
            if not data_model:
                raise Exception(
                    f"DataModel not found for data_model_id: {queue_item.data_model_id}"
                )

            # Download file from Minio
            minio_client = MinioClient.get_component()
            file_content: bytes = minio_client.get_object(
                object_name=queue_item.document_store_id,
                bucket_name=_config.import_file_bucket_name,
            )

            # Parse file into records
            all_records = parse_file_to_records(file_content, queue_item.document_store_id)
            total_records_in_file = len(all_records)
            total_records_ingested = 0

            _logger.info(
                "Parsed import_file_id=%s total_records=%s",
                import_file_id,
                total_records_in_file,
            )

            ingest_service = G2PIngestService.get_component()

            for record_number, record_data in enumerate(all_records, start=1):

                # Idempotency check
                existing_log: ImportFileProcessLog | None = (
                    session.execute(
                        select(ImportFileProcessLog).where(
                            ImportFileProcessLog.document_store_id == queue_item.document_store_id,
                            ImportFileProcessLog.record_number == record_number,
                        )
                    )
                    .scalar_one_or_none()
                )
                if existing_log:
                    _logger.debug(
                        "Skipping already-ingested record_number=%s import_file_id=%s",
                        record_number,
                        import_file_id,
                    )
                    continue

                # Build ingest payload
                ingest_data = {
                    "headers": {
                        "message_id": uuid.uuid4().hex,
                        "sender_id": _config.import_file_sender_id,
                        "signature": _config.import_file_signature,
                    },
                    "body": record_data,
                }

                # Call ingest pipeline
                correlation_id, _ = _loop.run_until_complete(
                    ingest_service.ingest_data(
                        data_model.data_model_mnemonic,
                        ingest_data,
                        register_id=queue_item.register_id,
                        intake_form_id=queue_item.intake_form_id,
                    )
                )
                _logger.debug(
                    "Ingested record_number=%s import_file_id=%s correlation_id=%s",
                    record_number,
                    import_file_id,
                    correlation_id,
                )

                # Write per-record log
                session.add(
                    ImportFileProcessLog(
                        import_file_id=import_file_id,
                        document_store_id=queue_item.document_store_id,
                        record_number=record_number,
                        ingestion_timestamp=datetime.now(),
                    )
                )
                session.commit()
                total_records_ingested += 1

            # Mark queue item as processed
            queue_item.number_of_records_present = total_records_in_file
            queue_item.number_of_records_ingested = total_records_ingested
            queue_item.intake_form_ingestion_status = ProcessStatusEnum.PROCESSED.value
            queue_item.intake_form_ingestion_timestamp = datetime.now()
            queue_item.intake_form_ingestion_error = None
            session.add(queue_item)
            session.commit()

            _logger.info(
                "Completed import_file_id=%s records_in_file=%s records_ingested=%s",
                import_file_id,
                total_records_in_file,
                total_records_ingested,
            )

        except Exception as e:
            _logger.error(
                "Error in import_file_process_worker for import_file_id=%s: %s",
                import_file_id,
                str(e),
                exc_info=True,
            )
            session.rollback()

            if queue_item:
                queue_item.intake_form_ingestion_timestamp = datetime.now()
                queue_item.intake_form_ingestion_error = str(e)
                if (queue_item.intake_form_ingestion_attempts or 0) >= _config.worker_max_attempts:
                    queue_item.intake_form_ingestion_status = ProcessStatusEnum.FAILED.value
                else:
                    queue_item.intake_form_ingestion_status = ProcessStatusEnum.PENDING.value
                session.add(queue_item)
                session.commit()

            raise

    _logger.info(
        "Import file process worker finished for import_file_id=%s", import_file_id
    )

# ---------------------------------------------------------------------------
# File parsers
# ---------------------------------------------------------------------------

def parse_file_to_records(file_content: bytes, filename: str) -> list[dict[str, Any]]:
    """
    Parse any supported file format into a flat list of row dicts.

    Supported formats:
        - CSV   (.csv)
        - TSV   (.tsv)
        - Excel (.xlsx, .xls)
        - JSON  (.json)  — expects a list of objects, or {"data": [...]}
        - JSONL (.jsonl) — expects one JSON object per line
        - XML   (.xml)   — expects repeated sibling elements under a root

    Args:
        file_content: Raw bytes of the file.
        filename:     Original filename (used to detect format by extension).

    Returns:
        List of dicts, one per record.

    Raises:
        ValueError: If the file format is unsupported or content is malformed.
    """
    filename_lower = filename.lower()

    if filename_lower.endswith(".csv"):
        return _parse_csv(file_content, delimiter=",")

    elif filename_lower.endswith(".tsv"):
        return _parse_csv(file_content, delimiter="\t")

    elif filename_lower.endswith((".xlsx", ".xls")):
        return _parse_excel(file_content)

    elif filename_lower.endswith(".json"):
        return _parse_json(file_content)

    elif filename_lower.endswith(".jsonl"):
        return _parse_jsonl(file_content)

    elif filename_lower.endswith(".xml"):
        return _parse_xml(file_content)

    else:
        ext = filename.rsplit(".", 1)[-1] if "." in filename else "unknown"
        raise ValueError(
            f"Unsupported file format '.{ext}'. "
            f"Supported formats: {', '.join(SUPPORTED_FORMATS)}"
        )


def _parse_csv(file_content: bytes, delimiter: str = ",") -> list[dict[str, Any]]:
    """Parse CSV or TSV bytes into a list of row dicts."""
    try:
        text = file_content.decode("utf-8-sig")  # strips BOM if present
        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        records = list(reader)
        _logger.debug("Parsed %d records from CSV/TSV", len(records))
        return records
    except Exception as e:
        raise ValueError(f"Failed to parse CSV/TSV file: {e}") from e


def _parse_excel(file_content: bytes) -> list[dict[str, Any]]:
    """Parse Excel (.xlsx / .xls) bytes into a list of row dicts."""
    try:
        workbook = openpyxl.load_workbook(
            io.BytesIO(file_content), read_only=True, data_only=True
        )
        sheet = workbook.active
        rows = list(sheet.iter_rows(values_only=True))
        workbook.close()

        if not rows:
            return []

        headers = [
            str(cell).strip() if cell is not None else f"column_{i}"
            for i, cell in enumerate(rows[0])
        ]

        records = []
        for row in rows[1:]:
            if all(cell is None for cell in row):  # skip empty rows
                continue
            record = {
                headers[i]: (str(row[i]).strip() if row[i] is not None else "")
                for i in range(len(headers))
            }
            records.append(record)

        _logger.debug("Parsed %d records from Excel", len(records))
        return records
    except Exception as e:
        raise ValueError(f"Failed to parse Excel file: {e}") from e


def _parse_json(file_content: bytes) -> list[dict[str, Any]]:
    """
    Parse JSON bytes into a list of row dicts.

    Accepts:
        - A JSON array:              [ {...}, {...} ]
        - A wrapped object with any top-level list value:
                                     { "data": [...] }
                                     { "records": [...] }
    """
    try:
        parsed = json.loads(file_content.decode("utf-8"))

        if isinstance(parsed, list):
            records = parsed
        elif isinstance(parsed, dict):
            list_key = next(
                (k for k, v in parsed.items() if isinstance(v, list)), None
            )
            if not list_key:
                raise ValueError(
                    "JSON object has no top-level list. "
                    "Expected format: [{...}] or {\"data\": [{...}]}"
                )
            records = parsed[list_key]
            _logger.debug("Using JSON key '%s' as record list", list_key)
        else:
            raise ValueError("JSON content must be an array or an object containing an array.")

        records = [_flatten_dict(r) for r in records]
        _logger.debug("Parsed %d records from JSON", len(records))
        return records
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON content: {e}") from e


def _parse_jsonl(file_content: bytes) -> list[dict[str, Any]]:
    """
    Parse JSON Lines (.jsonl) bytes into a list of row dicts.

    Expects one valid JSON object per line, with blank lines ignored, e.g.:
        {"name": "Alice", "age": 30}
        {"name": "Bob",   "age": 25}
    """
    try:
        records = []
        for line_number, raw_line in enumerate(
            file_content.decode("utf-8").splitlines(), start=1
        ):
            line = raw_line.strip()
            if not line:          # skip blank lines
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"Invalid JSON on line {line_number}: {e}"
                ) from e
            if not isinstance(obj, dict):
                raise ValueError(
                    f"Expected a JSON object on line {line_number}, "
                    f"got {type(obj).__name__}"
                )
            records.append(_flatten_dict(obj))

        _logger.debug("Parsed %d records from JSONL", len(records))
        return records
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"Failed to parse JSONL file: {e}") from e


def _parse_xml(file_content: bytes) -> list[dict[str, Any]]:
    """
    Parse XML bytes into a list of row dicts.

    Expects repeated sibling elements under a single root, e.g.:
        <records>
            <record><name>Alice</name><age>30</age></record>
            <record><name>Bob</name><age>25</age></record>
        </records>
    """
    try:
        parsed = xmltodict.parse(file_content.decode("utf-8"))

        root = parsed
        if len(parsed) == 1:
            root = next(iter(parsed.values()))

        list_key = next(
            (k for k, v in root.items() if isinstance(v, list)), None
        )

        if list_key:
            records = root[list_key]
        elif isinstance(root, dict):
            records = [root]  # single record — wrap in list
        else:
            raise ValueError("Could not locate a list of records in the XML structure.")

        records = [_flatten_dict(r) for r in records]
        _logger.debug("Parsed %d records from XML", len(records))
        return records
    except Exception as e:
        raise ValueError(f"Failed to parse XML file: {e}") from e


def _flatten_dict(
    d: dict,
    parent_key: str = "",
    sep: str = ".",
) -> dict[str, Any]:
    """
    Recursively flatten a nested dict into dot-notation keys.

    Example:
        {"name": {"first": "Alice", "last": "Smith"}, "age": 30}
        → {"name.first": "Alice", "name.last": "Smith", "age": 30}
    """
    items: list[tuple[str, Any]] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(_flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)