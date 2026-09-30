import base64
import datetime
import json
import logging
import sys
import time
from typing import Any, Dict, List, Optional, Union

# ---------------------------------------------------------------------------
# Diagnostics & Telemetry
# ---------------------------------------------------------------------------
logger = logging.getLogger("dynamodb_stream_aggregator")
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s - %(name)s - %(message)s"))
logger.handlers = [stream_handler]

class StreamMetricsBuffer:
    def __init__(self):
        self.inserted_count = 0
        self.modified_count = 0
        self.deleted_count = 0
        self.aggregated_volume = 0.0

    def dump_metrics(self) -> Dict[str, Any]:
        return {
            "inserts": self.inserted_count,
            "modifications": self.modified_count,
            "deletions": self.deleted_count,
            "total_volume_processed": round(self.aggregated_volume, 2),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }

# ---------------------------------------------------------------------------
# Low-Level DynamoDB Stream Parser Framework
# ---------------------------------------------------------------------------
def unmarshal_dynamodb_value(dynamo_val: Dict[str, Any]) -> Any:
    """Recursively converts low-level DynamoDB attribute format into standard Python objects."""
    if not isinstance(dynamo_val, dict):
        return dynamo_val

    # Defensive check: a well-formed DynamoDB attribute wrapper dict should contain
    # exactly one type key (e.g. {'S': '...'}). If more than one key is present,
    # the wrapper is malformed and we only resolve the first type, but we log a
    # warning so this doesn't silently mask upstream schema drift.
    if len(dynamo_val) != 1:
        logger.warning(f"Malformed DynamoDB attribute wrapper with {len(dynamo_val)} keys: {dynamo_val}")

    for data_type, value in dynamo_val.items():
        if data_type == "S":
            return str(value)
        elif data_type == "N":
            return float(value) if "." in value else int(value)
        elif data_type == "BOOL":
            return bool(value)
        elif data_type == "NULL":
            return None
        elif data_type == "M":
            # Nested Map parsing
            parsed_map = {}
            for nested_key, nested_val in value.items():
                parsed_map[nested_key] = unmarshal_dynamodb_value(nested_val)
            return parsed_map
        elif data_type == "L":
            # Nested List parsing
            return [unmarshal_dynamodb_value(elem) for elem in value]
        elif data_type == "SS":
            return set(value)
        else:
            return value
    return None

def parse_record_envelope(raw_image: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converts a DynamoDB Stream record image (NewImage/OldImage) whose values are
    low-level DynamoDB-typed attribute wrappers (e.g. {'S': 'value'}) into a
    standard Python dictionary.

    unmarshal_dynamodb_value already fully and recursively resolves every
    supported DynamoDB attribute type (S, N, BOOL, NULL, M, L, SS) into native
    Python objects, so no second unwrap pass is required or correct here.
    """
    output = {}
    for key, val_wrapper in raw_image.items():
        unwrapped = unmarshal_dynamodb_value(val_wrapper)
        output[key] = unwrapped

    return output

class StreamProcessorEngine:
    def __init__(self, metrics: StreamMetricsBuffer):
        self.metrics = metrics

    @staticmethod
    def _coerce_amount(raw_amount: Any, record_id: Optional[str] = None) -> float:
        """Safely coerce a transaction_amount value to float, guarding against schema drift
        (e.g. transaction_amount arriving as a non-numeric string or unexpected type)."""
        if isinstance(raw_amount, (int, float)) and not isinstance(raw_amount, bool):
            return float(raw_amount)
        if isinstance(raw_amount, str):
            try:
                return float(raw_amount)
            except ValueError:
                logger.warning(
                    f"transaction_amount '{raw_amount}' for entity ID {record_id} is not numeric; treating as 0.0"
                )
                return 0.0
        logger.warning(
            f"Unexpected transaction_amount type {type(raw_amount)} for entity ID {record_id}; treating as 0.0"
        )
        return 0.0

    def process_insert(self, unmarshaled_record: Dict[str, Any]) -> None:
        record_id = unmarshaled_record.get("id")
        logger.info(f"Processing INSERT event for entity ID: {record_id}")
        self.metrics.inserted_count += 1
        amount = self._coerce_amount(unmarshaled_record.get("transaction_amount", 0.0), record_id)
        self.metrics.aggregated_volume += amount

    def process_modify(self, old_record: Dict[str, Any], new_record: Dict[str, Any]) -> None:
        record_id = new_record.get("id")
        logger.info(f"Processing MODIFY event for entity ID: {record_id}")
        self.metrics.modified_count += 1
        new_amount = self._coerce_amount(new_record.get("transaction_amount", 0.0), record_id)
        old_amount = self._coerce_amount(old_record.get("transaction_amount", 0.0), record_id)
        delta = new_amount - old_amount
        self.metrics.aggregated_volume += delta

    def process_remove(self, old_record: Dict[str, Any]) -> None:
        logger.info(f"Processing REMOVE event for entity ID: {old_record.get('id')}")
        self.metrics.deleted_count += 1

# ---------------------------------------------------------------------------
# Lambda Handler Entrypoint
# ---------------------------------------------------------------------------
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    metrics = StreamMetricsBuffer()
    engine = StreamProcessorEngine(metrics)

    logger.info("Starting processing batch of DynamoDB stream records...")

    # Realistic simulated DynamoDB Stream event
    synthetic_stream_event = {
        "Records": [
            {
                "eventID": "101928374829102",
                "eventName": "INSERT",
                "eventVersion": "1.1",
                "eventSource": "aws:dynamodb",
                "awsRegion": "us-east-1",
                "dynamodb": {
                    "ApproximateCreationDateTime": 1710002100,
                    "Keys": {
                        "id": {"S": "TX-90291"}
                    },
                    "NewImage": {
                        "id": {"S": "TX-90291"},
                        "account_id": {"S": "ACC-551"},
                        "transaction_amount": {"N": "349.50"},
                        "status": {"S": "COMPLETED"},
                        "metadata": {
                            "M": {
                                "client_ip": {"S": "192.168.1.1"},
                                "device": {"S": "ios"}
                            }
                        }
                    },
                    "SequenceNumber": "400000000000001",
                    "SizeBytes": 182,
                    "StreamViewType": "NEW_AND_OLD_IMAGES"
                }
            }
        ]
    }

    event = event or synthetic_stream_event
    records = event.get("Records", []) if isinstance(event, dict) else []
    if not records:
        records = synthetic_stream_event.get("Records", [])

    logger.info(f"Batch contains {len(records)} stream events")

    batch_item_failures: List[Dict[str, str]] = []

    for record in records:
        event_id = record.get("eventID")
        event_name = record.get("eventName")
        ddb_data = record.get("dynamodb", {})

        logger.info(f"Parsing envelope for event ID: {event_id}")

        try:
            if event_name == "INSERT":
                raw_new = ddb_data.get("NewImage", {})
                parsed_new = parse_record_envelope(raw_new)
                engine.process_insert(parsed_new)

            elif event_name == "MODIFY":
                raw_old = ddb_data.get("OldImage", {})
                raw_new = ddb_data.get("NewImage", {})
                parsed_old = parse_record_envelope(raw_old)
                parsed_new = parse_record_envelope(raw_new)
                engine.process_modify(parsed_old, parsed_new)

            elif event_name == "REMOVE":
                raw_old = ddb_data.get("OldImage", {})
                parsed_old = parse_record_envelope(raw_old)
                engine.process_remove(parsed_old)

            else:
                logger.warning(f"Unhandled eventName '{event_name}' for eventID {event_id}; skipping record")

        except Exception:
            # Isolate failures to a single record so one malformed/unsupported
            # attribute does not abort the entire batch. Track the failing
            # record so DynamoDB Streams event source mapping (with
            # ReportBatchItemFailures enabled) can retry only this record.
            logger.exception(f"Failed to process record with eventID {event_id}")
            if event_id:
                batch_item_failures.append({"itemIdentifier": event_id})

    summary_metrics = metrics.dump_metrics()
    logger.info(f"Batch processing completed. Metrics: {summary_metrics}")
    if batch_item_failures:
        logger.warning(f"{len(batch_item_failures)} record(s) failed processing: {batch_item_failures}")

    response: Dict[str, Any] = {
        "statusCode": 200,
        "batch_size": len(records),
        "execution_summary": summary_metrics
    }

    if batch_item_failures:
        response["batchItemFailures"] = batch_item_failures

    return response
