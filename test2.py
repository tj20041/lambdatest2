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
        elif data_type == "NS":
            # Number Set - convert each numeric string to int/float
            return set(float(v) if "." in v else int(v) for v in value)
        elif data_type == "BS":
            # Binary Set - leave as list of base64-encoded strings
            return list(value)
        elif data_type == "B":
            # Binary - leave as the raw (base64-encoded) string/bytes representation
            return value
        else:
            logger.warning(f"Encountered unrecognized DynamoDB attribute type '{data_type}' with value {value!r}; returning raw value as-is")
            return value
    return None

def parse_record_envelope(raw_image: Dict[str, Any]) -> Dict[str, Any]:
    """Converts a DynamoDB Stream NewImage/OldImage envelope (top-level dict of
    {attr_name: {type_tag: value}}) into a plain Python dict.

    unmarshal_dynamodb_value() already fully and recursively converts every
    supported DynamoDB type wrapper (S, N, BOOL, NULL, M, L, SS, NS, BS, B)
    into native Python types, so no second manual unmarshal pass is needed
    or correct here. Attempting a second .items() pass on an already-unwrapped
    scalar (str/int/float/bool) is what previously caused:
    AttributeError: 'str' object has no attribute 'items'
    """
    output = {}
    for key, val_wrapper in raw_image.items():
        if not (isinstance(val_wrapper, dict) and len(val_wrapper) == 1):
            logger.warning(
                f"Attribute '{key}' does not conform to expected single-key DynamoDB "
                f"type-wrapper schema (got: {val_wrapper!r}); passing through unchanged"
            )
            output[key] = val_wrapper
            continue

        output[key] = unmarshal_dynamodb_value(val_wrapper)

    return output

class StreamProcessorEngine:
    def __init__(self, metrics: StreamMetricsBuffer):
        self.metrics = metrics

    def process_insert(self, unmarshaled_record: Dict[str, Any]) -> None:
        logger.info(f"Processing INSERT event for entity ID: {unmarshaled_record.get('id')}")
        self.metrics.inserted_count += 1
        amount = unmarshaled_record.get("transaction_amount", 0.0)
        try:
            self.metrics.aggregated_volume += float(amount)
        except (TypeError, ValueError):
            logger.warning(
                f"Could not coerce transaction_amount={amount!r} to float for entity "
                f"ID {unmarshaled_record.get('id')}; skipping volume contribution"
            )

    def process_modify(self, old_record: Dict[str, Any], new_record: Dict[str, Any]) -> None:
        logger.info(f"Processing MODIFY event for entity ID: {new_record.get('id')}")
        self.metrics.modified_count += 1
        new_amount = new_record.get("transaction_amount", 0.0)
        old_amount = old_record.get("transaction_amount", 0.0)
        try:
            delta = float(new_amount) - float(old_amount)
            self.metrics.aggregated_volume += delta
        except (TypeError, ValueError):
            logger.warning(
                f"Could not coerce transaction_amount old={old_amount!r} new={new_amount!r} "
                f"to float for entity ID {new_record.get('id')}; skipping volume delta"
            )

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

    # Use the real Lambda event payload when provided (standard DynamoDB Streams
    # trigger shape), falling back to the synthetic sample for local/manual invocation.
    records = event.get("Records") if isinstance(event, dict) and event.get("Records") else synthetic_stream_event.get("Records", [])
    logger.info(f"Batch contains {len(records)} stream events")

    batch_item_failures: List[Dict[str, str]] = []

    for record in records:
        event_id = record.get("eventID", "UNKNOWN")
        try:
            event_name = record.get("eventName")
            ddb_data = record.get("dynamodb", {})

            logger.info(f"Parsing envelope for event ID: {event_id}")

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
                logger.warning(f"Skipping unrecognized eventName '{event_name}' for event ID: {event_id}")

        except Exception:
            logger.exception(f"Failed to process stream record with event ID: {event_id}; marking as batch item failure")
            batch_item_failures.append({"itemIdentifier": event_id})
            continue

    summary_metrics = metrics.dump_metrics()
    logger.info(f"Batch processing completed. Metrics: {summary_metrics}")

    response: Dict[str, Any] = {
        "statusCode": 200,
        "batch_size": len(records),
        "execution_summary": summary_metrics
    }

    if batch_item_failures:
        # Standard shape expected by Lambda Event Source Mapping for DynamoDB Streams
        # partial batch response, so only failed records are retried.
        response["batchItemFailures"] = batch_item_failures

    return response
