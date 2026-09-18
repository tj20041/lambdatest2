```python
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

# Recognized low-level DynamoDB attribute type codes.
KNOWN_DYNAMODB_TYPES = {"S", "N", "BOOL", "NULL", "M", "L", "SS", "NS", "B", "BS"}


class StreamMetricsBuffer:
    def __init__(self):
        self.inserted_count = 0
        self.modified_count = 0
        self.deleted_count = 0
        self.failed_count = 0
        self.aggregated_volume = 0.0

    def dump_metrics(self) -> Dict[str, Any]:
        return {
            "inserts": self.inserted_count,
            "modifications": self.modified_count,
            "deletions": self.deleted_count,
            "failed_records": self.failed_count,
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
        elif data_type in ("SS", "NS"):
            return set(value)
        elif data_type in ("B", "BS"):
            return value
        else:
            # Fail fast with a descriptive error instead of silently returning
            # an unrecognized/raw wrapper that would corrupt downstream logic.
            logger.error(
                f"Encountered unrecognized DynamoDB attribute type code: '{data_type}' "
                f"in payload: {dynamo_val}"
            )
            raise ValueError(
                f"Unrecognized DynamoDB attribute type code '{data_type}'. "
                f"Expected one of: {sorted(KNOWN_DYNAMODB_TYPES)}"
            )
    return None

def parse_record_envelope(raw_image: Dict[str, Any]) -> Dict[str, Any]:
    """Unmarshals a full DynamoDB stream record image (NewImage/OldImage) into
    a plain Python dict. unmarshal_dynamodb_value() already fully resolves
    nested M (map) and L (list) structures recursively, so every top-level
    attribute simply needs a single call - there is no need (and it is
    incorrect) to re-iterate over the already-resolved scalar value.
    """
    output = {}
    for key, val_wrapper in raw_image.items():
        output[key] = unmarshal_dynamodb_value(val_wrapper)

    return output

class StreamProcessorEngine:
    def __init__(self, metrics: StreamMetricsBuffer):
        self.metrics = metrics

    def process_insert(self, unmarshaled_record: Dict[str, Any]) -> None:
        logger.info(f"Processing INSERT event for entity ID: {unmarshaled_record.get('id')}")
        self.metrics.inserted_count += 1
        amount = unmarshaled_record.get("transaction_amount", 0.0)
        self.metrics.aggregated_volume += float(amount)

    def process_modify(self, old_record: Dict[str, Any], new_record: Dict[str, Any]) -> None:
        logger.info(f"Processing MODIFY event for entity ID: {new_record.get('id')}")
        self.metrics.modified_count += 1
        delta = new_record.get("transaction_amount", 0.0) - old_record.get("transaction_amount", 0.0)
        self.metrics.aggregated_volume += float(delta)

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
            },
            {
                "eventID": "101928374829103",
                "eventName": "INSERT",
                "eventVersion": "1.1",
                "eventSource": "aws:dynamodb",
                "awsRegion": "us-east-1",
                "dynamodb": {
                    "ApproximateCreationDateTime": 1710002200,
                    "Keys": {
                        "id": {"S": "TX-90292"}
                    },
                    "NewImage": {
                        "id": {"S": "TX-90292"},
                        "account_id": {"S": "ACC-552"},
                        "transaction_amount": {"N": "12.75"},
                        "status": {"S": "COMPLETED"}
                    },
                    "SequenceNumber": "400000000000002",
                    "SizeBytes": 96,
                    "StreamViewType": "NEW_AND_OLD_IMAGES"
                }
            }
        ]
    }

    records = synthetic_stream_event.get("Records", [])
    logger.info(f"Batch contains {len(records)} stream events")

    for record in records:
        event_id = record.get("eventID", "UNKNOWN")
        event_name = record.get("eventName")
        ddb_data = record.get("dynamodb", {})

        logger.info(f"Parsing envelope for event ID: {event_id}")

        # Per-record isolation: a single malformed/edge-case record must not
        # abort processing for the rest of the batch or lose the aggregated
        # metrics collected so far.
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
                logger.warning(f"Skipping unrecognized eventName '{event_name}' for event ID: {event_id}")

        except Exception as exc:
            metrics.failed_count += 1
            logger.error(
                f"Failed to process record with event ID '{event_id}' "
                f"(eventName={event_name}): {exc}",
                exc_info=True
            )
            continue

    summary_metrics = metrics.dump_metrics()
    logger.info(f"Batch processing completed. Metrics: {summary_metrics}")

    return {
        "statusCode": 200,
        "batch_size": len(records),
        "execution_summary": summary_metrics
    }
```

**Explanation of the fix:**

The crash occurred because `parse_record_envelope()` incorrectly assumed every attribute returned by `unmarshal_dynamodb_value()` was still a nested `{"TYPE": value}` wrapper dict. In reality, `unmarshal_dynamodb_value()` already fully and recursively resolves DynamoDB's low-level attribute format into native Python types (str, int, float, bool, None, list, set). The old code only special-cased `M` (map) attributes and, for every other type (S, N, BOOL, etc.), fell into an `else` branch that called `.items()` on the already-resolved scalar (e.g. the string `"TX-90291"` for the `id` field), producing `AttributeError: 'str' object has no attribute 'items'` on the very first record.

Fix summary:
1. **`parse_record_envelope()`** — removed the flawed "second pass" branch entirely. Since `unmarshal_dynamodb_value()` already resolves nested M/L structures recursively, each top-level key now simply gets `output[key] = unmarshal_dynamodb_value(val_wrapper)`.
2. **`unmarshal_dynamodb_value()`** — added defensive validation: if the dict's type code isn't one of the recognized DynamoDB type codes, it now raises a clear, descriptive `ValueError` (and logs it) instead of silently mis-happening or crashing further downstream with a confusing `AttributeError`. Also added handling for `NS`/`B`/`BS` type codes for completeness/robustness.
3. **`lambda_handler()`** — wrapped each record's processing logic in a `try/except` block that logs the failing record's `eventID`/`eventName` and continues to the next record, so one bad record no longer aborts the entire batch. Added a `failed_count` metric to `StreamMetricsBuffer` (and its `dump_metrics()` output) to surface partial-batch failures for observability/alerting, consistent with the recommendation to alert on repeated failures.
4. Added a second synthetic record with only scalar (`S`/`N`) top-level attributes and no nested `M` map, matching the exact regression scenario described in the root-cause analysis, to validate the fix against the failure mode that wasn't previously covered.