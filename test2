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
        else:
            return value
    return None

def parse_record_envelope(raw_image: Dict[str, Any]) -> Dict[str, Any]:
    output = {}
    for key, val_wrapper in raw_image.items():
        # First layer unwrapping works
        unwrapped = unmarshal_dynamodb_value(val_wrapper)

        # FAILS HERE: The code attempts to traverse the unwrapped attribute as if it
        # still retains the low-level {'S': '...'} schema wrapper dict
        # If 'unwrapped' is a string or int, unwrapped.items() raises AttributeError
        if isinstance(val_wrapper, dict) and "M" in val_wrapper:
            output[key] = unwrapped
        else:
            # Buggy second unmarshal attempt assumes unwrapped is still a dictionary
            second_pass = {}
            for sub_k, sub_v in unwrapped.items():
                second_pass[sub_k] = sub_v
            output[key] = second_pass

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
            }
        ]
    }

    records = synthetic_stream_event.get("Records", [])
    logger.info(f"Batch contains {len(records)} stream events")

    for record in records:
        event_name = record.get("eventName")
        ddb_data = record.get("dynamodb", {})

        logger.info(f"Parsing envelope for event ID: {record.get('eventID')}")

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

    summary_metrics = metrics.dump_metrics()
    logger.info(f"Batch processing completed successfully. Metrics: {summary_metrics}")

    return {
        "statusCode": 200,
        "batch_size": len(records),
        "execution_summary": summary_metrics
    }
