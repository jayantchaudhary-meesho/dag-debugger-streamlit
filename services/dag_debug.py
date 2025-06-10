import grpc
import requests
import json
from typing import Dict, Any, Optional, Union
import debug.debug_pb2 as debug_pb2
import debug.debug_pb2_grpc as debug_pb2_grpc
from google.protobuf.json_format import MessageToDict
import re

# Constants
HTTP_FEED_TYPES = ["catalog_listing_page", "recently_viewed_catalog_recommendation"]
DEFAULT_TIMEOUT = 30
DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "MEESHO-ISO-COUNTRY-CODE": "IN",
}

class MockResponse:
    """Mock response class to maintain consistent interface between HTTP and gRPC responses."""
    def __init__(self, success: bool, results: Optional[Dict[str, str]] = None, error: Optional[str] = None):
        self.Success = success
        self.Results = results or {}
        self.Error = error or ""

# Helper functions for key conversion

def camel_to_snake(name: str) -> str:
    """Convert CamelCase or camelCase to snake_case lowercase."""
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    return s2.lower()

ALIAS_MAPPING = {
    "tenant_ctx": "tenant_context",
    "user_ctx": "user_context",
    "feed_ctx": "feed_context",
}

def convert_keys_snake(obj: Any) -> Any:
    """Recursively convert dict keys to snake_case and apply alias mapping."""
    if isinstance(obj, dict):
        return {ALIAS_MAPPING.get(camel_to_snake(k), camel_to_snake(k)): convert_keys_snake(v) 
                for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_keys_snake(item) for item in obj]
    return obj

def convert_floats_to_ints(obj: Any) -> Any:
    """Recursively convert float values that are integers to int type."""
    if isinstance(obj, dict):
        return {k: convert_floats_to_ints(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_ints(item) for item in obj]
    elif isinstance(obj, float) and obj.is_integer():
        return int(obj)
    return obj

def call_execute_dag_http(
    request_kwargs: Dict[str, Any],
    config_source_type: str,
    user_id: str,
    user_context: str,
    iop_host: str
) -> MockResponse:
    """Handle HTTP requests for catalog_listing_page and recently_viewed_catalog_recommendation."""
    url = f"http://{iop_host}/debug/dag/execute"
    headers = {
        **DEFAULT_HEADERS,
        "MEESHO-USER-ID": user_id,
        "MEESHO-USER-CONTEXT": user_context,
    }

    http_payload: Dict[str, Any] = {}

    # Config kind
    if "ConfigKind" in request_kwargs:
        http_payload["config_kind"] = request_kwargs["ConfigKind"]

    # Data
    if "Data" in request_kwargs:
        data_proto = request_kwargs["Data"]
        data_dict = MessageToDict(data_proto, preserving_proto_field_name=True)
        data_dict = convert_floats_to_ints(data_dict)
        http_payload["Data"] = convert_keys_snake(data_dict)

    # Raw config or Path depending on source type
    if config_source_type == "RawConfigJson" and "RawConfigJson" in request_kwargs:
        try:
            http_payload["raw_config"] = json.loads(request_kwargs["RawConfigJson"])
        except json.JSONDecodeError:
            http_payload["raw_config"] = request_kwargs["RawConfigJson"]
    elif config_source_type == "Selector" and "Selector" in request_kwargs:
        selector_proto = request_kwargs["Selector"]
        selector_dict = MessageToDict(selector_proto, preserving_proto_field_name=True)
        http_payload["Path"] = convert_keys_snake(selector_dict)

    # Meta logging
    http_payload["Meta"] = {"IsLoggingEnabled": True}

    try:
        response = requests.post(url, headers=headers, json=http_payload, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        resp_json = response.json()

        if isinstance(resp_json, dict) and resp_json.get("success") is False:
            return MockResponse(False, error=resp_json.get("error", "Unknown error from service"))

        results_map = {}
        if isinstance(resp_json, dict):
            if "results" in resp_json:
                results_map = {k: json.dumps(v) for k, v in resp_json["results"].items()}
            # Surface debug_config if present for UI consumption
            if "debug_config" in resp_json:
                results_map["debug_config"] = resp_json["debug_config"]

        if not results_map:  # Fallback – put entire response under root
            results_map["root"] = json.dumps(resp_json)

        return MockResponse(True, results=results_map)

    except requests.RequestException as e:
        error_msg = f"Request failed: {str(e)}"
        if hasattr(e, "response") and e.response is not None:
            try:
                error_details = e.response.json()
                error_msg += f"\nDetails: {json.dumps(error_details)}"
            except Exception:
                error_msg += f"\nResponse: {e.response.text}"
        return MockResponse(False, error=error_msg)

def call_execute_dag_grpc(
    request_kwargs: Dict[str, Any],
    user_id: str,
    user_context: str,
    iop_host: str
) -> Union[debug_pb2.ExecuteDAGResponse, MockResponse]:
    """Handle gRPC requests for for_you and catalog_recommendation."""
    try:
        request_data = debug_pb2.ExecuteDAGRequest(**request_kwargs)
        with grpc.insecure_channel(iop_host) as channel:
            stub = debug_pb2_grpc.DAGDebugServiceStub(channel)
            metadata = [
                ("meesho-user-id", user_id),
                ("meesho-user-context", user_context)
            ]
            response = stub.ExecuteDAG(request_data, metadata=metadata)
            # Extract debug_config from gRPC response if present
            results_map = {}
            if hasattr(response, "results"):
                results_map = {k: v for k, v in response.results.items()}
            if hasattr(response, "debug_config") and response.debug_config:
                results_map["debug_config"] = response.debug_config
            return MockResponse(True, results=results_map)
    except grpc.RpcError as e:
        return MockResponse(False, error=f"gRPC Error: {str(e)}")

def call_execute_dag(
    request_kwargs: Dict[str, Any],
    config_source_type: str,
    user_id: str,
    user_context: str,
    feed_type: str,
    iop_host: str
) -> Union[debug_pb2.ExecuteDAGResponse, MockResponse]:
    """Route to HTTP or gRPC based on feed_type."""
    if feed_type in HTTP_FEED_TYPES:
        return call_execute_dag_http(request_kwargs, config_source_type, user_id, user_context, iop_host)
    return call_execute_dag_grpc(request_kwargs, user_id, user_context, iop_host) 