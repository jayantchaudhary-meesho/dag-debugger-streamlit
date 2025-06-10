import grpc
import requests
import json
import debug.debug_pb2 as debug_pb2
import debug.debug_pb2_grpc as debug_pb2_grpc
from google.protobuf.json_format import MessageToDict
import re

class MockResponse:
    def __init__(self, success, results=None, error=None):
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

def convert_keys_snake(obj):
    """Recursively convert dict keys to snake_case and apply alias mapping."""
    if isinstance(obj, dict):
        new_dict = {}
        for k, v in obj.items():
            new_key = camel_to_snake(k)
            # Apply alias mapping if applicable
            new_key = ALIAS_MAPPING.get(new_key, new_key)
            new_dict[new_key] = convert_keys_snake(v)
        return new_dict
    elif isinstance(obj, list):
        return [convert_keys_snake(item) for item in obj]
    else:
        return obj

def convert_floats_to_ints(obj):
    if isinstance(obj, dict):
        return {k: convert_floats_to_ints(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_ints(item) for item in obj]
    elif isinstance(obj, float) and obj.is_integer():
        return int(obj)
    else:
        return obj

def call_execute_dag_http(request_kwargs, config_source_type, user_id, user_context, iop_host):
    """Handle HTTP requests for catalog_listing_page and recently_viewed_catalog_recommendation"""
    url = f"http://{iop_host}/debug/dag/execute"

    headers = {
        "Content-Type": "application/json",
        "MEESHO-USER-ID": user_id,
        "MEESHO-USER-CONTEXT": user_context,
        "MEESHO-ISO-COUNTRY-CODE": "IN",
    }

    http_payload: dict = {}

    # Config kind
    if "ConfigKind" in request_kwargs:
        http_payload["config_kind"] = request_kwargs["ConfigKind"]

    # Data
    if "Data" in request_kwargs:
        data_proto = request_kwargs["Data"]
        data_dict = MessageToDict(data_proto, preserving_proto_field_name=True)
        # Recursively convert all floats that are integers to int
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
        # Optional debug print
        print("HTTP Request payload:", json.dumps(http_payload, indent=2))
        response = requests.post(url, headers=headers, json=http_payload, timeout=30)
        response.raise_for_status()

        resp_json = response.json()
        print("HTTP Response:", json.dumps(resp_json, indent=2))

        # Check the "success" flag returned by the service
        if isinstance(resp_json, dict) and resp_json.get("success") is False:
            error_msg = resp_json.get("error", "Unknown error from service")
            return MockResponse(False, error=error_msg)

        # Map successful results
        results_map = {}
        if isinstance(resp_json, dict) and "results" in resp_json:
            for key, val in resp_json["results"].items():
                results_map[key] = json.dumps(val)
        else:
            # Fallback – put entire response under root
            results_map["root"] = json.dumps(resp_json)

        return MockResponse(True, results=results_map)

    except requests.RequestException as e:
        error_msg = f"Request failed: {str(e)}"
        if getattr(e, "response", None) is not None:
            try:
                error_details = e.response.json()
                error_msg += f"\nDetails: {json.dumps(error_details)}"
            except Exception:
                error_msg += f"\nResponse: {e.response.text}"
        return MockResponse(False, error=error_msg)


def call_execute_dag_grpc(request_kwargs, user_id, user_context, iop_host):
    """Handle gRPC requests for for_you and catalog_recommendation"""
    try:
        request_data = debug_pb2.ExecuteDAGRequest(**request_kwargs)
        with grpc.insecure_channel(iop_host) as channel:
            stub = debug_pb2_grpc.DAGDebugServiceStub(channel)
            metadata = [("meesho-user-id", user_id), ("meesho-user-context", user_context)]
            response = stub.ExecuteDAG(request_data, metadata=metadata)
        return response
    except grpc.RpcError as e:
        return MockResponse(False, error=f"gRPC Error: {str(e)}")


def call_execute_dag(request_kwargs, config_source_type, user_id, user_context, feed_type, iop_host):
    """Route to HTTP or gRPC based on feed_type. Accepts raw request_kwargs from the UI."""
    http_feed_types = ["catalog_listing_page", "recently_viewed_catalog_recommendation"]

    if feed_type in http_feed_types:
        return call_execute_dag_http(request_kwargs, config_source_type, user_id, user_context, iop_host)
    else:
        return call_execute_dag_grpc(request_kwargs, user_id, user_context, iop_host) 