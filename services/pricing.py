import grpc
from typing import List, Dict, Any, Optional, Tuple, Union
from pricing import pricing_service_pb2
from pricing import pricing_service_pb2_grpc

# Constants
PRICING_SERVICE_HOST = 'price-aggregator-go.prd.meesho.int:80'
DEFAULT_METADATA = {
    'meesho-user-context': 'logged_in',
    'meesho-user-city': 'Bangalore',
    'meesho_user_context': 'anonymous',
    'meesho-iso-country-code': 'IN',
}

def get_pricing_features(
    user_id: str,
    pdp_data: List[Tuple[str, str, str]],
    client_id: str,
    user_pincode: str,
    app_version_code: str,
    pricing_features: List[str],
    *,
    return_raw: bool = False,
) -> Union[Dict[str, Dict[str, str]], Tuple[Dict[str, Dict[str, str]], Any]]:
    """
    Fetch pricing features for a list of products.
    
    Args:
        user_id: User ID
        pdp_data: List of tuples containing (product_id, source, extra_info)
        client_id: Client ID (e.g., 'ios')
        user_pincode: User's pincode
        app_version_code: App version code
        pricing_features: List of pricing features to fetch
        return_raw: Flag to return raw gRPC response for debugging
        
    Returns:
        Dictionary mapping product IDs to their pricing features
    """
    channel = grpc.insecure_channel(PRICING_SERVICE_HOST)
    metadata = _build_metadata(user_id, client_id, user_pincode, app_version_code)
    
    response = None
    try:
        entity_ids = _build_entity_ids(user_id, pdp_data)
        feature_group = _build_feature_group(pricing_features)
        request = _build_request(entity_ids, feature_group)
        
        stub = pricing_service_pb2_grpc.PricingFeatureRetrievalServiceStub(channel)
        response = stub.retrieveFeatures(request=request, metadata=metadata)
        
        parsed = _process_response(response, entity_ids, pricing_features)
        if return_raw:
            return parsed, response
        return parsed
    except grpc.RpcError as e:
        print(f"gRPC call failed: {e}")
        if return_raw:
            return {}, response
        return {}
    finally:
        channel.close()

def _build_metadata(
    user_id: str,
    client_id: str,
    user_pincode: str,
    app_version_code: str
) -> List[Tuple[str, str]]:
    """Build gRPC metadata for the request."""
    return [
        ('meesho-user-id', str(user_id)),
        ('meesho-client-id', str(client_id)),
        ('meesho-user-pincode', str(user_pincode)),
        ('app-version-code', str(app_version_code)),
        *[(k, v) for k, v in DEFAULT_METADATA.items()]
    ]

def _build_entity_ids(
    user_id: str,
    pdp_data: List[Tuple[str, str, str]]
) -> List[pricing_service_pb2.EntityQueries.EntityId]:
    """Build entity IDs for the request."""
    entity_ids = []
    for hero_pid, _, _ in pdp_data:
        user_key = pricing_service_pb2.EntityQueries.EntityId.EntityKey(
            type="user_id",
            value=str(user_id)
        )
        product_key = pricing_service_pb2.EntityQueries.EntityId.EntityKey(
            type="product_id",
            value=str(hero_pid)
        )
        entity_ids.append(pricing_service_pb2.EntityQueries.EntityId(
            keys=[user_key, product_key]
        ))
    return entity_ids

def _build_feature_group(
    pricing_features: List[str]
) -> pricing_service_pb2.EntityQueries.FeatureGroups:
    """Build feature group for the request."""
    return pricing_service_pb2.EntityQueries.FeatureGroups(
        label="real_time_product_pricing",
        features=pricing_features
    )

def _build_request(
    entity_ids: List[pricing_service_pb2.EntityQueries.EntityId],
    feature_group: pricing_service_pb2.EntityQueries.FeatureGroups
) -> pricing_service_pb2.EntityQueries:
    """Build the gRPC request."""
    return pricing_service_pb2.EntityQueries(
        label="user_product",
        ids=entity_ids,
        featureGroups=[feature_group]
    )

def _process_response(
    response: Any,
    entity_ids: List[pricing_service_pb2.EntityQueries.EntityId],
    pricing_features: List[str]
) -> Dict[str, Dict[str, str]]:
    """Process the gRPC response into a dictionary of pricing features.

    The Pricing service returns data in the following shape:
    response.data[0].features -> header names (user_id, product_id, real_time_product_pricing:serving_price, ...)
    subsequent data entries      -> corresponding values for each field
    """
    result: Dict[str, Dict[str, str]] = {}

    if not getattr(response, "data", None) or len(response.data) < 2:
        return result

    header = list(response.data[0].features)

    # Identify index positions we care about
    try:
        product_idx = header.index("product_id")
    except ValueError:
        return result  # malformed header

    feature_idx_map: Dict[str, int] = {}
    for feature_name in pricing_features:
        key_tag = f"real_time_product_pricing:{feature_name}"
        if key_tag in header:
            feature_idx_map[feature_name] = header.index(key_tag)

    # Iterate over rows starting from 1 (since 0 is header)
    for row in response.data[1:]:
        values = list(row.features)
        if len(values) != len(header):
            continue  # skip malformed row
        product_id = values[product_idx]
        if not product_id:
            continue
        features: Dict[str, str] = {}
        for fname, idx in feature_idx_map.items():
            if idx < len(values):
                features[fname] = values[idx]
        if features:
            result[str(product_id)] = features

    return result
