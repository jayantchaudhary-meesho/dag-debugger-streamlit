import grpc
from typing import List, Dict, Any, Optional, Tuple
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
    pricing_features: List[str]
) -> Dict[str, Dict[str, str]]:
    """
    Fetch pricing features for a list of products.
    
    Args:
        user_id: User ID
        pdp_data: List of tuples containing (product_id, source, extra_info)
        client_id: Client ID (e.g., 'ios')
        user_pincode: User's pincode
        app_version_code: App version code
        pricing_features: List of pricing features to fetch
        
    Returns:
        Dictionary mapping product IDs to their pricing features
    """
    channel = grpc.insecure_channel(PRICING_SERVICE_HOST)
    metadata = _build_metadata(user_id, client_id, user_pincode, app_version_code)
    
    try:
        entity_ids = _build_entity_ids(user_id, pdp_data)
        feature_group = _build_feature_group(pricing_features)
        request = _build_request(entity_ids, feature_group)
        
        stub = pricing_service_pb2_grpc.PricingFeatureRetrievalServiceStub(channel)
        response = stub.retrieveFeatures(request=request, metadata=metadata)
        
        return _process_response(response, entity_ids, pricing_features)
    except grpc.RpcError as e:
        print(f"gRPC call failed: {e}")
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
    """Process the gRPC response into a dictionary of pricing features."""
    result = {}
    if not response.data:
        return result

    for i, _ in enumerate(response.data):
        if i >= len(entity_ids):
            continue

        product_id = None
        for key in entity_ids[i].keys:
            if key.type == "product_id":
                product_id = key.value
                break

        if not product_id:
            continue

        features = {}
        if len(response.data[i+1].features) >= 3:
            for j, feature in enumerate(pricing_features, start=2):
                if j < len(response.data[i+1].features):
                    features[feature] = str(response.data[i+1].features[j])

        result[response.data[i+1].features[1]] = features

    return result
