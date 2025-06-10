import grpc
from pricing import pricing_service_pb2
from pricing import pricing_service_pb2_grpc

def get_pricing_features(user_id, pdp_data, client_id, user_pincode, app_version_code, pricing_features):
    channel = grpc.insecure_channel('price-aggregator-go.prd.meesho.int:80')
    metadata = [
        ('meesho-user-id', str(user_id)),
        ('meesho-user-context', 'logged_in'),
        ('meesho-user-city', 'Bangalore'),
        ('meesho_user_context', 'anonymous'),
        ('meesho-client-id', str(client_id)),
        ('meesho-iso-country-code', 'IN'),
        ('meesho-user-pincode', str(user_pincode)),
        ('app-version-code', str(app_version_code))
    ]
    try:
        ids = []
        for hero_pid, _, _ in pdp_data:
            user_key = pricing_service_pb2.EntityQueries.EntityId.EntityKey(
                type="user_id",
                value=str(user_id)
            )
            product_key = pricing_service_pb2.EntityQueries.EntityId.EntityKey(
                type="product_id",
                value=str(hero_pid)
            )
            entity_id = pricing_service_pb2.EntityQueries.EntityId(
                keys=[user_key, product_key]
            )
            ids.append(entity_id)
        feature_group = pricing_service_pb2.EntityQueries.FeatureGroups(
            label="real_time_product_pricing",
            features=pricing_features
        )
        request = pricing_service_pb2.EntityQueries(
            label="user_product",
            ids=ids,
            featureGroups=[feature_group]
        )
        stub = pricing_service_pb2_grpc.PricingFeatureRetrievalServiceStub(channel)
        response = stub.retrieveFeatures(
            request=request,
            metadata=metadata
        )
        result = {}
        if response.data:
            for i, _ in enumerate(response.data):
                if i < len(ids):
                    product_id = None
                    for key in ids[i].keys:
                        if key.type == "product_id":
                            product_id = key.value
                            break
                    if product_id:
                        features = {}
                        j = 2
                        if len(response.data[i+1].features) >= 3:
                            for feature in pricing_features:
                                features[feature] = str(response.data[i+1].features[j])
                                j += 1
                        result[response.data[i+1].features[1]] = features
        return result
    except grpc.RpcError as e:
        print(f"gRPC call failed: {e}")
        return {}
    finally:
        channel.close()
