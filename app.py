import streamlit as st
import json
from services.dag_debug import call_execute_dag
from services.hero import get_heroPids_batch
from services.product import fetch_product_details
from services.pricing import get_pricing_features
from typing import List, Optional

PRICING_FEATURES = [
    "serving_price",
]

# Host mapping configuration
HOST_MAPPING = {
    "prod": {
        "for_you": "fy-iop-grpc-web.prd.meesho.int:80",
        "catalog_recommendation": "pdp-iop-grpc-service-web.prd.meesho.int:80",
        "catalog_listing_page": "clp-col-iop-web.prd.meesho.int",
        "recently_viewed_catalog_recommendation": "rv-iop-web.prd.meesho.int"
    },
    "pre-prod": {
        "for_you": "fy-iop-grpc-web.int.meesho.int:80",
        "catalog_recommendation": "pdp-iop-grpc-service-web.int.meesho.int:80",
        "catalog_listing_page": "clp-col-iop-web.int.meesho.int",
        "recently_viewed_catalog_recommendation": "rv-iop-web.prd.meesho.int"  # Using prod for RV as pre-prod not specified
    }
}

FEED_TYPES: List[str] = [
    "for_you",
    "catalog_recommendation",
    "catalog_listing_page",
    "recently_viewed_catalog_recommendation",
]

FEED_CONTEXTS = [
    "default",
    "gold",
    "mall",
    "cta_recommendation",
    "new",
    "widget",
    "gold_widget",
    "mall_widget",
    "high_asp_widget",
    "brand_widget",
    "top_of_pdp_widget",
    "fy_pop_up_widget",
    "search_pop_up_widget",
    "clp_applied_filters_enabled_gst_head_torso",
    "clp_applied_filters_enabled_gst_tail",
    "clp_applied_filters_enabled_mixed_head_torso",
    "clp_applied_filters_enabled_mixed_tail",
    "clp_applied_filters_disabled_gst_head_torso",
    "clp_applied_filters_disabled_gst_tail",
    "clp_applied_filters_disabled_mixed_head_torso",
    "clp_applied_filters_disabled_mixed_tail",
    "ss_cat",
    "wishlist",
]

@st.cache_data(show_spinner=False)
def _cached_get_hero_pid_map(catalog_ids: List[int]):
    """Batch-fetch hero PIDs for a list of catalog IDs (cached)."""
    return get_heroPids_batch(catalog_ids)

@st.cache_data(show_spinner=False)
def _cached_fetch_product_details(hero_pids: List[str]):
    """Fetch product details for the hero PIDs (cached)."""
    return fetch_product_details(hero_pids)

@st.cache_data(show_spinner=False)
def _cached_pricing_features(
    *,
    user_id: str,
    pdp_data: List[tuple],
    client_id: str = "ios",
    user_pincode: str = "122001",
    app_version_code: str = "685",
):
    """Retrieve pricing features for a batch of products (cached)."""
    return get_pricing_features(
        user_id=user_id,
        pdp_data=pdp_data,
        client_id=client_id,
        user_pincode=user_pincode,
        app_version_code=app_version_code,
        pricing_features=PRICING_FEATURES,
    )

st.title("Execute DAG Debugger")

user_id = st.text_input("User ID", value="123456")
environment = st.selectbox("Environment", ["pre-prod", "prod"], index=0)
feed_type = st.selectbox("Feed Type", FEED_TYPES, index=0)

# Automatically set IOP host based on feed type and environment
iop_host = HOST_MAPPING[environment][feed_type]
st.info(f"Using IOP Host: {iop_host}")

# Show relevant ID input based on feed type
collection_id_str = None
catalog_id_str = None
clp_id_str = None
ss_cat_id_str = None

if feed_type == "catalog_listing_page":
    clp_id_str = st.text_input("CLP ID")
elif feed_type == "catalog_recommendation":
    catalog_id_str = st.text_input("Catalog ID")
elif feed_type == "recently_viewed_catalog_recommendation":
    ss_cat_id_str = st.text_input("SS Cat ID")
# for_you feed type doesn't need any ID input

feed_context = st.selectbox("Feed Context", FEED_CONTEXTS, index=0)

tenant_context = st.selectbox("Tenant Context", ["organic", "ad", "ct"], index=0)

user_context = st.selectbox("User Context", ["logged_in", "anonymous"], index=0)

entity_type = st.text_input("Entity Type", value="catalog")

limit_str = st.text_input("Limit (for catalog_validator)")

cursor = st.text_input("Cursor")
catalog_scheduling_statuses = st.text_area("Catalog Scheduling Statuses (comma-separated)")

config_source_type = st.radio("Config Source Type", ("RawConfigJson", "Selector"))
raw_config_json = ""
selector_feed_type = ""
selector_tenant_ctx = ""
selector_user_ctx = ""
selector_feed_ctx = ""
selector_service_tag = ""
selector_variant_kind = ""
selector_variant_name = ""

selector = None

if config_source_type == "RawConfigJson":
    raw_config_json = st.text_area("Raw Config JSON", value="")
else:
    selector_feed_type = st.selectbox("Selector Feed Type", FEED_TYPES, index=0)
    selector_tenant_ctx = st.selectbox("Selector TenantCtx", ["organic", "ad", "ct"], index=0)
    selector_user_ctx = st.selectbox("Selector UserCtx", ["logged_in", "anonymous"], index=0)
    selector_feed_ctx = st.selectbox("Selector FeedCtx", FEED_CONTEXTS, index=0)
    # selector_service_tag = st.text_input("Selector ServiceTag")
    selector_variant_kind = st.text_input("Selector VariantKind")
    selector_variant_name = st.text_input("Selector VariantName")

config_kind = st.text_input("Config Kind", value="FeedWrite")
st.caption("💡 Examples: GenerateFeedOnTheFly, FeedWrite, FeedRead")
feed_metadata_json = st.text_area("Feed MetaData (JSON)")

def parse_int(val):
    try:
        return int(val)
    except (ValueError, TypeError):
        return None

collection_id = parse_int(collection_id_str)
catalog_id = parse_int(catalog_id_str)
clp_id = parse_int(clp_id_str)
ss_cat_id = parse_int(ss_cat_id_str)
limit = parse_int(limit_str)

if st.button("Execute DAG"):
    import debug.debug_pb2 as debug_pb2
    feed_id_kwargs = {}
    if collection_id is not None:
        feed_id_kwargs["CollectionId"] = collection_id
    if catalog_id is not None:
        feed_id_kwargs["CatalogId"] = catalog_id
    if clp_id is not None:
        feed_id_kwargs["ClpId"] = clp_id
    if ss_cat_id is not None:
        feed_id_kwargs["SSCatId"] = ss_cat_id
    feed_id = debug_pb2.FeedId(**feed_id_kwargs) if feed_id_kwargs else None

    data_kwargs = {}
    if user_id:
        data_kwargs["UserId"] = user_id
    if feed_type:
        data_kwargs["FeedType"] = feed_type
    if feed_id:
        data_kwargs["FeedId"] = feed_id
    if feed_context:
        data_kwargs["FeedContext"] = feed_context
    if tenant_context:
        data_kwargs["TenantContext"] = tenant_context
    if user_context:
        data_kwargs["UserContext"] = user_context
    if entity_type:
        data_kwargs["EntityType"] = entity_type
    if limit is not None:
        data_kwargs["Limit"] = limit
    if cursor:
        data_kwargs["Cursor"] = cursor
    if catalog_scheduling_statuses:
        data_kwargs["CatalogSchedulingStatuses"] = catalog_scheduling_statuses.split(',')
    if feed_metadata_json:
        try:
            feed_metadata = debug_pb2.google_dot_protobuf_dot_struct__pb2.Struct()
            feed_metadata.update(json.loads(feed_metadata_json))
            data_kwargs["FeedMetaData"] = feed_metadata
        except Exception as e:
            st.error(f"Invalid FeedMetaData JSON: {e}")
    data = debug_pb2.DebugExecutionRequestData(**data_kwargs) if data_kwargs else None

    selector = None
    if config_source_type == "Selector":
        selector_kwargs = {}
        if selector_feed_type:
            selector_kwargs["FeedType"] = selector_feed_type
        if selector_tenant_ctx:
            selector_kwargs["TenantCtx"] = selector_tenant_ctx
        if selector_user_ctx:
            selector_kwargs["UserCtx"] = selector_user_ctx
        if selector_feed_ctx:
            selector_kwargs["FeedCtx"] = selector_feed_ctx
        # if selector_service_tag:
        #     selector_kwargs["ServiceTag"] = selector_service_tag
        if selector_variant_kind:
            selector_kwargs["VariantKind"] = selector_variant_kind
        if selector_variant_name:
            selector_kwargs["VariantName"] = selector_variant_name
        if selector_kwargs:
            selector = debug_pb2.ConfigSelector(**selector_kwargs)

    request_kwargs = {}
    if config_source_type == "RawConfigJson" and raw_config_json:
        request_kwargs["RawConfigJson"] = raw_config_json
    if config_source_type == "Selector" and selector:
        request_kwargs["Selector"] = selector
    if config_kind:
        request_kwargs["ConfigKind"] = config_kind
    if data:
        request_kwargs["Data"] = data

    # Pass raw request_kwargs and config_source_type to the executor
    with st.spinner("Executing DAG..."):
        response = call_execute_dag(
            request_kwargs,
            config_source_type,
            user_id,
            user_context,
            feed_type,
            iop_host,
        )

    # Check for errors more comprehensively
    has_error = False
    error_message = ""
    
    if hasattr(response, 'Success') and not response.Success:
        has_error = True
        error_message = getattr(response, 'Error', 'Unknown error occurred')
    elif hasattr(response, 'Error') and response.Error:
        has_error = True
        error_message = response.Error
    elif not hasattr(response, 'Results') or not response.Results:
        has_error = True
        error_message = "No results returned from DAG execution"
    
    if has_error:
        st.error(f"Error executing DAG: {error_message}")
        # Optionally show the full response for debugging
        with st.expander("Full Response (for debugging)"):
            st.write(response)
    else:
        st.success("DAG executed successfully!")
        # st.write(response)
        # Render debug_config DAG if present
        debug_cfg_raw = response.Results.get("debug_config") if isinstance(response.Results, dict) else None
        if debug_cfg_raw:
            try:
                debug_cfg_json = (
                    debug_cfg_raw
                    if isinstance(debug_cfg_raw, dict)
                    else json.loads(debug_cfg_raw)
                )
                dag_cfg = (
                    debug_cfg_json.get("dag_config")
                    or debug_cfg_json.get("config", {}).get("dag_config")
                )
                if dag_cfg:
                    dot_lines = ["digraph DAG {"]
                    for src, dst_list in dag_cfg.items():
                        for dst in dst_list:
                            dot_lines.append(f'  "{src}" -> "{dst}";')
                    dot_lines.append("}")
                    st.subheader("DAG Graph (debug_config)")
                    st.graphviz_chart("\n".join(dot_lines))
            except Exception as e:
                st.error(f"Failed to render debug_config DAG: {e}")

        for key, value in response.Results.items():
            # Skip debug_config here since already handled above
            if key == "debug_config":
                continue
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list) and all(isinstance(item, dict) for item in parsed):
                    # Show expander with product count
                    with st.expander(f"Result for: {key} ({len(parsed)} results)"):
                        with st.spinner("Fetching hero PIDs in batch..."):
                            catalog_ids = [item.get("id") for item in parsed if item.get("id") is not None]
                            hero_pid_map = _cached_get_hero_pid_map(catalog_ids)
                            for item in parsed:
                                cid = item.get("id")
                                item["hero_pid"] = hero_pid_map.get(cid, "N/A")
                        hero_pids = [pid for pid in hero_pid_map.values() if pid != "N/A"]
                        with st.spinner("Fetching product details for hero PIDs..."):
                            product_details = _cached_fetch_product_details(hero_pids)
                        if product_details:
                            st.subheader("Hero Product Details")
                            num_cols = 3
                            pdp_data = [(prod["product_id"], "source", "") for prod in product_details if prod.get("product_id")]
                            pricing_data = _cached_pricing_features(
                                user_id=user_id,
                                pdp_data=pdp_data,
                            )
                            # Debug: show raw pricing gRPC response (no nested expander)
                            _, raw_pricing_resp = get_pricing_features(
                                user_id=user_id,
                                pdp_data=pdp_data,
                                client_id="ios",
                                user_pincode="122001",
                                app_version_code="685",
                                pricing_features=PRICING_FEATURES,
                                return_raw=True,
                            )
                            # st.write(str(raw_pricing_resp))
                            for prod in product_details:
                                pid = str(prod.get("product_id"))
                                prod["pricing"] = pricing_data.get(pid, {})
                            for i in range(0, len(product_details), num_cols):
                                cols = st.columns(num_cols)
                                for j, product in enumerate(product_details[i:i+num_cols]):
                                    with cols[j]:
                                        st.markdown(f"**{product.get('catalog_name', 'N/A')}**")
                                        st.markdown(f"SSCat: {product.get('sscat_name', 'N/A')}")
                                        images = product.get('product_images', [])
                                        if images:
                                            st.image(images[0], width=150)
                                        # Create tabs for product view
                                        tab1, tab2 = st.tabs(["📋 Summary", "🔧 JSON Details"])
                                        
                                        with tab1:
                                            st.markdown(f"Product ID: `{product.get('product_id', 'N/A')}`")
                                            st.markdown(f"Catalog ID: `{product.get('catalog_id', 'N/A')}`")
                                            serving_price = product.get('pricing', {}).get('serving_price')
                                            if serving_price:
                                                st.markdown(f"**Serving Price:** ₹{serving_price}")
                                        
                                        with tab2:
                                            # Show original candidate details instead of enriched product
                                            candidate = next(
                                                (c for c in parsed if c.get("id") == product.get("catalog_id")),
                                                product,
                                            )
                                            st.json(candidate)
                                        st.markdown("---")
                        else:
                            st.info("No product details found for hero_pids.")
                else:
                    # Handle non-list or non-dict items
                    with st.expander(f"Result for: {key}"):
                        st.info("null")
            except Exception as e:
                with st.expander(f"Result for: {key}"):
                    st.error(f"Could not parse result for {key}: {e}")
                    st.text(value)
