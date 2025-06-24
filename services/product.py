import requests
from math import ceil
from typing import List, Dict, Any, Optional

# Constants
TAXONOMY_API_URL = "http://taxonomy-new.prd.meesho.int/api/v2/product/aggregation"
BATCH_SIZE = 100
REQUEST_TIMEOUT = 1000

def fetch_product_details(product_id_list: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch product details from taxonomy service in batches.
    
    Args:
        product_id_list: List of product IDs to fetch details for
        
    Returns:
        List of product details with catalog and product information
        
    Raises:
        Exception: If the API request fails
    """
    headers = {"Content-Type": "application/json"}
    combined_result = []

    total_batches = ceil(len(product_id_list) / BATCH_SIZE)
    for i in range(total_batches):
        batch_start = i * BATCH_SIZE
        batch_end = min(batch_start + BATCH_SIZE, len(product_id_list))
        batch = product_id_list[batch_start:batch_end]

        payload = {
            "product_ids": batch,
            "request_flags": {
                "fetch_old_sscat_details": True,
                "fetch_serving_data": True,
                "fetch_taxonomy_attributes": True
            }
        }

        try:
            response = requests.post(
                TAXONOMY_API_URL,
                headers=headers,
                json=payload,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            raise Exception(f"Failed to fetch product details: {str(e)}")

        result = _process_catalog_data(data.get("catalogs", []))
        result = _enrich_with_product_data(result, data.get("products", []))

        if len(batch) == 1:
            combined_result.extend(result)
            continue

        # Reorder results to match input order
        result_pid_to_index = {item['product_id']: i for i, item in enumerate(result)}
        reordered_products = [
            result[result_pid_to_index[pid]]
            for pid in batch
            if pid in result_pid_to_index
        ]
        combined_result.extend(reordered_products)

    return combined_result

def _process_catalog_data(catalogs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Process catalog data from taxonomy response."""
    return [{
        "catalog_id": catalog.get("id"),
        "catalog_name": catalog.get("name"),
        "old_sub_sub_category_id": catalog.get("old_category", {}).get("sub_sub_category_id"),
        "sscat_name": catalog.get("old_category", {}).get("sub_sub_category_name"),
        "product_images": [catalog.get("image")],
        "product_id": catalog.get('id')
    } for catalog in catalogs]

def _enrich_with_product_data(
    catalog_results: List[Dict[str, Any]],
    products: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Enrich catalog results with product data."""
    for product in products:
        cid = product.get('catalog_id')
        idx = next((i for i, item in enumerate(catalog_results) if item["catalog_id"] == cid), None)
        if idx is not None:
            catalog_results[idx]["product_id"] = product.get("id")
            catalog_results[idx]["product_images"] = product.get("images")
    return catalog_results 