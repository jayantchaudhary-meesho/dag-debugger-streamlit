import requests
from math import ceil

def fetch_product_details(product_id_list):
    url = "http://taxonomy-new.prd.meesho.int/api/v2/product/aggregation"
    headers = {"Content-Type": "application/json"}
    batch_size = 100
    combined_result = []

    total_batches = ceil(len(product_id_list) / batch_size)
    for i in range(total_batches):
        batch_start = i * batch_size
        batch_end = min(batch_start + batch_size, len(product_id_list))
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
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            raise Exception(f"Failed to fetch product details. Error: {e}")

        result = []
        for each_product_from_taxonomy in data.get("catalogs", []):
            extracted_data = {
                "catalog_id": each_product_from_taxonomy.get("id"),
                "catalog_name": each_product_from_taxonomy.get("name"),
                "old_sub_sub_category_id": each_product_from_taxonomy.get("old_category", {}).get("sub_sub_category_id"),
                "sscat_name": each_product_from_taxonomy.get("old_category", {}).get("sub_sub_category_name"),
                "product_images": [each_product_from_taxonomy.get("image")],
                "product_id": each_product_from_taxonomy.get('id')
            }
            result.append(extracted_data)

        for each_product_from_taxonomy in data.get("products", []):
            cid = each_product_from_taxonomy.get('catalog_id')
            idx = next((i for i, item in enumerate(result) if item["catalog_id"] == cid), None)
            if idx is not None:
                result[idx]["product_id"] = each_product_from_taxonomy.get("id")
                result[idx]["product_images"] = each_product_from_taxonomy.get("images")

        if len(batch) == 1:
            combined_result.extend(result)
            continue

        result_pid_to_index = {result[i]['product_id']: i for i in range(len(result))}
        reordered_products = []
        for product_id in batch:
            if product_id in result_pid_to_index:
                reordered_products.append(result[result_pid_to_index[product_id]])

        combined_result.extend(reordered_products)

    return combined_result 