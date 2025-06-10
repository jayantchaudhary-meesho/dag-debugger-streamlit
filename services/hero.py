import requests

def get_heroPids_batch(catalog_ids):
    url = "http://taxonomy-hero.prd.meesho.int/api/v1/products/hero-product"
    headers = {
        "Authorization": "Token bYTFfK5Czo42zfhMmPQoUvXmWiSJ9fV8EbTKdQfDFL4A40tJ",
        "MEESHO-ISO-COUNTRY-CODE": "IN",
        "Content-Type": "application/json"
    }
    payload = {"catalog_ids": catalog_ids}
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()
        hero_pid_map = {}
        for entry in data.get("data", []):
            cid = entry.get("catalog_id")
            hero_pid = entry.get("hero_product")
            error = entry.get("errors")
            hero_pid_map[cid] = hero_pid if not error else "N/A"
        return hero_pid_map
    except Exception:
        return {cid: "N/A" for cid in catalog_ids} 