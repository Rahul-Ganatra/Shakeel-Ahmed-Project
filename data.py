import requests
import urllib.parse
import json
import time

BASE_URL = "https://api.bsdd.buildingsmart.org/api"
IFC_BASE_URI = "https://identifier.buildingsmart.org/uri/buildingsmart/ifc/4.3/class/"

def fetch_ifc_data(start_class="IfcRoot", verbose=True):
    visited = set()
    output = []

    def debug(msg):
        if verbose:
            print(f"[DEBUG] {msg}")

    def fetch_class(class_code):
        if class_code in visited:
            debug(f"Skipping already visited class: {class_code}")
            return

        visited.add(class_code)

        uri = f"{IFC_BASE_URI}{class_code}"
        encoded_uri = urllib.parse.quote(uri, safe="")

        debug(f"Fetching class info for: {class_code}")

        try:
            # Fetch class metadata
            response = requests.get(f"{BASE_URL}/Class/v1?uri={encoded_uri}&includeChildClassReferences=true")
            response.raise_for_status()
            class_data = response.json()
            debug(f"Fetched metadata for {class_code}")

            # Fetch properties
            prop_response = requests.get(f"{BASE_URL}/Class/Properties/v1?classuri={encoded_uri}&offset=0&limit=1000")
            prop_response.raise_for_status()
            properties_data = prop_response.json().get("classProperties", [])
            debug(f"Fetched {len(properties_data)} properties for {class_code}")

            # Fetch relations
            relations_response = requests.get(f"{BASE_URL}/Class/Relations/v1?classuri={encoded_uri}&offset=0&limit=1000&getReverseRelations=true")
            relations_response.raise_for_status()
            relations_data = relations_response.json()
            debug(f"Fetched relations for {class_code}")

            # Group properties by category
            properties_by_category = {}
            for p in properties_data:
                category = p.get("propertySet", "Attributes")
                if not category:
                    category = "Attributes"
                if category not in properties_by_category:
                    properties_by_category[category] = []
                properties_by_category[category].append({
                    "name": p.get("name", ""),
                    "data_type": p.get("dataType", ""),
                    "definition": p.get("definition", "")
                })

            if not properties_by_category:
                properties_by_category["Attributes"] = [{
                    "name": "Attributes",
                    "data_type": "not in bSDD",
                    "definition": ""
                }]

            # Process relations
            class_relations = []
            for relation in relations_data.get("classRelations", []):
                class_relations.append({
                    "relation_type": relation.get("relationType", ""),
                    "class_uri": relation.get("classUri", ""),
                    "class_name": relation.get("className", ""),
                    "dictionary_uri": relation.get("dictionaryUri", "")
                })

            obj = {
                "class_name": class_data.get("name", ""),
                "code": class_data.get("code", ""),
                "child_classes": [
                    urllib.parse.urlparse(child["uri"]).path
                    for child in class_data.get("childClassReferences", [])
                ],
                "relations": class_relations,
                "incoming_relations": [],
                "url": class_data.get("uri", ""),
                "properties": properties_by_category,
                "relations_info": {
                    "class_uri": relations_data.get("classUri", ""),
                    "are_reversed_relations": relations_data.get("areReversedRelations", False),
                    "total_count": relations_data.get("totalCount", 0),
                    "offset": relations_data.get("offset", 0),
                    "count": relations_data.get("count", 0)
                }
            }

            output.append(obj)
            debug(f"Appended class {class_code} with {len(properties_by_category)} property categories, {len(class_relations)} relations, and {len(obj['child_classes'])} children")

            # Recurse into children
            for child in class_data.get("childClassReferences", []):
                fetch_class(child["code"])

            time.sleep(0.2)  # To avoid overloading the API

        except requests.RequestException as e:
            debug(f"❌ Error fetching data for {class_code}: {e}")

    fetch_class(start_class)
    return output

# === Main Run ===
if __name__ == "__main__":
    class_name = "IfcRoot"
    data = fetch_ifc_data(class_name, verbose=True)

    filename = f"{class_name}_class_tree_grouped_all.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"\n✅ Done. Saved to: {filename}")