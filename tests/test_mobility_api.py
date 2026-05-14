import pytest
import requests
import json
from pymeos import *
import urllib.parse
import time
import pandas as pd
import ijson
import os
HOST = "http://localhost:8080"

pymeos_initialize()

script_dir = os.path.dirname(os.path.abspath(__file__))
data= os.path.join(script_dir, "../data", "trajectories_mf1.json")
def log_request_response(action: str, response: requests.Response):
    req = response.request
    print(f"\n===| {action.upper()} |===")
    print(f"==> {req.method} {req.url}")
    #if request body
    if req.body:
        try:
            body = json.loads(req.body)
            print("Request JSON:", json.dumps(body, indent=2)[:600])
        except Exception:
            print("Request body:", req.body[:500])
    #response status code
    print(f"<== Status: {response.status_code}")
    try:
        print("Response JSON:", json.dumps(response.json(), indent=2)[:700])
    except Exception:
        print("Response Text:", response.text[:600])
    print("=" * 60 + "\n")



    # Return the feature object at the given index (0‑based) from the JSON array.
def get_feature_by_index(file_path, index):

    with open(file_path, 'rb') as f:
        objects = ijson.items(f, 'item')
        for i, obj in enumerate(objects):
            if i == index:
                return obj
    return None

# yield feature objects starting from `start_index` (0‑based) without loading all.
def iter_features_from_index(file_path, start_index=0):
    
    with open(file_path, 'rb') as f:
        objects = ijson.items(f, 'item')
        for i, obj in enumerate(objects):
            if i >= start_index:
                yield obj
# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------

@pytest.fixture(scope="session")
def create_collections():
   
    collections = [
        {
            "title": "ships",
            "updateFrequency": 1000,
            "description": "Collection of ship trajectories (Trips)",
            "itemType": "movingfeature",
        },
        {
            "title": "boats",
            "updateFrequency": 1000,
            "description": "Collection of boat movements (Trips)",
            "itemType": "movingfeature",
        },
    ]

    created = []
    for col in collections:
        resp = requests.post(f"{HOST}/collections", json=col)
        log_request_response(f"Create collection {col['title']}", resp)
        
        assert resp.status_code in (201, 409)
        
        if resp.status_code == 201:

            # check the resp.json without formatting- important 
            created.append({
                "id": col["title"].lower().replace(" ", "_"),
                "title": col["title"],
                "updateFrequency": col["updateFrequency"],
                "itemType": col["itemType"]
            })
        else:
            created.append({
                "id": col["title"].lower(),
                "updateFrequency": col["updateFrequency"],
                "itemType": col["itemType"]
            })

    yield created

    # print("\n=============DELETING COLLECTIONS- clean up=============")
    # for col in created:
    #     col_id = col["id"]
    #     resp = requests.delete(f"{HOST}/collections/{col_id}")
    #     print(f"Deleted {col_id}: {resp.status_code}")


# ============= FIXTURE FOR QUERY TESTS WITH MULTI-POINT TRAJECTORY =============
# Create collection and feature for query tests (single feature )
@pytest.fixture(scope="module")
def setup_query_test_data():
    
    # Create collection
    collection_data = {
        "title": "query_test",
        "description": "Collection for query testing",
        "updateFrequency": 1000,
        "itemType": "movingfeature"
    }
    #===========================================Create collection query_test=============================================
    resp = requests.post(f"{HOST}/collections", json=collection_data)
    log_request_response("Create query_test collection", resp)
    assert resp.status_code in (201, 409)
    
    collection_id = "query_test"
    
    # Create TemporalGeometry 
    TemporalGeometry = {
        "type": "MovingPoint",
        "datetimes": [
            "2024-03-01 00:00:00+00",
            "2024-03-01 00:15:00+00", 
            "2024-03-01 00:30:00+00",
            "2024-03-01 00:45:00+00",
            "2024-03-01 01:00:00+00"
        ],
        "coordinates": [
            [12.675237, 54.524345],
            [12.685237, 54.534345],
            [12.695237, 54.544345],
            [12.705237, 54.554345],
            [12.715237, 54.564345]
        ],
        "interpolation": "Linear"
    }
    
    feature = {
        "type": "Feature",
        "id": "query_test_001", #???? check
        "crs": {  # ← ADD THIS
            "type": "name",
            "properties": "urn:ogc:def:crs:EPSG::4326"  
    },
        "temporalGeometry": TemporalGeometry ,
        "properties": {
            "name": "Query Test Feature",
            "type": "test" #check
        }
    }
    #===========================================Create feature in query_test collection=============================================
    
    resp = requests.post(
        f"{HOST}/collections/{collection_id}/items",
        json=feature, #check ogc
        headers={"Content-Type": "application/json"}
    )
    # get the tempo geom id
    log_request_response("Create query test feature", resp)
    assert resp.status_code in (201, 409)
    
    resp = requests.get(f"{HOST}/collections/{collection_id}/items/query_test_001/tgsequence")
    log_request_response("Get Temporal geometry id", resp)
    assert resp.status_code == 200
    geom_data = resp.json()
    geometry_id = geom_data["geometrySequence"][0]["id"] if geom_data["geometrySequence"] else 1
    
    yield {
        "collection_id": collection_id,
        "feature_id": "query_test_001",
        "geometry_id": geometry_id
    }
    
    # Clean
    # print("\n=== CLEANING UP QUERY TEST DATA ===")
    # requests.delete(f"{HOST}/collections/{collection_id}/items/query_test_001")#this should be deleted on cascad-->assert deleted
    # requests.delete(f"{HOST}/collections/{collection_id}")


# ============= COLLECTION TESTS =============
#==========================================================GET /collections============================================
def test_get_all_collections(create_collections):
    
    resp = requests.get(f"{HOST}/collections")
    log_request_response("Get all collections", resp)
    assert resp.status_code == 200
    data = resp.json()
    
    assert "collections" in data
    assert "links" in data
    
    collection_ids = [c["id"] for c in data.get("collections", [])]
    for col in create_collections:
        assert col["id"] in collection_ids

#========================================================GET /collections/{id}====================================================
def test_get_single_collection(create_collections):
    
    col_id = create_collections[0]["id"]
    resp = requests.get(f"{HOST}/collections/{col_id}")
    log_request_response(f"Get single collection {col_id}", resp)
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("id") == col_id #recheck text
    assert data.get("itemType") == "movingfeature"
    assert "links" in data


#=========================================================PUT /collections/{id}==========================================================
def test_replace_collection(create_collections):
   
    col_id = create_collections[0]["id"]

    update_data = {
        "title": "Vessels",
        "description": "Updated collection for vessels",
    }
    resp = requests.put(f"{HOST}/collections/{col_id}", json=update_data)
    log_request_response(f"Update collection {col_id}", resp)
    assert resp.status_code in (200, 204)

    resp = requests.get(f"{HOST}/collections/{col_id}")
    log_request_response(f"Verify update {col_id}", resp)
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("title") == update_data["title"]
    assert data.get("description") == update_data["description"]
    assert data.get("updateFrequency") == create_collections[0]["updateFrequency"] #shouldn't be changed ogc

#========================================================DELETE /collections/{id}===============================================================
def test_delete_collection():
    tmp_data = {
        "title": "Temp",
        "description": "Temp collection",
        "updateFrequency": 1,
        "itemType": "movingfeature",
    }

    resp = requests.post(f"{HOST}/collections", json=tmp_data)
    log_request_response("Create temp collection", resp)
    assert resp.status_code in (201, 409)
    

    col_id = "temp"
    
    resp = requests.delete(f"{HOST}/collections/{col_id}")
    log_request_response(f"Delete collection {col_id}", resp)
    assert resp.status_code in (200, 204)


#************************************************************** MOVING FEATURES CREATE***************************************************************
################################################################POST###########################################################
#====================================================POST /collections/{id}/items - single feature==============================================================
def test_create_single_feature(create_collections):
    
    collection_id = "ships"
    feature_data = get_feature_by_index(data, 0)  # was data[0]
    if feature_data is None:
        pytest.fail("Could not read first feature")

    feature = {
        "type": "Feature",
        "id": str(feature_data["mmsi"]),
        "crs": {
            "type": "name",
            "properties": "urn:ogc:def:crs:EPSG::25832"
        },
        "temporalGeometry": json.loads(feature_data["trajectory"]),
        "properties": {
            "name": f"Ship_{feature_data['mmsi']}",
            "type": "cargo"
        }
    }

    resp = requests.post(
        f"{HOST}/collections/{collection_id}/items",
        json=feature,#recheck req body per ogc
        headers={"Content-Type": "application/json"}
    )
    log_request_response("Create single feature", resp)
    assert resp.status_code in (201, 409)


#=====================================================POST /collections/{id}/items - feature collection================================

def test_create_feature_collection(create_collections):
    collection_id = "ships"
    batch_size = 20 # max96
    created_count = 0
    batch = []
    
    # Skip the first feature (index 0) – start from index 1
    for obj in iter_features_from_index(data , start_index=1):
        batch.append(obj)
        if len(batch) >= batch_size:
            features = []
            for o in batch:
                features.append({
                    "type": "Feature",
                    "id": str(o["mmsi"]),
                    "properties": o["properties"],
                    "crs": {
                        "type": "name",
                        "properties": {
                            "name": "urn:ogc:def:crs:EPSG::25832"
                        }
                    },
                    "trs": {
                        "type": "Link",
                        "properties": {
                            "type": "ogcdef",
                            "href": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"
                        }
                    },
                    "temporalGeometry": json.loads(o["trajectory"]),
                    "temporalProperties": [{
                        "datetimes": [
                            "2011-07-14T22:01:01.450Z",
                            "2011-07-14T23:01:01.450Z",
                            "2011-07-15T00:01:01.450Z"
                        ],
                        "length": {
                            "type": "Measure",
                            "form": "http://qudt.org/vocab/quantitykind/Length",
                            "values": [1, 2.4, 1],
                            "interpolation": "Linear",
                            "description": "description1"
                        },
                        "discharge": {
                            "type": "Measure",
                            "form": "MQS",
                            "values": [3, 4, 5],
                            "interpolation": "Step"
                        }
                    },
                    {
                        "datetimes": [
                            "2011-07-15T23:01:01.450Z",
                            "2011-07-16T00:01:01.450Z"
                        ],
                        "camera": {
                            "type": "Image",
                            "values": [
                            "http://.../example/image1",
                            "VBORw0KGgoAAAANSUhEU......"
                            ],
                            "interpolation": "Discrete"
                        },
                        "labels": {
                            "type": "Text",
                            "values": ["car", "human"],
                            "interpolation": "Discrete"
                        }
                    }]
                })
            
            feature_collection = {
                "type": "FeatureCollection",
                "features": features
            }
            
            resp = requests.post(
                f"{HOST}/collections/{collection_id}/items",
                json=feature_collection,
                headers={"Content-Type": "application/json"}
            )
            
            if resp.status_code in (201, 409):
                created_count += len(batch)
                print(f"Batch: Created {len(batch)} features")
            batch = []  # free memory
    
    # Last batch
    if batch:
        features = []
        for o in batch:
            features.append({
                "type": "Feature",
                "id": str(o["mmsi"]),
                "properties": o["properties"],
                "crs": {
                    "type": "name",
                    "properties": {
                        "name": "urn:ogc:def:crs:EPSG::25832"
                    }
                },
                "trs": {
                    "type": "Link",
                    "properties": {
                        "type": "ogcdef",
                        "href": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"
                    }
                },
                "temporalGeometry": o["trajectory"],
                "temporalProperties": [{
                    "datetimes": [
                        "2011-07-14T22:01:01.450Z",
                        "2011-07-14T23:01:01.450Z",
                        "2011-07-15T00:01:01.450Z"
                    ],
                    "length": {
                        "type": "Measure",
                        "form": "http://qudt.org/vocab/quantitykind/Length",
                        "values": [1, 2.4, 1],
                        "interpolation": "Linear",
                        "description": "description1"
                    },
                    "discharge": {
                        "type": "Measure",
                        "form": "MQS",
                        "values": [3, 4, 5],
                        "interpolation": "Step"
                    }
                },
                {
                    "datetimes": [
                        "2011-07-15T23:01:01.450Z",
                        "2011-07-16T00:01:01.450Z"
                    ],
                    "camera": {
                        "type": "Image",
                        "values": [
                        "http://.../example/image1",
                        "VBORw0KGgoAAAANSUhEU......"
                        ],
                        "interpolation": "Discrete"
                    },
                    "labels": {
                        "type": "Text",
                        "values": ["car", "human"],
                        "interpolation": "Discrete"
                    }
                }]
            })
        
        feature_collection = {
            "type": "FeatureCollection",
            "features": features
        }
        
        resp = requests.post(
            f"{HOST}/collections/{collection_id}/items",
            json=feature_collection,
            headers={"Content-Type": "application/json"}
        )
        
        if resp.status_code in (201, 409):
            created_count += len(batch)
            print(f"Batch: Created {len(batch)} features")
    
    print(f"Successfully created {created_count} features")
    assert created_count > 0


# ####################################################### GET MOVING FEATURES ##################################################""
#=======================================================GET /collections/{id}/items==============================================
def test_get_all_items(create_collections):
    collection_id = "ships"
    resp = requests.get(f"{HOST}/collections/{collection_id}/items")
    log_request_response(f"GET all items from {collection_id}", resp)
    assert resp.status_code == 200

    data_resp = resp.json()
    assert data_resp["type"] == "FeatureCollection"
    assert "features" in data_resp
    assert "numberMatched" in data_resp
    assert "numberReturned" in data_resp
    assert "links" in data_resp
    assert data_resp["numberReturned"] == len(data_resp["features"])

#==================================================GET /collections/{id}/items?limit=1==========================================
def test_get_items_with_limit(create_collections):

    collection_id = "ships"
    limit = 1
    resp = requests.get(
        f"{HOST}/collections/{collection_id}/items?limit={limit}")
    log_request_response(f"GET items with limit={limit}", resp)
    assert resp.status_code == 200

    data_resp = resp.json()
    assert len(data_resp["features"]) <= limit
    assert data_resp["numberReturned"] == len(data_resp["features"])


#=================================================GET /collections/{id}/items?bbox=...====================================================
def test_get_items_with_bbox(create_collections):
   
    collection_id = "ships"
    bbox = "12.675237,54.524345,12.685237,54.534345" #check , string or obj array?
    resp = requests.get(
        f"{HOST}/collections/{collection_id}/items?bbox={bbox}")
    log_request_response(f"GET items with bbox={bbox}", resp)
    assert resp.status_code == 200

    data_resp = resp.json()
    #first 1s temp geo -check
    #BBOX is returned null , recheck
    for f in data_resp["features"]:
        if "temporalGeometry" in f and f["temporalGeometry"]:
            coords = f["temporalGeometry"][0]["coordinates"]
            if coords:
                x, y = coords[0][0], coords[0][1]
                x1, y1, x2, y2 = map(float, bbox.split(','))
                assert x1 <= x <= x2
                assert y1 <= y <= y2


#==============================================GET /collections/{id}/items?datetime=...===============================================
def test_get_items_with_datetime(create_collections):
    
    collection_id = "ships"
    dt = urllib.parse.quote("2024-03-01T00:00:00+01")
    resp = requests.get(
        f"{HOST}/collections/{collection_id}/items?datetime={dt}")
    log_request_response(f"GET items with datetime={dt}", resp)
    assert resp.status_code == 200

#=======================================GET /collections/{id}/items?limit=invalid should return 400==================================
def test_get_items_invalid_limit(create_collections):
    
    collection_id = "ships"
    resp = requests.get(
        f"{HOST}/collections/{collection_id}/items?limit=invalid")
    log_request_response("GET items with invalid limit", resp)
    assert resp.status_code == 400


#=======================================GET with subTrajectory=true but no datetime should return 400========================================
def test_get_items_subtrajectory_without_datetime(create_collections):
    
    collection_id = "ships"
    resp = requests.get(
        f"{HOST}/collections/{collection_id}/items?subTrajectory=true")
    log_request_response("GET items with subTrajectory=true without datetime", resp)
    assert resp.status_code == 400


#================================================GET with subTrajectory=true and datetime interval=============================================
def test_get_items_subtrajectory_with_interval(create_collections):
    
    collection_id = "ships"
    interval = urllib.parse.quote("2024-03-01T00:00:00+01/2024-03-01T01:00:00+01") #re check ogc /
    resp = requests.get(
        f"{HOST}/collections/{collection_id}/items?subTrajectory=true&datetime={interval}")
    log_request_response(f"GET items with subTrajectory interval={interval}", resp)
    assert resp.status_code == 200


#check get mfs
#==============================================GET with leaf=true should return only the last instant=======================================
def test_get_items_leaf(create_collections):
    
    collection_id = "ships"
    resp = requests.get(f"{HOST}/collections/{collection_id}/items?leaf=true")
    log_request_response(f"GET items with leaf=true", resp)
    assert resp.status_code == 200
    data_resp = resp.json()
    
    for f in data_resp["features"]:
        if "temporalGeometry" in f and f["temporalGeometry"]:
            tg = f["temporalGeometry"][0]
            if "coordinates" in tg and "datetimes" in tg:
                assert len(tg["coordinates"]) == 1
                assert len(tg["datetimes"]) == 1
                assert tg.get("interpolation") == "Discrete"
                #assert it's the last re check

#=============================================GET with leaf=true and subTrajectory=true should return 400===============================
def test_get_items_leaf_with_subtrajectory(create_collections):
    
    collection_id = "ships"
    interval = urllib.parse.quote("2024-03-01T00:00:00+01/2024-03-01T01:00:00+01")
    resp = requests.get(
        f"{HOST}/collections/{collection_id}/items?leaf=true&subTrajectory=true&datetime={interval}")
    log_request_response(f"GET items with leaf=true & subTrajectory=true", resp)
    assert resp.status_code == 400


#****************************************************SINGLE MOVING FEATURE*************************************************************
#============================================GET /collections/{id}/items/{featureId}===================================================
def test_get_single_moving_feature(create_collections):
   
    collection_id = "ships"
    feature_id = str(get_feature_by_index(data, 0)["mmsi"])  
    resp = requests.get(f"{HOST}/collections/{collection_id}/items/{feature_id}")
    
    print(f"\n=== GET single feature {feature_id} ===")
    print(f"==> URL: {resp.url}")
    print(f"<== Status: {resp.status_code}") #log req response check not urgent clean
    try:
        print("Response JSON:", json.dumps(resp.json(), indent=2))
    except Exception:
        print("Response Text:", resp.text)
    print("="*60)

    assert resp.status_code == 200

    feature = resp.json()
    assert feature["type"] == "Feature"
    assert feature["id"] == feature_id
    # assert "geometry" in feature
    assert "properties" in feature
    assert "links" in feature
    
    if feature.get("temporalGeometry"): #check geometry ogc list
        assert isinstance(feature["temporalGeometry"], list)

#================================================DELETE /collections/{id}/items/{featureId}===========================================
def test_delete_single_moving_feature(create_collections):
    
    collection_id = "ships"
    feature_id = str(get_feature_by_index(data, 0)["mmsi"])  # was data[0]["mmsi"]

    resp = requests.delete(f"{HOST}/collections/{collection_id}/items/{feature_id}")
    
    print(f"\n=== DELETE single feature {feature_id} ===")
    print(f"→ URL: {resp.url}")
    print(f"← Status: {resp.status_code}")
    print("="*60)

    assert resp.status_code in (200, 204)

    resp_check = requests.get(f"{HOST}/collections/{collection_id}/items/{feature_id}")
    assert resp_check.status_code == 404
    #assert cascade delete? optional clean

# ******************************************************TEMPORAL GEOMETRY SEQUENCE ****************************************************

#=================================================GET /collections/{id}/items/{featureId}/tgsequence=================================================
def test_get_tgsequence(create_collections):
    
    collection_id = "ships"
    feature_id = str(get_feature_by_index(data, 0)["mmsi"])  
    
    resp = requests.get(
        f"{HOST}/collections/{collection_id}/items/{feature_id}/tgsequence"
    )
    
    log_request_response("GET tgsequence", resp)
    assert resp.status_code in (200, 404)

# =====================================================TEMPORAL GEOMETRY QUERY TESTS===================================================
#======================================Test GET /collections/{id}/items/{fid}/tgsequence/{gid}/distance====================================
def test_distance_query(setup_query_test_data):
    data = setup_query_test_data

    resp = requests.get(
        f"{HOST}/collections/{data['collection_id']}/items/{data['feature_id']}/tgsequence/{data['geometry_id']}/distance"
    )

    log_request_response("Distance query", resp)

    assert resp.status_code == 200

    result = resp.json()

    assert result["type"] == "TReal"

    assert "values" in result
    assert isinstance(result["values"], dict)

    first_point = result["values"]

    assert "datetimes" in first_point
    assert "values" in first_point

    assert isinstance(first_point["datetimes"], list)
    assert isinstance(first_point["values"], list)

    assert len(first_point["values"]) > 1

    assert isinstance(first_point["values"][0], (int, float))

    vals = first_point["values"]

    for i in range(1, len(vals)):
        assert vals[i] > vals[i - 1]

    assert result["form"] == "meters"
    assert result["name"] == "distance"
    assert "links" in result

#==================================================Test GET /collections/{id}/items/{fid}/tgsequence/{gid}/velocity===================================================================
def test_velocity_query(setup_query_test_data):
    data = setup_query_test_data

    resp = requests.get(
        f"{HOST}/collections/{data['collection_id']}/items/{data['feature_id']}/tgsequence/{data['geometry_id']}/velocity"
    )

    log_request_response("Velocity query", resp)

    assert resp.status_code == 200

    result = resp.json()

    assert result["type"] == "TReal"

    assert "values" in result
    assert isinstance(result["values"], dict)

    first_point = result["values"]

    assert "datetimes" in first_point
    assert "values" in first_point

    assert isinstance(first_point["datetimes"], list)
    assert isinstance(first_point["values"], list)

    assert len(first_point["values"]) > 1

    assert isinstance(first_point["values"][0], (int, float))

    assert result["form"] == "m/s"
    assert result["name"] == "velocity"

    assert "links" in result

#===========================================Test GET /collections/{id}/items/{fid}/tgsequence/{gid}/acceleration======================================
def test_acceleration_query(setup_query_test_data):
    data = setup_query_test_data

    resp = requests.get(
        f"{HOST}/collections/{data['collection_id']}/items/{data['feature_id']}/tgsequence/{data['geometry_id']}/acceleration"
    )

    log_request_response("Acceleration query", resp)

    assert resp.status_code == 200

    result = resp.json()

    assert result["type"] == "TReal"

    assert "values" in result
    assert isinstance(result["values"], dict)

    first_point = result["values"]

    assert "datetimes" in first_point
    assert "values" in first_point

    assert isinstance(first_point["datetimes"], list)
    assert isinstance(first_point["values"], list)

    assert len(first_point["values"]) >= 1

    assert isinstance(first_point["values"][0], (int, float))

    assert result["form"] == "m/s²"
    assert result["name"] == "acceleration"

    assert "links" in result
#======================================================Test query WITH non-existent TEMP geom id======================================
def test_query_with_invalid_geometry_id(setup_query_test_data):
    
    data = setup_query_test_data
    resp = requests.get(
        f"{HOST}/collections/{data['collection_id']}/items/{data['feature_id']}/tgsequence/999999/distance"
    )
    log_request_response("Invalid temporal geometry id", resp)
    
    assert resp.status_code == 404

#=====================================================Test query with non-existent feature id=========================================
def test_query_with_invalid_feature_id(setup_query_test_data):
    
    data = setup_query_test_data
    resp = requests.get(
        f"{HOST}/collections/{data['collection_id']}/items/invalid_feature/tgsequence/{data['geometry_id']}/distance"
    )
    log_request_response("Invalid feature ID", resp)
    
    assert resp.status_code == 404

#======================================================Test query with non-existent collection id=======================================================
def test_query_with_invalid_collection_id(setup_query_test_data):
    
    data = setup_query_test_data
    resp = requests.get(
        f"{HOST}/collections/invalid_collection/items/{data['feature_id']}/tgsequence/{data['geometry_id']}/distance"
    )
    log_request_response("Invalid collection ID", resp)
    
    assert resp.status_code == 404


#*************************************************************TEMPORAL PROPERTIES TESTS*************************************************************
#----------------------------------------------------------------------stop--------------------------------------------------------------------------------
#====================================================Create collection and feature for property testing===============================================
@pytest.fixture(scope="module")
def setup_property_test_data():
    
    # Create collection
    collection_data = {
        "title": "prop_test",
        "description": "Collection for property testing",
        "updateFrequency": 1000,
        "itemType": "movingfeature"
    }
    resp = requests.post(f"{HOST}/collections", json=collection_data)
    assert resp.status_code in (201, 409)
    
    collection_id = "prop_test"
    
    TemporalGeom = {
        "type": "MovingPoint",
        "datetimes": [
            "2024-03-01 00:00:00+00",
            "2024-03-01 00:15:00+00",
            "2024-03-01 00:30:00+00",
            "2024-03-01 00:45:00+00",
            "2024-03-01 01:00:00+00"
        ],
        "coordinates": [
            [12.675237, 54.524345],
            [12.685237, 54.534345],
            [12.695237, 54.544345],
            [12.705237, 54.554345],
            [12.715237, 54.564345]
        ],
        "interpolation": "Linear"
    }
    
    feature = {
        "type": "Feature",
        "id": "prop_test_001",
         "crs": {  #
        "type": "name",
        "properties": "urn:ogc:def:crs:EPSG::4326"  
    },
        "temporalGeometry": TemporalGeom,
        "properties": {"name": "Property Test Feature"}
    }
    
    resp = requests.post(
        f"{HOST}/collections/{collection_id}/items",
        json=feature,
        headers={"Content-Type": "application/json"}
    )
    assert resp.status_code in (201, 409)
    
    yield {
        "collection_id": collection_id,
        "feature_id": "prop_test_001"
    }
    
    # Clean
    # print("\n=== CLEANING UP PROPERTY TEST DATA ===")
    # requests.delete(f"{HOST}/collections/{collection_id}/items/prop_test_001")
    # requests.delete(f"{HOST}/collections/{collection_id}")

#============================================================POST /collections/{id}/items/{fid}/tproperties==========================================
def test_create_temporal_property(setup_property_test_data):
    
    data = setup_property_test_data
    property_data = {
        "name": "speed",
        "type": "TReal",
        "form": "KMH",
        "description": "Speed over ground"
    }
    
    resp = requests.post(
        f"{HOST}/collections/{data['collection_id']}/items/{data['feature_id']}/tproperties",
        json=property_data
    )
    log_request_response("Create temporal property", resp)
    assert resp.status_code in (201,409)

#============================================================POST /collections/{id}/items/{fid}/tproperties with values==========================================
def test_create_temporal_property_with_values(setup_property_test_data):
    
    data = setup_property_test_data
    property_data ={
        "datetimes": [
            "2011-07-14T22:01:06.000Z",
            "2011-07-14T22:01:07.000Z",
            "2011-07-14T22:01:08.000Z"
        ],
        "speedv2": {
            "type": "TReal",
            "form": "KMH",
            "values": [65.0, 70.0, 80.0],
            "interpolation": "Linear"
        }
        }
            
    resp = requests.post(
        f"{HOST}/collections/{data['collection_id']}/items/{data['feature_id']}/tproperties",
        json=property_data
    )
    log_request_response("Create temporal property", resp)
    assert resp.status_code in (201,409)



#=========================================================GET /collections/{id}/items/{fid}/tproperties==============================
def test_get_temporal_properties_list(setup_property_test_data):
   
    data = setup_property_test_data
    resp = requests.get(
        f"{HOST}/collections/{data['collection_id']}/items/{data['feature_id']}/tproperties"
    )
    log_request_response("Get temporal properties list", resp)
    assert resp.status_code == 200
    result = resp.json()
    assert "temporalProperties" in result
    # assert len(result["temporalProperties"]) > 0



def test_tproperties_subtemporal_with_interval(setup_property_test_data):
    data = setup_property_test_data

    interval = urllib.parse.quote(
        "2011-07-14T22:01:06+00/2011-07-14T22:01:07+00"
    )

    resp = requests.get(
        f"{HOST}/collections/{data['collection_id']}/items/{data['feature_id']}/tproperties?subTemporalValue=true&datetime={interval}"
    )
    log_request_response("Get temporal properties list with subtemporal", resp)

    assert resp.status_code == 200

 #====================================================POST /collections/{id}/items/{fid}/tproperties/{property-name}=================================
def test_add_temporal_values(setup_property_test_data):
   
    data = setup_property_test_data
    values_data = {
        "datetimes": ["2024-03-01T00:00:00Z", "2024-03-01T01:00:00Z"],
        "values": [15.5, 16.2],
        "interpolation": "Linear"
    }
    
    resp = requests.post(
        f"{HOST}/collections/{data['collection_id']}/items/{data['feature_id']}/tproperties/speed",
        json=values_data
    )
    log_request_response("Add temporal values", resp)
    assert resp.status_code == 201

#==========================================================GET /collections/{id}/items/{fid}/tproperties/{name}=======================
def test_get_temporal_property(setup_property_test_data):
   
    data = setup_property_test_data
    resp = requests.get(
        f"{HOST}/collections/{data['collection_id']}/items/{data['feature_id']}/tproperties/speed"
    )
    log_request_response("Get temporal property", resp)
    assert resp.status_code == 200
    result = resp.json()
    assert result["name"] == "speed"
    assert result["type"] == "TReal"
    assert "values" in result #check ogc 
    assert len(result["values"]) > 0

#============================================================DELETE /collections/{id}/items/{fid}/tproperties/{name}===========================
def test_delete_temporal_property(setup_property_test_data):
    
    data = setup_property_test_data
    resp = requests.delete(
        f"{HOST}/collections/{data['collection_id']}/items/{data['feature_id']}/tproperties/speed"
    )
    log_request_response("Delete temporal property", resp)
    assert resp.status_code in (200, 204)
    
    # Verify deletion
    resp = requests.get(
        f"{HOST}/collections/{data['collection_id']}/items/{data['feature_id']}/tproperties/speed"
    )
    assert resp.status_code == 404
    # assert cascade delete temporal values #check optional


    ####subtemporal is missing 
# ============================================== TEST delete all collection==============================================
# def test_delete_all_created_collections(create_collections):
#     created_collections = ["ships", "boats"]
#     for col_id in created_collections:
#         resp = requests.delete(f"{HOST}/collections/{col_id}")
#         print(f"Deleting collection {col_id} ====> status: {resp.status_code}")
#         assert resp.status_code in (200, 204, 404)


def test_finalize():
    pymeos_finalize()
    print("#######################################################################################")
    print("***************************************END OF TESTS************************************")
    print("#######################################################################################")