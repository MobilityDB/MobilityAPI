# REQ 2: /req/mf-collection/collections-post
# REQ 4: /req/mf-collection/collections-post-success

from http.server import BaseHTTPRequestHandler, HTTPServer
from utils import send_json_response
from resource.collection.collection_helper import (
    validate_collection_data,
    collection_exists,
    insert_collection,
    build_collection_response
)
import json

def post_collections(self, connection, cursor):
    try:
        # Get and decode request body
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data_dict = json.loads(post_data.decode('utf-8'))
        
        #Attribute data validation, is_update is false for post operation
        errors, validated_data = validate_collection_data(data_dict, is_update=False)
        if errors:
            self.handle_error(400, "; ".join(errors))
            return
        
        collection_id = validated_data.pop("id") #for collection check and inserting if not exist
        
        #create collections table if it doesn't exist yet
        #RECHECK==>ogc clean
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS collections (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT,
                update_frequency INTEGER,
                item_type TEXT DEFAULT 'movingfeature',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS moving_features (
                id TEXT PRIMARY KEY,
                collection_id TEXT REFERENCES collections(id) ON DELETE CASCADE,
                type TEXT DEFAULT 'Feature',
                properties JSONB,
                bbox STBOX,
                time TSTZSPAN,
                crs JSONB DEFAULT '{"type":"Name","properties":{"name":"urn:ogc:def:crs:OGC:1.3:CRS84"}}'::jsonb,
                trs JSONB DEFAULT '{"type":"Name","properties":{"name":"urn:ogc:data:time:iso8601"}}'::jsonb,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS temporal_geometries (
                id SERIAL PRIMARY KEY,
                feature_id TEXT REFERENCES moving_features(id) ON DELETE CASCADE,
                collection_id TEXT REFERENCES collections(id) ON DELETE CASCADE,
                geometry_type TEXT,
                geometry geometry,
                trajectory tgeompoint,
                interpolation TEXT,
                base JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        connection.commit()
        
        # check if collection with same id already exists eg id = netherlands_ships (collection_helper.py)
        if collection_exists(cursor, collection_id):
            self.handle_error(409, f'Collection "{data_dict["title"]}" already exists.')
            return
        
#       else new collection id :
        insert_collection(cursor, collection_id, validated_data)
        connection.commit()
        
        # __________________________start Build response:
        base_url = f"http://{self.server.server_name}:{self.server.server_port}"
        
        # Reconstruct collection data for response 
        
        collection_data = {
            "id": collection_id,
            "title": validated_data.get("title"),
            "description": validated_data.get("description"),
            "item_type": validated_data.get("itemType", "movingfeature"),
            "update_frequency": validated_data.get("updateFrequency")
        }
        #!!>  recheck code clean collection_data
        # response = build_collection_response(collection_data, base_url)
        # __________________________end Build response

        # As per OGC, 201 new collection created successfully + endpoint to new collection
        self.send_response(201)
        self.send_header("Location", f"{base_url}/collections/{collection_id}")
        send_json_response(self, 201)
        
    except json.JSONDecodeError:
        self.handle_error(400, "Invalid JSON- (check ogc specifications for more details)")
    except Exception as e:
        connection.rollback()
        print(f"Error in post_collections: {e}")
        self.handle_error(500, f"Internal server error: {str(e)}")