# REQ42: /req/movingfeatures/tproperty-post
# REQ45: /req/movingfeatures/tproperty-post-success
# REQ 47: /req/movingfeatures/tpvalue-mandatory
import traceback
from utils import send_json_response
from resource.temporal_properties.property_helper import validate_interpolation
from resource.temporal_properties.validation_helper import (
    parse_request_body, validate_value_data,
    validate_collection_exists, validate_feature_exists,
    validate_property_exists, validate_temporal_continuity
)
import json
from datetime import datetime
# POST /collections/{collectionId}/items/{featureId}/tproperties/{propertyName}  temporal_values table
#rename to post temporal property value, misleading, check clean
def post_temporal_property(self, collection_id, feature_id, property_name, connection, cursor):
    try:
        data = parse_request_body(self)
        # Validate 
        errors = validate_value_data(data)
        if errors:
            self.handle_error(400, "; ".join(errors))
            return
        
        # Acceptable interpolation values?
        if "interpolation" in data and not validate_interpolation(data["interpolation"]):
            self.handle_error(400, f"Invalid interpolation: {data['interpolation']}")
            return
        
        # collection exists
        if not validate_collection_exists(cursor, collection_id):
            self.handle_error(404, f"Collection '{collection_id}' not found")
            return
        
        # feature exists
        if not validate_feature_exists(cursor, feature_id, collection_id):
            self.handle_error(404, f"Feature '{feature_id}' not found in collection '{collection_id}'")
            return
        
        # property exists?
        prop_row = validate_property_exists(cursor, feature_id, property_name)
        if prop_row is None:
            self.handle_error(404, f"Property '{property_name}' not found for feature '{feature_id}'")
            return
        
        property_id = prop_row[0]
#New values must start after existing data (last time::
        first_new_time = datetime.fromisoformat(data["datetimes"][0].replace('Z', '+00:00'))
        is_valid, last_time = validate_temporal_continuity(cursor, property_id, first_new_time)
        
        if not is_valid:
            self.handle_error(400, f"New values must start after existing data (last time: {last_time.isoformat()})")
            return
        
        # datetimes----> pgsql acceptable timestamp format
        # Note: pgsql expects timestamps without 'Z' and with space instead of 'T'
        pg_datetimes = []
        for dt_str in data["datetimes"]:
            pg_dt = dt_str.replace('T', ' ').replace('Z', '+00')
            pg_datetimes.append(pg_dt)
        
        # INSERT INTO temporal_values:
        cursor.execute("""
            INSERT INTO temporal_values 
            (property_id, datetimes, values, interpolation)
            VALUES (%s, %s::timestamptz[], %s, %s)
            RETURNING id
        """, (
            property_id,
            pg_datetimes,  
            json.dumps(data["values"]),#jsonb
            data.get("interpolation", "Linear")
        ))
        
        new_id = cursor.fetchone()[0]
        connection.commit()
        
        #201
        #re check content clean
        base_url = f"http://{self.server.server_name}:{self.server.server_port}" #clean mod 14/03 **
        path = f"/collections/{collection_id}/items/{feature_id}/tproperties/{property_name}/{new_id}"
        Location = f"{base_url}{path}"  #clean mod 14/03 **
        self.send_response(201)  
        self.send_header("Location", Location)   #clean mod 14/03 **
        send_json_response(self, 201, {"message": "Values added successfully", "id": new_id})
        
    except json.JSONDecodeError:
        self.handle_error(400, "Invalid JSON")
    except Exception as e:
        connection.rollback()
        print(f"Error in post_temporal_property: {e}")
        traceback.print_exc()
        self.handle_error(500, f"Internal server error: {str(e)}")