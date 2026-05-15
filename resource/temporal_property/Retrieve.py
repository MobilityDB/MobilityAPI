# REQ 41: /req/movingfeatures/tproperty-get
# REQU 44: /req/movingfeatures/tproperty-get-success

from utils import send_json_response
import json
import traceback
from urllib.parse import urlparse, parse_qs

# GET /collections/{collectionId}/items/{featureId}/tproperties/{propertyName}
def get_temporal_property(self, collection_id, feature_id, property_name, connection, cursor):

    try:

        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)
        datetime_param = query_params.get('datetime', [None])[0]
        subTemporalValue = query_params.get('subTemporalValue', ['false'])[0].lower() == "true" #only if true
        # leaf =query_params.get('leaf', [None])[0]
        # Parse datetime (Req52)
        dt1 = dt2 = None
        if datetime_param:
            if "/" in datetime_param:
                dt1, dt2 = datetime_param.split("/")
                # subTrajectory== true==> bounder interval (Req 12C)
                if subTemporalValue and (not dt1 or not dt2):
                    return self.handle_error(400, "subTemporalValue requires a bounded datetime interval")
            else:
                dt1 = datetime_param  
                if subTemporalValue:
                    return self.handle_error(400, "subTemporalValue requires a bounded interval, not a single instant")
        
        # subTrajectory without datetime interval code 400
        if subTemporalValue and not (dt1 and dt2):
            return self.handle_error(400, "subTemporalValue requires a datetime interval")

       # collection && feature exist:+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        cursor.execute(
            "SELECT id FROM collections WHERE id = %s",
            (collection_id,)
        )
        if cursor.fetchone() is None:
            self.handle_error(404, f"Collection '{collection_id}' not found")
            return
    
        cursor.execute(
            "SELECT id FROM moving_features WHERE id = %s AND collection_id = %s",
            (feature_id, collection_id)
        )
        if cursor.fetchone() is None:
            self.handle_error(404, f"Feature '{feature_id}' not found in collection '{collection_id}'")
            return
        
   
        cursor.execute("""
            SELECT id FROM temporal_properties
            WHERE feature_id = %s AND property_name = %s
        """, (feature_id, property_name))
        
        prop_row = cursor.fetchone()
        if prop_row is None:
            self.handle_error(404, f"Property '{property_name}' not found for feature '{feature_id}'")
            return
        
        property_id = prop_row[0]
        

        temporal_properties = []
        
        if dt1 and dt2:
            if subTemporalValue:

                query = """
                    SELECT
                        array_agg(d.t ORDER BY d.idx) AS datetimes,
                        array_agg(v.val::float ORDER BY d.idx) AS values,
                        tv.interpolation
                    FROM temporal_values tv
                    CROSS JOIN LATERAL unnest(tv.datetimes) WITH ORDINALITY AS d(t, idx)
                    CROSS JOIN LATERAL jsonb_array_elements_text(tv.values) WITH ORDINALITY AS v(val, idx2)
                    WHERE tv.property_id = %s
                      AND d.idx = v.idx2
                      AND d.t >= %s AND d.t <= %s
                    GROUP BY tv.interpolation
                """
                cursor.execute(query, (property_id, dt1, dt2))
                row = cursor.fetchone()
                if row and row[0]:
                    temporal_properties.append({
                        "datetimes": [dt.isoformat() for dt in row[0]],
                        "values": row[1],
                        "interpolation": row[2] or "Linear"
                    })
            else:
                query = """
                    SELECT tv.datetimes, tv.values, tv.interpolation
                    FROM temporal_values tv
                    CROSS JOIN LATERAL unnest(tv.datetimes) AS d(t)
                    WHERE tv.property_id = %s
                      AND d.t >= %s AND d.t <= %s
                    ORDER BY tv.datetimes[1]
                """
                cursor.execute(query, (property_id, dt1, dt2))
                rows = cursor.fetchall()
                for row in rows:
                    temporal_properties.append({
                        "datetimes": [dt.isoformat() for dt in row[0]],
                        "values": row[1],
                        "interpolation": row[2] or "Linear"
                    })
        else:

            cursor.execute("""
                SELECT datetimes, values, interpolation
                FROM temporal_values
                WHERE property_id = %s
                ORDER BY datetimes[1]
            """, (property_id,))
            rows = cursor.fetchall()
            for row in rows:
                temporal_properties.append({
                    "datetimes": [dt.isoformat() for dt in row[0]],
                    "values": row[1],
                    "interpolation": row[2] or "Linear"
                })
        
        base_url = f"http://{self.server.server_name}:{self.server.server_port}"
        path = f"/collections/{collection_id}/items/{feature_id}/tproperties/{property_name}"
        
        response = {
            "temporalProperties": temporal_properties,
            "links": [
                {
                    "href": f"{base_url}{path}",
                    "rel": "self",
                    "type": "application/json"
                }
            ]
        }
        
        send_json_response(self, 200, response)
        
    except Exception as e:
        connection.rollback()
        print(f"Error in get_temporal_property: {e}")
        traceback.print_exc()
        self.handle_error(500, f"Internal server error: {str(e)}")