# REQ 32: /req/movingfeatures/tpgeometry-query
# REQ 33: /req/movingfeatures/tpgeometry-query-success
# SECTION 8.7.5. Acceleration Query 
from utils import send_json_response
from resource.temporal_geom_query.query_helper import build_query_response
import json
import traceback
from datetime import datetime

#GET /collections/{collectionId}/items/{featureId}/tgsequence/{geometryId}/acceleration
def get_acceleration(self, collection_id, feature_id, geometry_id, connection, cursor):
    try:
        #collection exists
        cursor.execute(
            "SELECT id FROM collections WHERE id = %s",
            (collection_id,)
        )
        if cursor.fetchone() is None:
            self.handle_error(404, f"Collection '{collection_id}' not found")
            return
        
        #feature exists 
        cursor.execute(
            "SELECT id FROM moving_features WHERE id = %s AND collection_id = %s",
            (feature_id, collection_id)
        )
        if cursor.fetchone() is None:
            self.handle_error(404, f"Feature '{feature_id}' not found in collection '{collection_id}'")
            return
        #  +modif 15/03 CLEAN
        #temp geom exists for feature 
        cursor.execute("""
            SELECT id FROM temporal_geometries 
            WHERE id = %s AND feature_id = %s and collection_id = %s
        """, (geometry_id, feature_id, collection_id))
        
        if cursor.fetchone() is None:
            self.handle_error(404, f"Temporal geometry '{geometry_id}' not found for feature '{feature_id}'")
            return
##############################################################################################################################
        cursor.execute("""
        SELECT 
            getTimestamp(unnest(instants(speed(trajectory)))) as time,
            getValue(unnest(instants(speed(trajectory)))) as speed
            FROM temporal_geometries
            WHERE id = %s AND feature_id = %s and collection_id = %s
        """, (geometry_id, feature_id, collection_id))
        rows = cursor.fetchall()
                
#remark: speed returns stepwise interpolation, derivative requires LInear, toLinear() doesn't accept tfloat type , for now python compute the derivative clean check
        
        print("Speed data:", rows)
        if len(rows) < 2:
            self.handle_error(404, f"Not enough speed data points for acceleration")
            return

        #ACCELERATION between each pair of points (tfloats) 
        datetimes = []
        acceleration_values = []
        for i in range(len(rows) - 1):
            t1, s1 = rows[i]
            t2, s2 = rows[i + 1]
            dt = (t2 - t1).total_seconds()
            if dt > 0:
                accel = (s2 - s1) / dt
                datetimes.append(
                    t2.isoformat() if hasattr(t2, "isoformat") else str(t2)
                )
                acceleration_values.append(float(accel))

        # optional first point = 0
        datetimes.insert(
            0,
            rows[0][0].isoformat() if hasattr(rows[0][0], "isoformat") else str(rows[0][0])
        )

        acceleration_values.insert(0, 0.0)

        values = {
            "datetimes": datetimes,
            "values": acceleration_values
        }
                #acc first point to 0

        if not values:
            self.handle_error(404, f"No acceleration data found")
            return
        # response
        base_url = f"http://{self.server.server_name}:{self.server.server_port}"
        path = f"/collections/{collection_id}/items/{feature_id}/tgsequence/{geometry_id}/acceleration"

        response = build_query_response(
            values=values,
            unit="m/s²",
            query_type="acceleration",
            base_url=base_url,
            path=path
        )

        send_json_response(self, 200, response)
        
    except Exception as e:
        connection.rollback()
        #clean traceback : cofirm acceleration is correct - derivative()?
        print(f"Error in acceleration query: {e}")
        traceback.print_exc()
        self.handle_error(500, f"Internal server error: {str(e)}")