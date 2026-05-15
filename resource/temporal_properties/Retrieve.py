# REQ36: /req/movingfeatures/tproperties-get
# REQ38: /req/movingfeatures/tproperties-get-success

from utils import send_json_response
from resource.temporal_properties.property_helper import build_properties_list_response
import json
from urllib.parse import urlparse, parse_qs
import traceback
# GET properties  base/collections/{collectionId}/items/{featureId}/tproperties
def get_tproperties(self, collection_id, feature_id, connection, cursor):
    try:


        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)
        

        try:
            limit = min(int(query_params.get('limit', [10])[0]), 10000)  # Req 50: max 10000
        except ValueError:
            return self.handle_error(400, "Invalid limit parameter")

        datetime_param = query_params.get('datetime', [None])[0]
        subTemporalValue = query_params.get('subTemporalValue', ['false'])[0].lower() == "true" #only if true
 # Parse datetime (Req52)
        dt1 = dt2 = None
        if datetime_param:
            if "/" in datetime_param:
                dt1, dt2 = datetime_param.split("/")
                # subTrajectory== true==> bounder interval (Req 12C)
                if subTemporalValue and (not dt1 or not dt2):
                    return self.handle_error(400, "subTrajectory requires a bounded datetime interval")
            else:
                dt1 = datetime_param  # instant
                if subTemporalValue:
                    return self.handle_error(400, "subTrajectory requires a bounded interval, not a single instant")
        
        # subTrajectory without datetime interval code 400
        if subTemporalValue and not (dt1 and dt2):
            return self.handle_error(400, "subTrajectory requires a datetime interval")

        #collection exists?
        cursor.execute(
            "SELECT id FROM collections WHERE id = %s",
            (collection_id,)
        )
        if cursor.fetchone() is None:
            self.handle_error(404, f"Collection '{collection_id}' not found")
            return
        
        # feature exists?
        cursor.execute(
            "SELECT id FROM moving_features WHERE id = %s AND collection_id = %s",
            (feature_id, collection_id)
        )
        if cursor.fetchone() is None:
            self.handle_error(404, f"Feature '{feature_id}' not found in collection '{collection_id}'")
            return
        properties = []
        if subTemporalValue:
            if not (dt1 and dt2):
                return self.handle_error(400, "subTemporalValue requires a datetime interval")
            query = """
                SELECT
                    tp.property_name,
                    tp.property_type,
                    tp.form,
                    tp.description,
                    tv.interpolation,
                    array_agg(d.t ORDER BY d.idx) AS datetimes,
                    array_agg(v.val::float ORDER BY d.idx) AS values
                FROM temporal_properties tp
                LEFT JOIN temporal_values tv ON tp.id = tv.property_id
                CROSS JOIN LATERAL unnest(tv.datetimes) WITH ORDINALITY AS d(t, idx)
                CROSS JOIN LATERAL jsonb_array_elements_text(tv.values) WITH ORDINALITY AS v(val, idx2)
                WHERE tp.feature_id = %s
                  AND d.idx = v.idx2
                  AND d.t >= %s AND d.t <= %s
                GROUP BY tp.property_name, tp.property_type, tp.form, tp.description, tv.interpolation
                ORDER BY tp.property_name
                LIMIT %s
            """
            cursor.execute(query, (feature_id, dt1, dt2, limit))
            rows = cursor.fetchall()
            temporal_properties_obj = {"datetimes": []}
            for row in rows:
                name = row[0]
                if not temporal_properties_obj["datetimes"]:
                    temporal_properties_obj["datetimes"] = [dt.isoformat() for dt in row[5]]
                temporal_properties_obj[name] = {
                    "type": row[1],
                    "form": row[2],
                    "values": row[6],
                    "interpolation": row[4] or "Linear",
                    "description": row[3],
                }
            properties = [temporal_properties_obj] if rows else []
        else:

            query = """
                SELECT DISTINCT tp.property_name, tp.property_type, tp.form, tp.description
                FROM temporal_properties tp
                LEFT JOIN temporal_values tv ON tp.id = tv.property_id
                WHERE tp.feature_id = %s
            """
            params = [feature_id]
            if dt1 and dt2:
                query += """ AND EXISTS (
                    SELECT 1 FROM unnest(tv.datetimes) AS d(t)
                    WHERE d.t >= %s AND d.t <= %s
                )"""
                params += [dt1, dt2]
            query += " ORDER BY tp.property_name LIMIT %s"
            params.append(limit)
            cursor.execute(query, params)
            rows = cursor.fetchall()
            for row in rows:
                properties.append({
                    "name": row[0],
                    "type": row[1],
                    "form": row[2],
                    "interpolation": "linear",
                    "description": row[3]
                })
        # response
        base_url = f"http://{self.server.server_name}:{self.server.server_port}"
        path = f"/collections/{collection_id}/items/{feature_id}/tproperties"
        
        response = build_properties_list_response(properties, base_url, path)
        send_json_response(self, 200, response)
        
    except Exception as e:
        connection.rollback()
        print(f"Error in get_tproperties: {e}", flush=True)
        traceback.print_exc()
        self.handle_error(500, f"Internal server error: {str(e)}")