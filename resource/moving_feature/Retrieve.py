# REQ 19: /req/movingfeatures/mf-get
# REQ 21: /req/movingfeatures/mf-get-success

from http.server import BaseHTTPRequestHandler, HTTPServer
from utils import send_json_response
from resource.moving_feature.feature_helper import build_feature_from_row
import json
import traceback

def get_movement_single_moving_feature(self, collection_id, feature_id, connection, cursor):
    try:
        #check collection exists--->
        cursor.execute(
            "SELECT id FROM collections WHERE id = %s",
            (collection_id,)
        )
        if cursor.fetchone() is None:
            self.handle_error(404, f"Collection '{collection_id}' not found")
            return
        #check moving features collection exists ? clean just like in Delete.py -->caught by exception- not urgent
        #feature + its aggregated temporal geoms
        cursor.execute("""
            SELECT 
                mf.id,
                mf.type,
                mf.properties,
                mf.bbox::text,
                mf.time::text,
                mf.crs,
                mf.trs,
                json_agg(
                    json_build_object(
                        'id', tg.id,
                        'type', tg.geometry_type,
                        'trajectory', asMFJSON(tg.trajectory),
                        'interpolation', tg.interpolation,
                        'base', tg.base
                    )
                ) FILTER (WHERE tg.id IS NOT NULL) as temporal_geometries
            FROM moving_features mf
            LEFT JOIN temporal_geometries tg ON mf.id = tg.feature_id
            WHERE mf.collection_id = %s AND mf.id = %s
            GROUP BY mf.id, mf.type, mf.properties, mf.bbox, 
                     mf.time, mf.crs, mf.trs
        """, (collection_id, feature_id))
        
        row = cursor.fetchone()
        
        if row is None:
            self.handle_error(404, f"Feature '{feature_id}' not found in collection '{collection_id}'")
            return
        
        feature = build_feature_from_row(row, collection_id, include_temporal=True, single=True)
        
        send_json_response(self, 200, json.dumps(feature))

    except Exception as e:
        print(f"Error retrieving feature: {e}")
        traceback.print_exc()
        self.handle_error(500, f"Internal server error: {str(e)}")