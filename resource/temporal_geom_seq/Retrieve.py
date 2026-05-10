# REQ 25: /req/movingfeatures/tgsequence-get
# REQ 27: /req/movingfeatures/tgsequence-get-success

from utils import send_json_response
from datetime import datetime
import json
# GET base/collections/{collectionId}/items/{featureId}/tgsequence
def get_tgsequence(self, connection, cursor):

    try:
        # base/collections/{collectionId}/items/{featureId}/tgsequence
        parsed_path = self.path.split('/')
        collection_id = parsed_path[2]
        feature_id = parsed_path[4]
        
        #"""""""""""""""""""""""""""""""""""""""""collection && feature with provided ids exsit:::
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
            self.handle_error(404, f"Feature '{feature_id}' not found")
            return
        # """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""":::

        # Get temporal geometries for this feature
        cursor.execute("""
            SELECT 
                id,
                geometry_type,
                asMFJSON(trajectory) as trajectory,
                interpolation,
                base
            FROM temporal_geometries
            WHERE feature_id = %s and collection_id = %s
            ORDER BY id
        """, (feature_id,collection_id))
        
        rows = cursor.fetchall()
        
        geometries = []
        for row in rows:
            traj = json.loads(row[2]) if row[2] else {}
            geometries.append({
                "id": row[0],
                "type": row[1],
                "datetimes": traj.get("datetimes", []),
                "coordinates": traj.get("coordinates", []),
                "interpolation": row[3],
                "base": row[4]
            })
        
        # ref Table 9 ogc 
        response = {
            "type": "TemporalGeometrySequence",
            "geometrySequence": geometries,
            "links": [
                {
                    "href": f"/collections/{collection_id}/items/{feature_id}/tgsequence",
                    "rel": "self",
                    "type": "application/json"
                }
            ],
            #deprecation clean
            "timeStamp": datetime.utcnow().isoformat() + "Z",
            "numberMatched": len(geometries),
            "numberReturned": len(geometries)
        }
        send_json_response(self, 200, response)
        
    except Exception as e:
        # print(f"Error in get_tgsequence: {e}")
        self.handle_error(500, f"Internal server error: {str(e)}")

