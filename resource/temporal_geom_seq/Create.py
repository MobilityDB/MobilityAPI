# REQ 26: /req/movingfeatures/tgsequence-post
# REQ 28: /req/movingfeatures/tgsequence-post-success
from utils import send_json_response
import json

    # POST base/collections/{collectionId}/items/{featureId}/tgsequence
def post_tgsequence(self, connection, cursor):
    try:
        #base/collections/{collectionId}/items/{featureId}/tgsequence
        parsed_path= self.path.split('/')
        collection_id = parsed_path[2]
        feature_id = parsed_path[4]
        
        # LOad request body
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data_dict = json.loads(post_data.decode('utf-8'))
        
        #collection exists?
        cursor.execute(
            "SELECT id FROM collections WHERE id = %s",
            (collection_id,)
        )
        if cursor.fetchone() is None:
            self.handle_error(404, f"Collection '{collection_id}' not found")
            return
        print("eollction",flush=True)
        # feature exists?
        cursor.execute(
            "SELECT id FROM moving_features WHERE id = %s AND collection_id = %s",
            (feature_id, collection_id)
        )
        print("feature",flush=True)
        if cursor.fetchone() is None:
            self.handle_error(404, f"Feature '{feature_id}' not found")
            return
        

        #get srid of mfeature :
        cursor.execute(
            "SELECT crs FROM moving_features WHERE id = %s AND collection_id = %s",
            (feature_id, collection_id)
        )
        crs = cursor.fetchone()
  

        import re
        match = re.search(r'(\d+)', str(crs[0]["properties"]))
        srid = int(match.group(1))
        tgeom_mfjson = json.dumps(data_dict)
        # INSERT INTO temporal_geometries 
        columns = ["feature_id","collection_id","geometry_type","geometry",
            "trajectory",
            "interpolation"
        ]
        values = [ feature_id,collection_id,data_dict.get("type", "MovingPoint"),tgeom_mfjson,srid,tgeom_mfjson,
            srid,
            data_dict.get("interpolation", "Linear")
        ]
        placeholders = [
            "%s", "%s", "%s",
            "trajectory(SETSRID(tgeompointFromMFJSON(%s), %s))",
            "SETSRID(tgeompointFromMFJSON(%s), %s)",
            "%s"
        ]
        if data_dict.get("base") is not None:
            columns.append("base")
            placeholders.append("%s")
            values.append(data_dict.get("base"))

        if data_dict.get("orientations") is not None:
            columns.append("orientations")
            placeholders.append("%s")
            values.append(data_dict.get("orientations"))


        query = f"""
                INSERT INTO temporal_geometries 
                ({", ".join(columns)})
                VALUES (
                    {", ".join(placeholders)}
                )
                RETURNING ID
            """
        cursor.execute(query, values)        
        new_id = cursor.fetchone()[0]
        connection.commit()
        print("ididid",new_id, flush=True)
        # codde 201 success + Location
        self.send_response(201)
        self.send_header("Location", f"/collections/{collection_id}/items/{feature_id}/tgsequence/{new_id}")
        send_json_response(self, 201, data_dict)
        
    except Exception as e:
        connection.rollback()
        print(f"Error in post_tgsequence: {e}",flush=True)
        self.handle_error(500, f"Internal server error: {str(e)}")