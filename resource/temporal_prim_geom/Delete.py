# REQ 30: /req/movingfeatures/tpgeometry-delete
# REQ 31: /req/movingfeatures/tpgeometry-delete-success

from utils import send_json_response


# DELETE base/collections/{collectionId}/items/{featureId}/tgsequence/{geometryId}
def delete_single_temporal_primitive_geo(self, collection_id, feature_id, geometry_id, connection, cursor):
    
    try:
        #---------------------------------collection && feature && geomerty exist ??---------------------------------------
        cursor.execute(
            "SELECT id FROM collections WHERE id = %s",
            (collection_id,)
        )
        if cursor.fetchone() is None:
            self.handle_error(404, f"Collection '{collection_id}' not found")
            return
        #feature exists?
        # addition 14/03 clean
        cursor.execute(
            "SELECT id FROM moving_features WHERE id = %s AND collection_id = %s",
            (feature_id, collection_id)
        )
        if cursor.fetchone() is None:
            self.handle_error(404, f"Feature '{feature_id}' not found")
            return
        # geometry exists (by collection by mf) 
        cursor.execute("""
            SELECT tg.id 
            FROM temporal_geometries tg
            JOIN moving_features mf ON tg.feature_id = mf.id
            WHERE tg.id = %s 
              AND mf.id = %s 
              AND mf.collection_id = %s
        """, (geometry_id, feature_id, collection_id))
        
        if cursor.fetchone() is None:
            self.handle_error(404, f"Temporal geometry {geometry_id} not found")
            return
        #----------------------------------------------------------------------------------------------------------------------
        # delete
        cursor.execute(
            """DELETE FROM temporal_geometries tg 
            WHERE tg.id = %s 
              AND tg.feature_id = %s 
              AND tg.collection_id = %s
        """, (geometry_id, feature_id, collection_id)
        )
        
        connection.commit()
        
        # response Req 31)
        self.send_response(204)
        self.end_headers()
        
    except Exception as e:
        connection.rollback()
        print(f"Error in delete_single_temporal_primitive_geo: {e}", flush=True)
        self.handle_error(500, f"Internal server error: {str(e)}")