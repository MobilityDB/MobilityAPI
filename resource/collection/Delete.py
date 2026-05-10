# REQ 8: /req/mf-collection/collection-delete
# REQU11: /req/mf-collection/collection-delete-success

from http.server import BaseHTTPRequestHandler, HTTPServer

def delete_collection(self, collection_id, connection, cursor):
    try:
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'collections'
            )
        """)

        table_exists = cursor.fetchone()[0]

        if not table_exists:
            self.handle_error(404, f"Collection '{collection_id}' not found")
            return

        # check If exists 
        cursor.execute(
            "SELECT id FROM collections WHERE id = %s",
            (collection_id,)
        )
        if not cursor.fetchone():
            self.handle_error(404, f"Collection '{collection_id}' not found")
            return
        #clean recheck ogc
        # Delete the collection (on cascades deletes moving_features, temporal_geometries...) 
        cursor.execute(
            "DELETE FROM collections WHERE id = %s",
            (collection_id,)
        )
        
        connection.commit()
        
        # Delete success 204 no content
        self.send_response(204)
        self.end_headers()
        
    except Exception as e:
        connection.rollback()
        print(f"Error in delete_collection: {e}")
        self.handle_error(500, f"Internal server error: {str(e)}")