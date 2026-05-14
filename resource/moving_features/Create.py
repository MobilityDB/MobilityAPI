# REQ15: /req/movingfeatures/features-post
# REQ 17: /req/movingfeatures/features-post-success
import uuid
import json
from psycopg2 import sql
import traceback
def post_collection_items(self, collection_id, connection, cursor):
    try:
        content_length = int(self.headers.get("Content-Length", 0))
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data.decode("utf-8"))

        object_type = data.get("type")
        if not object_type:
            raise Exception("DataError: Missing mandatory 'type'")

        # Check the target collection exists eg ships
        cursor.execute(
            "SELECT id FROM collections WHERE id = %s",
            (collection_id,)
        )
        if cursor.fetchone() is None:
            raise Exception(f'DataError: collection with id {collection_id} does not exist')

        created_feature_ids = []

        if object_type == "FeatureCollection":
            features = data.get("features")
            if not isinstance(features, list):
                raise Exception("DataError: FeatureCollection missing 'features' array")

            for feat in features:
                new_id = insert_feature(self, feat, collection_id, connection, cursor)
                if new_id:
                    created_feature_ids.append(new_id)

        elif object_type == "Feature":
            new_id = insert_feature(self, data, collection_id, connection, cursor)
            if new_id:
                created_feature_ids.append(new_id)
#object_type is neither Feature or FeatureCollection
        else:
            raise Exception("DataError: Invalid 'type'")

        connection.commit()

        # Req17: 201 POST with location headers
        self.send_response(201)
        self.send_header("Content-Type", "application/json")
        
        # Add Location header for each created feature
        for fid in created_feature_ids:
            self.send_header("Location", f"/collections/{collection_id}/items/{fid}")
        
        self.end_headers()
        
        response = {
            "message": f"Created {len(created_feature_ids)} features",
            "ids": created_feature_ids
        }
        self.wfile.write(bytes(json.dumps(response), "utf-8"))

    except Exception as e:
        connection.rollback()
        print(f"Error in post_tproperties: {e}")
        traceback.print_exc()
        msg = str(e)
        if "does not exist" in msg:
            code = 404
        elif "DataError" in msg:
            code = 400
        elif "duplicate key" in msg.lower():
            code = 409
        else:
            code = 500
        print("error", msg)
        self.handle_error(code, msg)

#add single moving feature to moving_features table
def insert_feature(self, feature, collection_id, connection, cursor):
    if feature.get("type") != "Feature":
        raise Exception("DataError: Invalid feature type")

    # generate or use given feature ID
    feat_id = feature.get("id")
    if feat_id is None:
        feat_id = str(uuid.uuid4())
    else:
        feat_id = str(feat_id)
    bbox_calculated = None
    time_range_calculated = None


    # *convert temporalGeometry to TGeomPoint
    temporal_geometry = feature.get("temporalGeometry")
    tgeom_mfjson=None
    if temporal_geometry:
        if isinstance(temporal_geometry, dict): 
            tgeom_mfjson= json.dumps(temporal_geometry)
            
        # elif isinstance(temporal_geometry, str):
        #     print("eeee",flush=True)
        #     tgeom_mfjson = temporal_geometry

        # time range
        # time_range_calculated = [stbox.tmin().isoformat(), stbox.tmax().isoformat()]
        
    properties = feature.get("properties", {})
    #mf life span time range:
    time_range = feature.get("time")
    crs = feature.get("crs")
    trs = feature.get("trs")

# __________________________________________check required tables exist____________________________________________
    #If moving_features table not exists, then create it
    #geometry Projective geometry of the moving feature. ? spatial project of temporal geom but one mf can have multiple tem geom????
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
    
    #If temporal_geometries table not exists, then create it
    #geometry eq trajectry and trajectory eq trip clean
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
            orientations JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cursor.execute("""
            CREATE OR REPLACE FUNCTION update_mfeatures_on_tg()
            RETURNS TRIGGER AS $$
            DECLARE
                target_feature_id TEXT;
                target_collection_id TEXT;
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    target_feature_id := OLD.feature_id;
                    target_collection_id := OLD.collection_id;
                ELSE
                    target_feature_id := NEW.feature_id;
                    target_collection_id := NEW.collection_id;
                END IF;

                -- recompute bbox + time for the parent moving feature
                UPDATE moving_features mf
                SET
                    bbox = (
                        SELECT extent(tg.trajectory)
                        FROM temporal_geometries tg
                        WHERE tg.feature_id = target_feature_id
                        AND tg.collection_id = target_collection_id
                    ),

                    time = (
                        SELECT extent(tg.trajectory)::tstzspan
                        FROM temporal_geometries tg
                        WHERE tg.feature_id = target_feature_id
                        AND tg.collection_id = target_collection_id
                    )

                WHERE mf.id = target_feature_id
                AND mf.collection_id = target_collection_id;

                RETURN COALESCE(NEW, OLD);

            END;
            $$ LANGUAGE plpgsql;
            CREATE OR REPLACE TRIGGER trg_update_mfeatures_on_tg
            AFTER INSERT OR UPDATE OR DELETE
            ON temporal_geometries
            FOR EACH ROW
            EXECUTE FUNCTION update_mfeatures_on_tg();
                    """)
    #If temporal_properties nad temporal_values tables not exists, then create
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS temporal_properties (
            id SERIAL PRIMARY KEY,
            feature_id TEXT REFERENCES moving_features(id) ON DELETE CASCADE,
            property_name TEXT NOT NULL,
            property_type TEXT NOT NULL,
            form TEXT,
            description TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    #not temporal type because i can't fix the column to treal timage etc since we can have diff types of properties
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS temporal_values (
            id SERIAL PRIMARY KEY,
            property_id INTEGER REFERENCES temporal_properties(id) ON DELETE CASCADE,
            datetimes TIMESTAMPTZ[] NOT NULL,
            values JSONB NOT NULL,
            interpolation TEXT DEFAULT 'Linear',
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    
    connection.commit()
#___________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________


    srid = 4326 #world,
    if crs and isinstance(crs, dict):
        props = crs.get("properties", "")

        # CRS can be either:
        # - "urn:ogc:def:crs:EPSG::25832"
        # - {"name": "EPSG::25832"}
        if isinstance(props, dict):
            props = props.get("name", "")

        import re
        match = re.search(r'(\d+)', str(props))

        if match:
            srid = int(match.group(1))
    # INSERT INTO moving_features :temporal_geometries:Insert feature into moving_features table


    columns = ["id", "collection_id", "type", "properties"]
    values = [feat_id, collection_id, "Feature", json.dumps(properties)]

    if crs is not None:
        columns.append("crs")
        values.append(json.dumps(crs))

    if trs is not None:
        columns.append("trs")
        values.append(json.dumps(trs))

    query = sql.SQL("""
        INSERT INTO moving_features ({fields})
        VALUES ({placeholders})
        ON CONFLICT (id) DO NOTHING
        RETURNING id
    """).format(
        fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
        placeholders=sql.SQL(", ").join(sql.Placeholder() * len(values))
    )
    cursor.execute(query, values)
    inserted = cursor.fetchone()

    # INSERT INTO temporal_geometries: If the create feature has a temporal_geom, then add to temporal_geometries table    
    #RE CHECK OGC (must the uiser always provide the temporal geom unsure 40 percent)
    base = temporal_geometry.get("base",None)
    if inserted and tgeom_mfjson:

        geometry_type = "MovingPoint"  # Default 
        if temporal_geometry and isinstance(temporal_geometry, dict):
            geometry_type = temporal_geometry.get("type", "MovingPoint") #get geom_type of not default MovingPoint
            interpolation = temporal_geometry.get("interpolation", "Linear")
           
            orientations = temporal_geometry.get("orientations",None)
        else:
            interpolation = "Linear"
        columns = ["feature_id","collection_id","geometry_type","geometry",
            "trajectory",
            "interpolation"
        ]
        values = [feat_id,collection_id,geometry_type,tgeom_mfjson,srid,tgeom_mfjson,
            srid,
            interpolation
        ]
        placeholders = [
            "%s", "%s", "%s",
            "trajectory(SETSRID(tgeompointFromMFJSON(%s), %s))",
            "SETSRID(tgeompointFromMFJSON(%s), %s)",
            "%s"
        ]
        if base is not None:
            columns.append("base")
            placeholders.append("%s")
            values.append(base)

        if orientations is not None:
            columns.append("orientations")
            placeholders.append("%s")
            values.append(orientations)


        query = f"""
                INSERT INTO temporal_geometries 
                ({", ".join(columns)})
                VALUES (
                    {", ".join(placeholders)}
                )
                RETURNING ID
            """
        cursor.execute(query, values)
        inserted = cursor.fetchone()
    if inserted:
        # print(f"Inserted feature {feat_id}")
        return feat_id
    else:
        return None