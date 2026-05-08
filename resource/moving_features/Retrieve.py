# REQ 14: /req/movingfeatures/features-get
# REQ16: /req/movingfeatures/features-get-success
# REQ50-52: Common params (limit, bbox, datetime) 
# REQ 12-13: subTrajectory param(ogc)
# REQ23-24: leaf param
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from utils import send_json_response
from pymeos import TGeomPoint
from urllib.parse import urlparse, parse_qs
import json
from datetime import datetime
from dateutil import parser, tz
from resource.moving_feature.feature_helper import build_feature_from_row, build_feature_collection_response
import traceback

def get_collection_items(self, collection_id, connection, cursor):
    try:
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)
        
        # Parse parameters
        try:
            limit = min(int(query_params.get('limit', [10])[0]), 10000)  # Req 50: max 10000
        except ValueError:
            return self.handle_error(400, "Invalid limit parameter")

        bbox = query_params.get('bbox', [None])[0]
        datetime_param = query_params.get('datetime', [None])[0]
        subTrajectory = query_params.get('subTrajectory', ['false'])[0].lower() == "true" #only if true
        leaf = query_params.get('leaf', ['false'])[0].lower() == "true"

        #\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\_ query params combos validation(Reqs 13E and 24E)\\\\\\\\\\\\\\\\\\\\\\\
        if subTrajectory and leaf:
            return self.handle_error(400, "subTrajectory cannot be used with leaf")
        
        # Parse bbox (Req 51)
        x1 = y1 = x2 = y2 = None
        if bbox:
            try:
                coords = [float(c) for c in bbox.split(',')]
                if len(coords) != 4:
                    return self.handle_error(400, "Invalid bbox format - must be 4 numbers")
                x1, y1, x2, y2 = coords
            except Exception:
                return self.handle_error(400, "Invalid bbox coordinates")

        # Parse datetime (Req52)
        dt1 = dt2 = None
        if datetime_param:
            if "/" in datetime_param:
                dt1, dt2 = datetime_param.split("/")
                # subTrajectory== true==> bounder interval (Req 12C)
                if subTrajectory and (not dt1 or not dt2):
                    return self.handle_error(400, "subTrajectory requires a bounded datetime interval")
            else:
                dt1 = datetime_param  # instant
                if subTrajectory:
                    return self.handle_error(400, "subTrajectory requires a bounded interval, not a single instant")
        
        # subTrajectory without datetime interval code 400
        if subTrajectory and not (dt1 and dt2):
            return self.handle_error(400, "subTrajectory requires a datetime interval")
        #\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\_
        #\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\_check collection by path param exists\\\\\\\\\\\\\\\\\__\\\\\\\\\\\\
        cursor.execute(
            "SELECT id FROM collections WHERE id = %s",
            (collection_id,)
        )
        if cursor.fetchone() is None:
            return self.handle_error(404, f"Collection '{collection_id}' not found")
        #\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\__\\\\\\\\\\\\\\\\\\\__

        # \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\Build query
        #check left join on mf fetures , is it possible that a mffeature exists without a single tempgeo , same for post f  clean
        # ST_AsGeoJSON(ST_Transform(ST_SetSRID(trajectory(tg.trajectory), 25832), 4326)) as geometry, replaced by geometry
        query = """
            SELECT 
                mf.id,
                mf.type,
                ST_AsGeoJSON(tg.geometry) as geometry,
                mf.properties,
                mf.bbox::text,
                mf.time::text,
                mf.crs,
                mf.trs,
                tg.id as geom_id,
                tg.geometry_type,
                tg.trajectory,
                tg.interpolation,
                tg.base,
                COUNT(*) OVER() AS total_count
            FROM moving_features mf
            LEFT JOIN temporal_geometries tg ON mf.id = tg.feature_id
            WHERE mf.collection_id = %s
        """
        params = [collection_id]

        # + spatial filter using MobilityDB if bbox param
        if x1 is not None:
            query += f" AND tg.trajectory && setsrid(stbox 'STBOX X(({x1},{y1}),({x2},{y2}))',srid(tg.trajectory))"
        
        # + temporal filter using MobilityDB if datetime pram
        # check mobility functions **> not urgent clean 
        if dt1 and dt2: 
            query += f" AND tg.trajectory && tstzspan '[{dt1}, {dt2}]'"
        elif dt1:     # instant 
            # intesects same time instant (atPeriod didn't workd , re check slightly urgent)
            query += f" AND tg.trajectory && tstzspan '[{dt1}, {dt1}]'"

        query += f" ORDER BY mf.created_at LIMIT %s"
        params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()
        #\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
        ##############################################prepare response################################################
        if not rows:
            return send_json_response(self, 200, json.dumps({
                "type": "FeatureCollection",
                "features": [],
                "timeStamp": datetime.utcnow().isoformat() + "Z",
                "numberMatched": 0,
                "numberReturned": 0,
                "links": [{"href": self.path, "rel": "self", "type": "application/json"}]
            }))

        total_count = rows[0][13]  
        features_dict = {}

        # Group by feature since a feature can have 1..* geometries
        for row in rows:
            feature_id = row[0]
            if feature_id not in features_dict:
                # build base feat minus temp data (only moving_features cols)
                feature_row = (
                    row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]
                )
                feature = build_feature_from_row(feature_row, collection_id, include_temporal=False)
                feature["temporalGeometry"] = []  
                feature["geometry"] = [] 
                
                features_dict[feature_id] = feature

            
            # if temporal geometry **
            if row[8]:  # geom_id
                tgeom = row[10]  # trajectory
                geometry = row[2]
                if tgeom:
                    # Pymeos object t->MF-JSON
                    mf_json = json.loads(tgeom.as_mfjson())
                    sub_geometry = None  
#-------------------------------------------------------check------------------------------------------------
                    # Apply subTrajectory if included req13
                    if subTrajectory and dt1 and dt2:
                        try:

                            # Use a parameterized query for safety and clarity
                            interval = f'[{dt1}, {dt2}]'
                            sub_query = """
                            WITH time_window AS (
                                    SELECT tstzspan %s AS tw
                                )
                            SELECT
                                    asMFJSON(atTime(tg.trajectory, tw)) as tempotime,
                                    ST_AsGeoJSON(trajectory(atTime(tg.trajectory, tw))) AS geom
                                FROM temporal_geometries tg 
                                CROSS JOIN time_window
                                WHERE id = %s
                            """
                            cursor.execute(sub_query, (interval,row[8]))
                            sub_row = cursor.fetchone()
                            if sub_row and sub_row[0]:
                                mf_json = json.loads(sub_row[0])
                                sub_geometry = sub_row[1]  
                        except Exception as e:
                            print(f"Error in subTrajectory: {e}")
                            connection.rollback()
            
#----------------------------------------------------------------------------------------------------------------------------------
                    # Apply leaf param if included (Req 24)
                    if leaf and "datetimes" in mf_json:
                        #Take only last point
                        mf_json["coordinates"] = [mf_json["coordinates"][-1]]
                        mf_json["datetimes"] = [mf_json["datetimes"][-1]]
                        mf_json["interpolation"] = "Discrete"  # Req 24C         
                    # Add to temporalGeometry array
                    temporal_geom = {
                        "id": row[8],
                        "type": row[9] or "MovingPoint",
                        "datetimes": mf_json.get("datetimes", []),
                        "coordinates": mf_json.get("coordinates", []),
                        "base": row[12]
                    }

                    # Use the interpolation from mf_json if leaf was applied
                    if leaf and "interpolation" in mf_json:#DISCRETE
                        temporal_geom["interpolation"] = mf_json["interpolation"]  
                    else:
                        temporal_geom["interpolation"] = row[11] or "Linear"

                    features_dict[feature_id]["temporalGeometry"].append(temporal_geom)

                    if sub_geometry and isinstance(sub_geometry, str):
                        try:
                            geometry = json.loads(sub_geometry)
                        except:
                            geometry = json.loads(geometry)
                    else:
                        geometry = json.loads(geometry)
                    
                    features_dict[feature_id]["geometry"].append(geometry)
        features = list(features_dict.values())


        base_url = f"http://{self.server.server_name}:{self.server.server_port}"
        response = build_feature_collection_response(
            features=features,
            total_count=total_count,
            limit=limit,
            base_url=base_url,
            path=f"/collections/{collection_id}/items",
            bbox=bbox,
            datetime_param=datetime_param
        )

        send_json_response(self, 200, json.dumps(response))

    except Exception as e:
        connection.rollback()  
        #clean::
        print("ERROR:", e)
        traceback.print_exc()
        self.handle_error(500, f"Internal server error: {str(e)}")