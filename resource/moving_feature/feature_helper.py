import json 
from datetime import datetime

import re
from datetime import datetime
def build_feature_from_row(row, collection_id, include_temporal=True, single=False):
    cleaned = re.findall(r"[-+]?\d*\.\d+|\d+", str(row[4])) if not single else re.findall(r"[-+]?\d*\.\d+|\d+", str(row[3])) 

    # convert to float
    bbox= list(map(float, cleaned))
    geometry_json = row[2]
    if geometry_json and isinstance(geometry_json, str):
        try:
            geometry = json.loads(geometry_json)
        except:
            geometry = None
    else:
        geometry = None

        #Parse time_range if exists (col indx 5)
    time = row[5] if not single else row[4]
    if time:
        if time and time.startswith('[') and time.endswith(']'):
            times = time[1:-1].split(',')
            time = [t.strip() for t in times]
    feature = {
        "type": "Feature",
        "id": str(row[0]),
        # "geometry": geometry,
        "properties": row[3] if not single else row[2] or {},
        "bbox": bbox[:4],
        "time":time,
        "crs": row[6] if not single else row[5] ,
        "trs": row[7] if not single else row[6],
        "links": [
            {
                "href": f"/collections/{collection_id}/items/{row[0]}",
                "rel": "self",
                "type": "application/json"
            },
            {
                "href": f"/collections/{collection_id}/items/{row[0]}/tgsequence",
                "rel": "http://www.opengis.net/def/rel/ogc/1.0/temporal-geometry",
                "type": "application/json"
            },
            {
                "href": f"/collections/{collection_id}/items/{row[0]}/tproperties",
                "rel": "http://www.opengis.net/def/rel/ogc/1.0/temporal-properties",
                "type": "application/json"
            }
        ]
    }
    

    
    # Parse temporal geometries if included col 8 ***************Only for Retrieve single moving feature
    if include_temporal and len(row) > 7 and row[7]:
        temporal_geometries = []
        tg_list = row[7]
        for tg in tg_list:
            if tg.get('trajectory'):
                # trajectory bjson to dict
                traj = json.loads(tg['trajectory'])
                temporal_geometries.append({
                    "id": tg['id'],
                    "type": tg['type'],
                    "datetimes": traj.get('datetimes', []),
                    "coordinates": traj.get('coordinates', []),
                    "interpolation": tg['interpolation'],
                    "base": tg['base']
                })
        feature["temporalGeometry"] = temporal_geometries
    # *********************************************************************
    return feature

#with pagination next links (class dgrm ogc)
def build_feature_collection_response(features, total_count, limit, base_url, path, 
                                      bbox=None, datetime_param=None):
#deprecated utcnow clean 
    response = {
        "type": "FeatureCollection",
        "features": features,
        "timeStamp": datetime.utcnow().isoformat() + "Z",
        "numberMatched": total_count,
        "numberReturned": len(features),
        "links": [
            {
                "href": f"{base_url}{path}",
                "rel": "self",
                "type": "application/json"
            }
        ]
    }

    # Next page >>
    if total_count > limit:
        next_params = f"limit={limit}&offset={limit}"
        if bbox:
            next_params += f"&bbox={bbox}"
        if datetime_param:
            next_params += f"&datetime={datetime_param}"
        #next link:
        response["links"].append({
            "href": f"{base_url}{path}?{next_params}",
            "rel": "next",
            "type": "application/json"
        })

    return response





