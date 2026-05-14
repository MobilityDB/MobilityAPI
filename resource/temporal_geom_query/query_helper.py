from datetime import datetime
from enum import Enum
import json
#req 33 , response obj for temporal geometries distance, velocity and acceleration queries
def build_query_response(values, unit, query_type, base_url, path):
    description = QueryDescription[query_type].value
    return {
        "name": query_type,
        "type": "TReal",
        "form": unit, 
        "description": description,
        "values": values,  # [{time, value}{}]
        "links": [
            {
                "href": f"{base_url}{path}",
                "rel": "self",
                "type": "application/json"
            }
        ],
        # "timeStamp": datetime.utcnow().isoformat() + "Z"
    }

class QueryDescription(Enum):
    distance="a graph of the time to distance function as a form of the TemporalProperty.",
    velocity = "a graph of the time to velocity function as a form of the TemporalProperty.",
    acceleration = "a graph of the time to acceleration function as a form of the TemporalProperty."
