import requests
import pandas as pd
from dataclasses import dataclass
from typing import Tuple
from geopy.distance import geodesic

from helpers import _load_states

GOOGLE_DISTANCE_API_KEY = "ENV_TODO"

@dataclass
class GeoPair:
    origin: Tuple[float, float]
    destination: Tuple[float, float]

    @property
    def distance_geodesic(self):
        return int(geodesic(self.origin, self.destination).miles)

def _call_google_distance_api(geo_pair: GeoPair) -> dict:
    """
        Requires Google Distance Matrix API Key
        cost $5 per 1k: https://developers.google.com/maps/documentation/distance-matrix/usage-and-billing
        with $200 per month included (40k)
    """
    def _tuple_to_string(x: tuple) -> str: return f"{x[0]},{x[1]}"
    params = {
        'origins': _tuple_to_string(geo_pair.origin),
        'destinations': _tuple_to_string(geo_pair.destination),
        'units': 'imperial',
        'key': GOOGLE_DISTANCE_API_KEY
    }
    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(response.status_code, response.text)
    return response.json()


def _get_destination_state(distance_matrix_response):
    try:
        destination = distance_matrix_response['destination_addresses'][0]
        destination_state = destination.split(",")[2].strip().split(" ")[0]
        return destination_state
    except:
        pass


def _get_origin_state(distance_matrix_response):
    try: 
        origin = distance_matrix_response['origin_addresses'][0]
        origin_state = origin.split(",")[2].strip().split(" ")[0]
        return origin_state
    except:
        pass


def _fetch_google_distance_matrix(geo_pair: GeoPair) -> dict:
    
    def _get_google_drive_distance(google_distance_response: dict) -> int:
        """
            get driving miles
        """
        try:
            meters = google_distance_response['rows'][0]['elements'][0]['distance']['value']
            miles = meters / 1609
            return int(miles)
        except Exception as e:
            # print(e, google_distance_response)
            pass

    def _get_google_drive_duration(google_distance_response: dict) -> int:
        """
            get driving minute
        """
        try:
            seconds = google_distance_response['rows'][0]['elements'][0]['duration']['value']
            minutes = seconds / 60
            return int(minutes)
        except Exception as e:
            # print(e, google_distance_response)
            pass

    def _get_distance_geodesic(a: tuple, b: tuple) -> int:
        return int(geodesic(a, b).miles)
        
    data = _call_google_distance_api(geo_pair)
    try:
        obj = dict(
            origin_lat = geo_pair.origin[0],
            origin_lng = geo_pair.origin[1],
            origin_state = _get_origin_state(data),
            destination_lat = geo_pair.destination[0],
            destination_lng = geo_pair.destination[1],
            destination_state = _get_destination_state(data),
            _distance_matrix_response = data,
            geodesic_miles = geo_pair.distance_geodesic,
            drive_miles = _get_google_drive_distance(data),
            drive_duration = _get_google_drive_duration(data),
        )
        return obj
    except Exception as e:
        print(e, geo_pair)


def load_k_closest_clinic_distances() -> pd.DataFrame:

    closest_clinics = pd.read_feather("data/google_distance_matrix.feather")
    closest_clinics['destination_state'] = closest_clinics['_distance_matrix_response'].apply(_get_destination_state) # add upstream
    closest_clinics['origin_state'] = closest_clinics['_distance_matrix_response'].apply(_get_origin_state) # add upstream
    columns = [
        'origin_lat', 'origin_lng', 'origin_state',
        'dest_lat', 'dest_lng', 'destination_state',
        'geodesic_miles', 'drive_miles', 'drive_duration'
    ]
    closest_clinics = closest_clinics[columns]
    closest_clinics['_roundtrip_hours'] = closest_clinics['drive_duration'].apply(lambda x: x*2/60)

    states = _load_states()
    unprotected_states = states[states['_status'] == "not_protected"]
    closest_clinics = closest_clinics[closest_clinics['origin_state'].isin(unprotected_states['_state'])].reset_index(drop=True)

    return closest_clinics