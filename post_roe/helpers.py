import plotly.express as px
import pandas as pd
from geopy.distance import geodesic
from typing import List, Tuple

px.set_mapbox_access_token(open(".mapbox_token").read())

adi = pd.read_feather("data/adi_stats_zip5.feather")


def _load_states():
    """
    _state: str # 2 digit
    _census_total: int
    _status_wp: 4 status_buckets
    _status: two buckets: protected, not_protected
    """
    states = pd.read_csv("data/wp_roe_data.csv")  # washington post
    states = states[["States", "DATAWRAPPER"]]
    states = states.rename(columns={"DATAWRAPPER": "_status_wp", "States": "_state"})
    # adi = pd.read_feather("adi_stats_zip5.feather")
    state_populations = (
        adi.groupby(["_state"]).agg(census_total=("_census_total", "sum")).reset_index()
    )
    states = states.merge(state_populations)

    def _classify(status_wp: str) -> str:
        if status_wp == "Legal and likely to be protected":
            return "protected"
        else:
            return "not_protected"

    states["_status"] = states["_status_wp"].apply(_classify)
    return states.sort_values("_status", ascending=False).reset_index(drop=True)


def _load_zip3_census():
    # adi = pd.read_feather("adi_stats_zip5.feather")
    adi["_zip3"] = adi["_zip5"].apply(lambda x: f"{x[0:3]}**")
    adi_zip3 = (
        adi.groupby(["_state", "_zip3"])
        .agg(
            _lat=("_lat", "mean"),
            _lng=("_lng", "mean"),
            _census_total=("_census_total", "sum"),
            _adi_mean=("adi_median", "mean"),
        )
        .reset_index()
    )
    return adi_zip3


def _load_at_risk_zip3(adi_floor=50):
    """
    Loads all the zip3s: List[str]

    Loads all the zip3s with an adi above

    The ADI Index is a 1-100 measure, with
    100 being most-deprived/highest-challenge, and
    1 being the least challenged / highest resourced
    """

    zip3_census = _load_zip3_census()
    states = _load_states()

    unprotected_states = states[states["_status"] == "not_protected"]["_state"]
    unprotected = zip3_census[zip3_census["_state"].isin(unprotected_states)]
    unprotected = unprotected.drop_duplicates(
        subset=["_zip3"]
    )  # not sure where that duplicates coming in
    # print(len(unprotected), "zip3 unprotected")
    # unprotected
    at_risk = unprotected[unprotected["_adi_mean"] > adi_floor].reset_index(drop=True)
    print(
        f"Finding distances to 10 closest clinics for each of {len(at_risk)} zip3 origin locations with ADI above",
        adi_floor,
    )
    at_risk["_type"] = "at_risk"
    return at_risk


#
def _load_synthetic_clinics(n=1000) -> pd.DataFrame:
    """
    Stand in Sample Method until I get the actual clinic locations.
    This grabs 1k random zip codes in protected states.

    Why synthetic locations and what is a syntethic?
    Syntethic = statistically simular to the parent Population

    Why synthetic? Publishing lists widely has had to
    historically consider the increased safety risks to
    practitioners, and this project does not need to
    know the specific zip codes to understand a statistical
    model of the geometries around these geolocations.

    """

    states = _load_states()
    adi = pd.read_feather("data/adi_stats_zip5.feather")
    protected_states = states[states["_status"] == "protected"]["_state"]
    adi_protected = adi[adi["_state"].isin(protected_states)].reset_index(drop=True)
    clinics = adi_protected.sample(n).reset_index(drop=True)
    clinics["_clinic_geo"] = clinics.apply(lambda x: (x["_lat"], x["_lng"]), axis=1)
    # clinics = clinics.rename(columns={"_zip5":"_clinic_zip5"})
    clinics = clinics[["_state", "_zip5", "_clinic_geo", "_lat", "_lng"]].reset_index(
        drop=True
    )
    clinics["_type"] = "synthetic_clinic"
    return clinics


# def _get_zip5_geo(zip5: str) -> tuple:
#       adi = pd.read_feather("adi_stats_zip5.feather")
#     loc = adi[adi['_zip5']==zip5].to_dict(orient="records")[0]
#     return (loc['_lat'],loc['_lng'])

zip3_census = _load_zip3_census()


def _get_zip3_geo(zip3: str) -> tuple:
    loc = zip3_census[zip3_census["_zip3"] == zip3].to_dict(orient="records")[0]
    return (loc["_lat"], loc["_lng"])


# sbs = states.groupby(['_status']).agg(census_total=("census_total","sum")).reset_index()
# sbs['_pct'] = sbs['census_total'].apply(lambda x: x/sbs['census_total'].sum())
# sbs


def _get_google_time_distance(a: tuple, b: tuple) -> int:
    # hours or minutes, tbd
    return google_maps.directions(a, b)


def _get_distance_geodesic(a: tuple, b: tuple) -> int:
    return int(geodesic(a, b).miles)


def _k_closest_clinics(
    origin_zip3: str, clinics: pd.DataFrame, k: int = 10,
) -> pd.DataFrame:
    _origin_zip3_geo: tuple = _get_zip3_geo(origin_zip3)
    clinics = clinics.rename(columns={"_zip5": "_clinic_zip5"})  # consider normalizing
    clinics["_origin_zip3"] = origin_zip3
    # return clinics
    
    clinics["_distance_geodesic"] = clinics["_clinic_geo"].apply(
        lambda x: _get_distance_geodesic(x, _origin_zip3_geo)
    )  # gets distance to every target location in geodesic miles


    distance_sorted_clinics = clinics.sort_values("_distance_geodesic").reset_index(drop=True)
    closest_k_clinics = distance_sorted_clinics[0:k]  # choose the closest k
    return closest_k_clinics.drop(columns=["_clinic_geo"])

def build_distance_matrix(origin_zip3: List[str], clinics: pd.DataFrame, k:int =10) -> pd.DataFrame:
    #  this takes 2 min at 500 clinics / could consider optimizing
    """
        Iterates through each zip3 <> clinic permutation, 
        and selects the min(distance) k clinic locations
    """
    computation_count = len(origin_zip3) * len(clinics)
    print(f"computing: {computation_count} geodesic GeoPair distances, which should take {computation_count / 250000} min")
    dist_matrix = pd.concat(
        [_k_closest_clinics(zip3, clinics, k=k) for zip3 in origin_zip3]
    ).reset_index(drop=True)
    return dist_matrix

def draw_status_treemap() -> None:
    """
    Source: Washington Post
    """
    states = _load_states()
    protected_pct = (
        states[states["_status"] == "protected"]["census_total"].sum()
        / states["census_total"].sum()
    )
    unprotected_pct = (
        states[states["_status"] == "not_protected"]["census_total"].sum()
        / states["census_total"].sum()
    )
    px.treemap(
        states,
        path=["_status", "_status_wp", "_state"],
        color="_status_wp",
        values="census_total",
        height=600,
        title=f"Abortion Protections Status by State | Scaled by Total Population | Not::Protected {'{:.2f}'.format(unprotected_pct)}%::{'{:.2f}'.format(protected_pct)}% <br><sup>Source: Washington Post",
    ).show(renderer="notebook")


def draw_at_risk_vs_clinic_locations_map(states=[]) -> None:

    """
    Illustrative of Areas with Protections, vs those without
    Why syntethic clinics
    """
    at_risk = _load_at_risk_zip3()
    clinics = _load_synthetic_clinics(n=500)
    locations = pd.concat([clinics, at_risk]).fillna(at_risk["_census_total"].mean())
    px.scatter_mapbox(
        locations,
        lat="_lat",
        lon="_lng",
        size_max=15,
        mapbox_style="open-street-map",
        height=700,
        zoom=3,
        color="_status_wp",
        hover_data=["_state", "_zip3"],
        size="_census_total",
        title=f"At Risk Areas (3-Digit Zipcode) vs (Synthetic) Clinic Locations | Scaled by Population",
    ).show(renderer="notebook")
