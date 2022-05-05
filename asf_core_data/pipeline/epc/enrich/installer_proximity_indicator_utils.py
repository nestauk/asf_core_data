"""Script that contains utils for installre proximity indicator."""

###############################################################
import pyproj
from shapely.geometry import Polygon, Point
import numpy as np
from geopy.distance import distance
from collections import Counter
from functools import partial
from shapely.ops import transform

###############################################################


def calculate_point_midpoint(p1: np.ndarray, p2: np.ndarray) -> tuple:
    """Calculates the midpoint between two latitude and longitude points.

    Inputs:
        p1 (np.ndarray): latitude and longitude point 1
        p2 (np.ndarray): latitude and longitude point 2

    Outputs:
        Midpoint point of p1 and p2
    """
    return (p1[0] + p2[0]) / 2, (p1[1] + p2[1]) / 2


def generate_geodesic_point_buffer(lat: int, lon: int, km: int) -> Polygon:
    # https://gis.stackexchange.com/questions/289044/creating-buffer-circle-x-kilometers-from-point-using-python
    """Generates a buffer circle X kilometers from point.

    Inputs:
        lat (int): latitude of point
        lon (int): longitude of point
        km (int): distance from point in km

    Outputs:
        Buffer polygon
    """
    proj_wgs84 = pyproj.Proj("+proj=longlat +datum=WGS84")
    # Azimuthal equidistant projection
    aeqd_proj = "+proj=aeqd +lat_0={lat} +lon_0={lon} +x_0=0 +y_0=0"
    project = partial(
        pyproj.transform, pyproj.Proj(aeqd_proj.format(lat=lat, lon=lon)), proj_wgs84
    )
    buf = Point(0, 0).buffer(km * 1000)  # distance in metres
    return Polygon(transform(project, buf).exterior.coords[:])


def generate_distance_spread_buffers(
    clean_installations, cleaned_installer_companies
) -> dict:
    """Generates buffers based on distance distribution per HP company. If more than
    one installation, central_point is installations centroid. If one installation,
    central_point is installer company location.

    Inputs:
        clean_installations (DataFrame): DataFrame of heat pump installations.
        cleaned_installer_companies (DataFrame): DataFrame of heat pump installer companies.

    Outputs:
        company_install_shapes (Dict): Dictionary where key is company name and values
        are buffer polygons based on distance spread.
    """
    company_install_shapes = dict()
    for company_name, installation_info in clean_installations.groupby(
        "installer_name"
    ):
        latlongs = installation_info[["longitude", "latitude"]].values
        if len(latlongs) > 1:
            central_point = calculate_point_midpoint(latlongs[0], latlongs[1])
        elif company_name in list(set(cleaned_installer_companies.installer_name)):
            central_point = tuple(
                cleaned_installer_companies[
                    cleaned_installer_companies["installer_name"] == company_name
                ][["longitude", "latitude"]].values[0]
            )

        dists = [
            distance(central_point, install_latlong) for install_latlong in latlongs
        ]

        if len(dists) > 1:
            dist_25, dist_50, dist_75 = np.percentile(dists, [25, 50, 75], axis=0)
            comp_buffers = [
                generate_geodesic_point_buffer(
                    central_point[1], central_point[0], dist.km
                )
                for dist in (min(dists), dist_25, dist_50, dist_75, max(dists))
            ]
            company_install_shapes[company_name] = {
                "min_buffer": comp_buffers[0],
                "25_buffer": comp_buffers[1],
                "50_buffer": comp_buffers[2],
                "75_buffer": comp_buffers[3],
                "max_buffer": comp_buffers[4],
            }
        else:
            comp_buffer = generate_geodesic_point_buffer(
                central_point[1], central_point[0], dists[0].km
            )
            # tbd if this should be considered min or max
            company_install_shapes[company_name] = {"min_buffer": comp_buffer}

    return company_install_shapes


def calculate_buffer_distribution(company_install_shapes, epc_geo):
    """Calculates the count of company distance spread buffers per epc
     building.

    Inputs:
        epc_geo (np.ndarray): The latitude and longitude of an epc_building.

    Outputs:
        Counter(buffer_distribution) (dict): company distance spread buffer counts
    """
    buffer_distribution = []
    for company, company_shapes in company_install_shapes.items():
        in_buffer = [shape.contains(epc_geo) for shape in company_shapes.values()]
        if any(in_buffer) is not False:
            smallest_buffer = list(company_shapes.keys())[in_buffer.index(True)]
            buffer_distribution.append(smallest_buffer)
    return Counter(buffer_distribution)


def calculate_weighted_installer_proximity(buffer_distb: Counter) -> int:
    """Calculates weighted installer closeness score based
    on distribution of companies in the min, 25th, 50th,
    75th and max buffer distances. Score is between 0 and 1, where
    a 1 is a perfect "closeness" score.

    Inputs:
        buffer_dist (Counter): The count of companies in the min,
        25th, 50th, 75th and max buffer distance in relation to the
        EPC home.

    Outputs:
        weighted_installer_closeness_score (int): weighted installer closeness score.
    """
    weighted_installer_closeness_score = 0
    for buffer_dist, buffer_count in buffer_distb.items():
        if "min_" in buffer_dist:
            min_score = (1 * buffer_count) / buffer_distb["max_buffer"]
            weighted_installer_closeness_score += min_score
        elif "25_" in buffer_dist:
            twentyfive_score = (1 - 0.25) * buffer_count / buffer_distb["max_buffer"]
            weighted_installer_closeness_score += twentyfive_score
        elif "50_" in buffer_dist:
            fifty_score = (1 - 0.5) * buffer_count / buffer_distb["max_buffer"]
            weighted_installer_closeness_score += fifty_score
        elif "75_" in buffer_dist:
            seventyfive_score = (1 - 0.75) * buffer_count / buffer_distb["max_buffer"]
            weighted_installer_closeness_score += seventyfive_score

    assert (
        0 <= weighted_installer_closeness_score <= 1
    ), f"weighted installer closeness score above 1 or below 0, got: {weighted_installer_closeness_score}"

    return weighted_installer_closeness_score
