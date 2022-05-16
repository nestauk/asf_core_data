"""Script that contains utils for installer proximity indicator."""

###############################################################
import pyproj
from shapely.geometry import Polygon, Point
import numpy as np
from geopy.distance import distance
from collections import Counter
from functools import partial
from shapely.ops import transform
import statistics
###############################################################

def calculate_point_distance(long, lat, central_point):
    """Calculates distance between installation central point and a point.
    
    Inputs:
        long (np.ndarray): longitude of point 
        lat (np.ndarray): latitude of point 
        central_point (np.ndarry): latitude and longitude of point

    Outputs:
        Distance between installation central point and a point

    """
    return distance(tuple(central_point), (long, lat))


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
        if len(installation_info) > 1:
            central_point = cleaned_installer_companies[["longitude", "latitude"]].median()
        elif company_name in set(cleaned_installer_companies.installer_name):
            central_point = cleaned_installer_companies[
                cleaned_installer_companies["installer_name"] == company_name
            ][["longitude", "latitude"]].values[0]
        else:
            print(f"no installations to generate indicator for {company_name}.")

        dists = installation_info.apply(
            lambda x: calculate_point_distance(
                x["longitude"], x["latitude"], central_point
            ),
            axis=1,
        ).tolist()

        if len(dists) > 1:
            dist_25, dist_50, dist_75 = np.percentile(dists, [25, 50, 75], axis=0)
            company_install_shapes[company_name] = {
                buffer_name: generate_geodesic_point_buffer(
                    central_point[1], central_point[0], dist.km
                )
                for buffer_name, dist in [
                    ("min_buffer", min(dists)),
                    ("25_buffer", dist_25),
                    ("50_buffer", dist_50),
                    ("75_buffer", dist_75),
                    ("max_buffer", max(dists)),
                ]
            }
        else:
            comp_buffer = generate_geodesic_point_buffer(
                central_point[1], central_point[0], dists[0].km
            )
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
        for buffer_name, shape in company_shapes.items():
            if shape.contains(epc_geo):
                buffer_distribution.append(buffer_name)
                break
    return Counter(buffer_distribution)


def calculate_weighted_installer_proximity(buffer_count: Counter) -> int:
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
    min_score = (1 * buffer_count["min_buffer"]) / buffer_count["max_buffer"]
    twentyfive_score = (1 - 0.25)* buffer_count["25_buffer"]/ buffer_count["max_buffer"]
    fifty_score = (1 - 0.5) * buffer_count["50_buffer"]/ buffer_count["max_buffer"]
    max_score = (1 - 0.75) * buffer_count["75_buffer"]/ buffer_count["max_buffer"]

    assert (
        0 <= statistics.mean([min_score, twentyfive_score, fifty_score, max_score]) <= 1
    ), f"weighted installer closeness score above 1 or below 0, got: {weighted_installer_closeness_score}"

    return statistics.mean([min_score, twentyfive_score, fifty_score, max_score])