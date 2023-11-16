import math

def equirectangular_approx_distance(coord1, coord2):
    """
    Calculate the approximate distance in kilometers between two coordinates using the Equirectangular approximation.

    Args:
    coord1 (tuple): A tuple (latitude, longitude) for the first location.
    coord2 (tuple): A tuple (latitude, longitude) for the second location.

    Returns:
    float: Distance in kilometers.
    """

    R = 6371  # Radius of the Earth in kilometers
    lat1, lon1 = map(math.radians, coord1)
    lat2, lon2 = map(math.radians, coord2)

    x = (lon2 - lon1) * math.cos((lat1 + lat2) / 2)
    y = lat2 - lat1

    distance = math.sqrt(x*x + y*y) * R
    dict_distance = {"meters":distance*1000, "km":distance}
    return dict_distance