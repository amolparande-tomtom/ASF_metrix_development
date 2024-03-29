from math import sin, cos, sqrt, atan2, radians


def truncate(n: float, decimals: int = 0) -> int:
    """Simple function which truncates an incoming float value (n) and returns its integer value
    based on its multiplier value, later divided by the same multiplier value.
    :param n: Input numerical value, float type.
    :type n: flaot
    :param decimals: Input integer which defines the number of decimal point to return, defaults to 0
    :type decimals: int, optional
    :return:Output integer value after the truncate process of the input value (n).
    :rtype: int
    """

    multiplier = 10 ** decimals
    return int(n * multiplier) / multiplier


def haversine_distance(lt1: float,
                       ln1: float,
                       lt2: float,
                       ln2: float) -> float:
    """Function which calculates the distance in meters between two XY coodinates. The function requires
    the latitude and longitude for each point. At the end of the function, we truncate the output distance
    value with a maximun of 4 decimal points.
    :param lt1: Latitude value of first coordinate point.
    :type lt1: float
    :param ln1: Longitude value of first coordinate point.
    :type ln1: float
    :param lt2: Latitude value of second coordinate point.
    :type lt2: float
    :param ln2: Longitude value of second coordinate point.
    :type ln2: float
    :return: Distance between point in meters.
    :rtype: float
    """

    R = 6373.0  # approximate radius of earth in km
    lat1 = radians(lt1)
    lon1 = radians(ln1)
    lat2 = radians(lt2)
    lon2 = radians(ln2)
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = truncate(R * c, 4) * 1000
    return distance


# distance = [haversine_distance(nearest_point.y,nearest_point.x,point.y,point.x)if point.wkt != 'POINT (nan nan)'else 1e7
# for nearest_point, point in zip(nearest_points, points)]


lt1 = -23.5649653
ln1 = -46.5183107
lt2 = -23.564558
ln2 = -46.51807


print(haversine_distance(lt1,ln1,lt2,ln2))