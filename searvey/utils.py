from .custom_types import ScalarOrArray

# https://gis.stackexchange.com/questions/201789/verifying-formula-that-will-convert-longitude-0-360-to-180-to-180
# lon1 is the longitude varying from -180 to 180 or 180W-180E
# lon3 is the longitude variable from 0 to 360 (all positive)


def lon1_to_lon3(lon1: ScalarOrArray) -> ScalarOrArray:
    return lon1 % 360


def lon3_to_lon1(lon3: ScalarOrArray) -> ScalarOrArray:
    return ((lon3 + 180) % 360) - 180
