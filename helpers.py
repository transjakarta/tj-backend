import utm, copy


def get_utm_crs(lat: float, lon: float) -> dict:
    zone = utm.from_latlon(lat, lon)[2]
    result = f"EPSG:326{zone:02d}" if lat >= 0 else f"EPSG:327{zone:02d}"
    return result


def drop_feature_ids(collection: dict) -> dict:
    new_features = []
    for f in collection["features"]:
        new_f = copy.deepcopy(f)
        if "id" in new_f:
            del new_f["id"]
        new_features.append(new_f)

    collection["features"] = new_features
    return collection
