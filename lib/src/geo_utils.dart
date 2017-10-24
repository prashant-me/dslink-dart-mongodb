bool isValidGeoValue(value) {
  if (value is! List && value is! Map) {
    return false;
  }

  if (value is List) {
    if (value.length != 2) {
      return false;
    }

    var a = value[0];
    var b = value[1];

    if (a is! num || b is! num) {
      return false;
    }

    return isValidLngLat(a, b);
  } else if (value is Map) {
    var a = value["lng"];
    var b = value["lat"];

    if (a is! num || b is! num) {
      return false;
    }

    return isValidLngLat(a, b);
  } else {
    return false;
  }
}

bool isValidLngLat(num lng, num lat) {
  return lat >= -90.0 && lat <= 90.0 && lng >= -180.0 && lng <= 180.0;
}
