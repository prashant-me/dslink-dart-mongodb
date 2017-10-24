import 'package:dslink/dslink.dart';
import 'package:dslink/historian.dart';
import 'package:dslink_mongodb/mongo_historian.dart';
import 'package:mongo_dart/mongo_dart.dart';

class GeoqueryNearNode extends SimpleNode {
  WatchPathNode node;

  GeoqueryNearNode(String path) : super(path);

  @override
  void load(Map m) {
    super.load(m);
  }

  @override
  onInvoke(Map<String, dynamic> params) async {
    TimeRange range = parseTimeRange(params["time"]);
    num latitude = params["latitude"];
    num longitude = params["longitude"];
    num minDistance = params["minimumDistance"];
    num maxDistance = params["maximumDistance"];

    if (minDistance == null) {
      minDistance = 0;
    }

    if (maxDistance == null) {
      maxDistance = 40;
    }
    MongoDatabaseHistorianAdapter rdb = node.group.db.database;
    Db db = rdb.db;

    var ands = [];

    if (range.start != null) {
      ands.add({
        "timestamp": {r"$gte": range.start}
      });
    }

    if (range.end != null) {
      ands.add({
        "timestamp": {r"$lte": range.end}
      });
    }

    ands.add({
      "value": {
        r"$near": {
          r"$geometry": {
            "type": "Point",
            "coordinates": [longitude, latitude]
          },
          r"$maxDistance": maxDistance,
          r"$minDistance": minDistance
        }
      }
    });

    var query = {
      r"$query": {r"$and": ands}
    };

    return await db
        .collection("${node.group.displayName}:${node.valuePath}")
        .find(query)
        .map((Map map) {
      if (map[r"$err"] != null) {
        throw new Exception("MongoDB Error: ${map}");
      }

      var val = map["value"];

      if (val is Map && val["type"] == "Point") {
        var coords = val["coordinates"];
        val = {"lat": coords[1], "lng": coords[0]};
      }

      return [
        [map["timestamp"].toString(), val]
      ];
    });
  }
}
