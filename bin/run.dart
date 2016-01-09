import "dart:async";
import "dart:convert";
import "dart:io";

import "package:dslink/dslink.dart";
import "package:dslink/historian.dart";
import "package:dslink/nodes.dart";
import "package:dslink/utils.dart";

import "package:mongo_dart/mongo_dart.dart";

import "run_old.dart" as Old;

class MongoHistorianAdapter extends HistorianAdapter {
  @override
  List<Map<String, dynamic>> getCreateDatabaseParameters() => [
    {
      "name": "url",
      "type": "string",
      "description": "Connection Url",
      "placeholder": "mongodb://user:password@localhost:8080/mydb"
    }
  ];

  @override
  Future<HistorianDatabaseAdapter> getDatabase(Map config) async {
    var db = new Db(config["url"]);
    var adapter = new MongoDatabaseHistorianAdapter();
    adapter.db = db;
    await db.open();

    String name = config["Name"];

    DatabaseNode dbn = link.getNode("/${NodeNamer.createName(name)}");

    var evalNode = new EvaluateJavaScriptDatabaseNode("${dbn.path}/eval");

    evalNode.load({
      r"$name": "Evaluate JavaScript",
      r"$is": "evaluateJavaScript",
      r"$invokable": "write",
      r"$result": "table",
      r"$params": [
        {
          "name": "code",
          "type": "string",
          "editor": 'textarea',
          "description": "JavaScript Code",
          "placeholder": "db.name"
        }
      ],
      r"$columns": [
        {
          "name": "key",
          "type": "string"
        },
        {
          "name": "value",
          "type": "dynamic"
        }
      ]
    });

    provider.setNode(evalNode.path, evalNode);

    return adapter;
  }
}

class MongoDatabaseHistorianAdapter extends HistorianDatabaseAdapter {
  Db db;

  StreamController<ValueEntry> entryStream =
    new StreamController<ValueEntry>.broadcast();

  @override
  Stream<ValuePair> fetchHistory(String group, String path, TimeRange range) async* {
    var ands = [];

    if (range.start != null) {
      ands.add({
        "timestamp": {
          r"$gte": range.start
        }
      });
    }

    if (range.end != null) {
      ands.add({
        "timestamp": {
          r"$lte": range.end
        }
      });
    }

    var query = {
      r"$and": ands
    };

    Stream<Map> results = db.collection("${group}:${path}").find(query);

    await for (Map map in results) {
      var timestamp = map["timestamp"];
      if (timestamp is String) {
        timestamp = DateTime.parse(timestamp);
      }
      var pair = new ValuePair(timestamp.toString(), map["value"]);
      yield pair;
    }

    if (range.end == null) {
      await for (ValueEntry entry in entryStream.stream.where((x) {
        return x.group == group && x.path == x.path;
      })) {
        yield entry.asPair();
      }
    }
  }

  @override
  Future<HistorySummary> getSummary(String group, String path) async {
    var selector = new SelectorBuilder();
    selector.sortBy("timestamp");
    var first = await db.collection("${group}:${path}").findOne(selector);
    selector = new SelectorBuilder();
    selector.eq("path", path);
    selector.sortBy("timestamp", descending: true);
    var last = await db.collection(group).findOne(selector);
    var firstPair = first == null ? null : new ValuePair(
      first["timestamp"].toString(),
      first["value"]
    );
    var lastPair = last == null ? null : new ValuePair(
      last["timestamp"].toString(),
      last["value"]
    );
    var summary = new HistorySummary(firstPair, lastPair);
    return summary;
  }

  @override
  Future purgeGroup(String group, TimeRange range) async {
    var timeMap = {};

    if (range.start != null) {
      timeMap[r"$gte"] = range.start;
    }

    if (range.end != null) {
      timeMap[r"$lte"] = range.end;
    }

    List<String> names = await db.getCollectionNames();
    for (String name in names) {
      if (name.startsWith("${group}:")) {
        await db.collection(name).remove({
          "timestamp": timeMap
        });
      }
    }
  }

  @override
  Future purgePath(String group, String path, TimeRange range) async {
    var timeMap = {};

    if (range.start != null) {
      timeMap[r"$gte"] = range.start;
    }

    if (range.end != null) {
      timeMap[r"$lte"] = range.end;
    }

    await db.collection("${group}:${path}").remove({
      "timestamp": timeMap
    });
  }

  @override
  Future store(List<ValueEntry> entries) async {
    if (!entryStream.isClosed) {
      entries.forEach(entryStream.add);
    }

    for (var entry in entries) {
      var value = entry.value;

      if (geopoints.contains(entry.path)) {
        if (isValidGeoValue(value)) {
          var data = value;

          if (data is Map) {
            data = [data["lng"], data["lat"]];
          }

          value = {
            "type": "Point",
            "coordinates": data
          };
        } else {
          logger.warning(
            "Value ${value} is not valid for geospatial point in group"
              " ${entry.group} with path ${entry.path}"
          );
        }
      }

      try {
        await db.collection("${entry.group}:${entry.path}").insert({
          "timestamp": entry.time,
          "value": value
        });
      } catch (e, stack) {
        logger.warning(
          "Failed to insert value ${value} from group ${entry.group} and path ${entry.path}",
          e,
          stack
        );
      }
    }
  }

  @override
  Future close() async {
    await entryStream.close();
    await db.close();
  }

  Set<String> geopoints = new Set<String>();

  @override
  addWatchPathExtensions(WatchPathNode node) async {
    link.requester.list(node.valuePath).listen((RequesterListUpdate update) async {
      var ghr = "${link.remotePath}/${node.group.db.name}/${node.group.name}/${node.name}/getHistory";
      var bgh = "${link.remotePath}/${node.group.name}/${node.name}/getHistory";
      if (!update.node.attributes.containsKey("@@getHistory") ||
        update.node.attributes["@@getHistory"] == bgh) {
        link.requester.set("${node.valuePath}/@@getHistory", {
          "@": "merge",
          "type": "paths",
          "val": [
            ghr
          ]
        });
      }

      if (update.node.attributes[r"@geo"] != false && update.changes.contains("@geo")) {
        var val = update.node.attributes["@geo"];
        await db.ensureIndex("${node.group.name}:${node.valuePath}", keys: {
          "value": val is String ? val : "2dsphere"
        }, sparse: true, background: true);
      }

      if (update.node.attributes["@geo"] != null &&
        update.node.attributes["@geo"] != false) {
        geopoints.add(node.valuePath);
      } else {
        geopoints.remove(node.valuePath);
      }
    });

    var geoquery_Near = new GeoqueryNearNode("${node.path}/geoquerynear");
    geoquery_Near.load({
      r"$name": "Geographical Query - Near",
      r"$params": [
        {
          "name": "time",
          "type": "string",
          "editor": "daterange"
        },
        {
          "name": "latitude",
          "type": "number"
        },
        {
          "name": "longitude",
          "type": "number"
        },
        {
          "name": "maximumDistance",
          "type": "number",
          "description": "Maximum Distance in Meters"
        },
        {
          "name": "minimumDistance",
          "type": "number",
          "description": "Minimum Distance in Meters"
        }
      ],
      r"$columns": [
        {
          "name": "timestamp",
          "type": "string"
        },
        {
          "name": "location",
          "type": "dynamic"
        }
      ],
      r"$result": "stream",
      r"$invokable": "read"
    });

    geoquery_Near.node = node;
    geoquery_Near.serializable = false;
    provider.setNode(geoquery_Near.path, geoquery_Near);
  }
}

main(List<String> args) async {
  args = new List<String>.from(args);
  args = args.map((m) => m.toString()).toList(); // Weird issues with boolean.
  String argString = args.toString();

  if (args.contains("--old")) {
    try {
      var idx = args.indexOf("--old");
      args.removeAt(idx);
      args.removeAt(idx);
    } catch (e) {
      print(e);
    }
  }

  if (args.contains("--old=true") || args.contains("--old=false")) {
    args.removeWhere((x) => x.startsWith("--old="));
  }

  var file = new File("nodes.json");
  var useOldCode = false;
  if (await file.exists()) {
    var content = await file.readAsString();
    if (content.contains("Create_Connection") &&
      content.contains("Connection Name")) {
      print(
        "== NOTICE: Going into Compatibility Mode."
          " Delete the nodes.json file to use"
          " the new MongoDB Historian. =="
      );
      useOldCode = true;
    }
  }

  if (argString.contains("--old, true") || argString.contains("--old=true") || useOldCode) {
    return Old.main(args);
  }

  var adapter = new MongoHistorianAdapter();

  new Future.delayed(const Duration(seconds: 5), () async {
    if (link != null) {
      link.save();
    }
  });

  var result = await historianMain(args, "MongoDB", adapter);
  return result;
}

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
        "timestamp": {
          r"$gte": range.start
        }
      });
    }

    if (range.end != null) {
      ands.add({
        "timestamp": {
          r"$lte": range.end
        }
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
      r"$query": {
        r"$and": ands
      }
    };

    return await db.collection("${node.group.displayName}:${node.valuePath}").find(query).map((Map map) {
      if (map[r"$err"] != null) {
        throw new Exception("MongoDB Error: ${map}");
      }

      var val = map["value"];

      return [[map["timestamp"].toString(), val]];
    });
  }
}

class ColumnsMarker {
  List columns;
  List rows;
}

class EvaluateJavaScriptDatabaseNode extends SimpleNode {
  DatabaseNode node;

  EvaluateJavaScriptDatabaseNode(String path) : super(path);

  @override
  onCreated() {
    node = link.getNode(new Path(path).parentPath);
  }

  @override
  onInvoke(Map<String, dynamic> params) async {
    MongoDatabaseHistorianAdapter d = node.database;
    var command = new DbCommand(d.db, DbCommand.SYSTEM_COMMAND_COLLECTION, MongoQueryMessage.OPTS_NONE, 0, -1, {
      r"$eval": params["code"]
    }, null);

    var result = await d.db.executeDbCommand(command);
    if (result["ok"] != 1.0) {
      return [];
    }

    result = result["retval"];

    var out = [];

    if (result is BsonObject) {
      result = result.value;
    }

    if (result is! Map && result is! List) {
      result = [result];
    }

    if (result is List) {
      var m = {};
      var i = 0;
      result.forEach((n) => m[i++] = n);
      result = m;
    }

    for (var key in result.keys) {
      var value = result[key];

      if (value is List || value is Map) {
        value = const JsonEncoder().convert(value);
      }

      out.add([key, value]);
    }

    return out;
  }
}

SimpleNodeProvider get provider => link.provider;

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
