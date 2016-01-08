import "dart:async";
import "dart:convert";
import "dart:io";

import "package:dslink/dslink.dart";
import "package:dslink/historian.dart";
import "package:dslink/nodes.dart";

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
    Stream<Map> results = db.collection("${group}:${path}").find(
      where
        .gte("timestamp", range.start)
        .lte("timestamp", range.end)
    );

    await for (Map map in results) {
      var timestamp = map["timestamp"];
      if (timestamp is String) {
        timestamp = DateTime.parse(timestamp);
      }
      var pair = new ValuePair(timestamp.toString(), map["value"]);
      yield pair;
    }

    if (range.end == null) {
      await for (ValueEntry entry in entryStream.stream) {
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
      await db.collection("${entry.group}:${entry.path}").insert({
        "timestamp": entry.time,
        "value": geopoints.contains(entry.path) ? {
          "type": "Point",
          "coordinates": entry.value
        } : entry.value
      });
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
      if (update.node.attributes[r"@geo"] != false && update.changes.contains("@geo")) {
        var val = update.node.attributes["@geo"];
        await db.ensureIndex("${node.group.name}:${node.valuePath}", keys: {
          "value": val is String ? val : "2dsphere"
        });
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
      r"$result": "table",
      r"$invokable": "read"
    });

    geoquery_Near.node = node;
    geoquery_Near.serializable = false;
    provider.setNode(geoquery_Near.path, geoquery_Near);
  }
}

main(List<String> args) async {
  args = new List<String>.from(args);
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

  if (argString.contains("--old, true") || useOldCode) {
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

    var timeMap = {};

    if (range.start != null) {
      timeMap[r"$gte"] = range.start;
    }

    if (range.end != null) {
      timeMap[r"$lte"] = range.end;
    }

    return await db.collection("${node.group.name}:${node.valuePath}").find({
      "timestamp": timeMap,
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
    }).map((Map map) {
      if (map[r"$err"] != null) {
        throw new Exception("MongoDB Error: ${map}");
      }
      return [map["timestamp"].toString(), map["value"]];
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
