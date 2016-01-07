import "dart:async";
import "dart:io";

import "package:dslink/dslink.dart";
import "package:dslink/historian.dart";
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
    return adapter;
  }
}

class MongoDatabaseHistorianAdapter extends HistorianDatabaseAdapter {
  Db db;

  StreamController<ValueEntry> entryStream =
    new StreamController<ValueEntry>.broadcast();

  @override
  Stream<ValuePair> fetchHistory(String group, String path, TimeRange range) async* {
    var timeMap = {};

    if (range.start != null) {
      timeMap[r"$gte"] = range.start;
    }

    if (range.end != null) {
      timeMap[r"$lte"] = range.end;
    }

    await for (Map map in db.collection("${group}:${path}").find({
      "timestamp":  timeMap
    })) {
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
    entries.forEach(entryStream.add);

    for (var entry in entries) {
      await db.collection("${entry.group}:${entry.path}");
    }
  }

  @override
  Future close() async {
    await entryStream.close();
    await db.close();
  }

  @override
  addWatchPathExtensions(WatchPathNode node) async {
    link.requester.list(node.valuePath).listen((RequesterListUpdate update) async {
      if (update.node.attributes[r"@geo"] != false && update.changes.contains("@geo")) {
        var val = update.node.attributes["@geo"];
        await db.ensureIndex("${node.group.name}:${node.valuePath}", keys: {
          "value": val is String ? val : "2dsphere"
        });
      }
    });

    link.addNode("${node.path}/geoquerynear", {
      r"$is": "geoquerynear",
      "?watch": node,
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
  provider.addProfile("geoquerynear", (String path) => new GeoqueryNearNode(path));
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
        r"$geoNear": {
          "near": {
            "type": "Point",
            "coordinates": [longitude, latitude]
          },
          "maxDistance": maxDistance,
          "minDistance": minDistance
        }
      }
    }).map((Map map) {
      return [map["timestamp"].toString(), map["value"]];
    });
  }
}

SimpleNodeProvider get provider => link.provider;
