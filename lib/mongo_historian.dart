import 'dart:async';
import 'package:dslink/dslink.dart';
import 'package:dslink/historian.dart';
import 'package:dslink/nodes.dart';
import 'package:dslink/utils.dart';
import 'package:mongo_dart/mongo_dart.dart';
import 'package:stack_trace/stack_trace.dart';
import 'actions.dart';
import 'src/geo_utils.dart';

class MongoHistorianAdapter extends HistorianAdapter {
  SimpleNodeProvider provider; // Initialize after link is initialized

  MongoHistorianAdapter();

  @override
  List<Map<String, dynamic>> getCreateDatabaseParameters() => [
        {
          "name": "Url",
          "type": "string",
          "description": "Connection Url",
          "placeholder": "mongodb://user:password@localhost/mydb",
          "default": "mongodb://127.0.0.1/dsa"
        }
      ];

  @override
  Future<HistorianDatabaseAdapter> getDatabase(Map config) async {
    String url = config["Url"];
    if (url == null) {
      url = config["url"];
    }

    MongoDatabaseHistorianAdapter adapter;
    await Chain.capture(() async {
      var db = new Db(url);
      adapter = new MongoDatabaseHistorianAdapter(db, provider);
      await db.open();
    }, when: const bool.fromEnvironment("dsa.mode.debug", defaultValue: false));

    String name = config["Name"];

    DatabaseNode dbn = link.getNode("/${NodeNamer.createName(name)}");

    var evalNode =
        new EvaluateJavaScriptDatabaseNode("${dbn.path}/eval", adapter.db);
    var evalQueryNode =
        new EvaluateRawQueryNode("${dbn.path}/evalQuery", adapter.db);
    var evalStreamingQueryNode = new EvaluateRawStreamingQueryNode(
        "${dbn.path}/evalStreamingQuery", adapter.db);
    var dbRowsWrittenNode = new SimpleNode("${dbn.path}/_rows_written");

    evalQueryNode.load(EvaluateRawStreamingQueryNode.definition);
    evalStreamingQueryNode.load(EvaluateRawStreamingQueryNode.definition);
    evalNode.load(EvaluateJavaScriptDatabaseNode.definition);

    dbRowsWrittenNode
        .load({r"$name": "Rows Written", r"$type": "number", "@unit": "rows"});

    adapter.dbRowsWrittenNode = dbRowsWrittenNode;

    provider.setNode(evalNode.path, evalNode);
    provider.setNode(evalQueryNode.path, evalQueryNode);
    provider.setNode(evalStreamingQueryNode.path, evalStreamingQueryNode);
    provider.setNode(dbRowsWrittenNode.path, dbRowsWrittenNode);

    return adapter;
  }
}

class MongoDatabaseHistorianAdapter extends HistorianDatabaseAdapter {
  final Db db;
  final SimpleNodeProvider provider;
  Disposable dbStatsTimer;

  StreamController<ValueEntry> entryStream =
      new StreamController<ValueEntry>.broadcast();

  SimpleNode dbRowsWrittenNode;

  bool get connected => db.state == State.OPEN;

  MongoDatabaseHistorianAdapter(this.db, this.provider) {
    _setup();
  }

  void _setup() {
    dbStatsTimer = Scheduler.safeEvery(Interval.FIVE_SECONDS, () async {
      if (db != null && connected) {
        var cmd = await DbCommand.createQueryDbCommand(db, {"dbStats": 1});
        Map stats;
        try {
          stats = await db.executeDbCommand(cmd);
        } catch (e, s) {
          logger.warning('Error trying to execute DB command', e, s);
          return;
        }
        num objectCount = stats["objects"];
        if (dbRowsWrittenNode != null) {
          dbRowsWrittenNode.updateValue(objectCount.toInt());
        }
      }
    });
  }

  @override
  Stream<ValuePair> fetchHistory(
      String group, String path, TimeRange range) async* {
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

    var query = {r"$and": ands};

    Stream<Map> results = db.collection("${group}:${path}").find(query);

    await for (Map map in results) {
      var timestamp = map["timestamp"];
      if (timestamp is String) {
        timestamp = DateTime.parse(timestamp);
      }

      if (timestamp is DateTime) {
        timestamp = "${timestamp.toIso8601String()}${ValueUpdate.TIME_ZONE}";
      }

      var val = map["value"];
      val = encodeMongoObject(val);
      var pair = new ValuePair(timestamp.toString(), val);
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
    var firstPair = first == null
        ? null
        : new ValuePair(
            first["timestamp"].toString(), encodeMongoObject(first["value"]));

    var lastPair = last == null
        ? null
        : new ValuePair(
            last["timestamp"].toString(), encodeMongoObject(last["value"]));
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
        await db.collection(name).remove({"timestamp": timeMap});
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

    await db.collection("${group}:${path}").remove({"timestamp": timeMap});
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

          value = {"type": "Point", "coordinates": data};
        } else {
          logger.warning(
              "Value ${value} is not valid for geospatial point in group"
              " ${entry.group} with path ${entry.path}");
        }
      }

      try {
        await db
            .collection("${entry.group}:${entry.path}")
            .insert({"timestamp": entry.time, "value": value});
      } catch (e, stack) {
        logger.warning(
            "Failed to insert value ${value} from group ${entry
                .group} and path ${entry.path}",
            e,
            stack);
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
    link.requester
        .list(node.valuePath)
        .listen((RequesterListUpdate update) async {
      var ghr =
          "${link.remotePath}/${node.group.db.name}/${node.group.name}/${node
          .name}/getHistory";
      var bgh = "${link.remotePath}/${node.group.name}/${node.name}/getHistory";
      if (!update.node.attributes.containsKey("@@getHistory") ||
          update.node.attributes["@@getHistory"] == bgh) {
        link.requester.set("${node.valuePath}/@@getHistory", {
          "@": "merge",
          "type": "paths",
          "val": [ghr]
        });
      }

      if (update.node.attributes[r"@geo"] != false &&
          update.changes.contains("@geo")) {
        var val = update.node.attributes["@geo"];
        await db.ensureIndex("${node.group.name}:${node.valuePath}",
            keys: {"value": val is String ? val : "2dsphere"},
            sparse: true,
            background: true);
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
        {"name": "time", "type": "string", "editor": "daterange"},
        {"name": "latitude", "type": "number"},
        {"name": "longitude", "type": "number"},
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
        {"name": "timestamp", "type": "string"},
        {"name": "location", "type": "dynamic"}
      ],
      r"$result": "stream",
      r"$invokable": "read"
    });

    geoquery_Near.node = node;
    geoquery_Near.serializable = false;
    provider.setNode(geoquery_Near.path, geoquery_Near);
  }
}
