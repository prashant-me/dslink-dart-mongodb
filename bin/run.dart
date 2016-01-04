import "dart:async";

import "package:dslink_mongodb/historian.dart";
import "package:mongo_dart/mongo_dart.dart";

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

    await for (Map map in db.collection(group).find({
      "path": path,
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
    selector.eq("path", path);
    selector.sortBy("timestamp");
    var first = await db.collection(group).findOne(selector);
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

    await db.collection(group).remove({
      "timestamp": timeMap
    });
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

    await db.collection(group).remove({
      "path": path,
      "timestamp": timeMap
    });
  }

  @override
  Future store(List<ValueEntry> entries) async {
    entries.forEach(entryStream.add);

    Map<String, List<ValueEntry>> map = {};
    for (var entry in entries) {
      if (map[entry.group] is! Map) {
        map[entry.group] = [];
      }
      map[entry.group].add(entry);
    }

    for (String group in map.keys) {
      Iterable<Map> e = map[group].map((entry) {
        return {
          "path": entry.path,
          "timestamp": entry.time,
          "value": entry.value
        };
      }).toList();

      await db.collection(group).insertAll(e);
    }
  }

  @override
  Future close() async {
    await entryStream.close();
    await db.close();
  }
}

main(List<String> args) async {
  var adapter = new MongoHistorianAdapter();
  return historianMain(args, "MongoDB", adapter);
}
