import 'dart:convert';
import 'package:dslink/dslink.dart';
import 'package:dslink/historian.dart';
import 'package:mongo_dart/mongo_dart.dart';

class EvaluateRawStreamingQueryNode extends SimpleNode {
  DatabaseNode node;
  final Db db;

  static Map<String, dynamic> definition = {
    r"$name": "Evaluate Raw Streaming Query",
    r"$is": "evaluateRawStreamingQuery",
    r"$result": "stream",
    r"$invokable": "write",
    r"$params": [
      {
        "name": "collectionName",
        "type": "string",
      },
      {
        "name": "code",
        "type": "string",
        "editor": 'textarea',
        "description": "Raw query code",
        "placeholder": "db.name"
      },
      {
        "name": "limit",
        "type": "number",
        "default": 0,
        "description": "max number of items in the query (0 equals no limit)",
      },
      {
        "name": "skip",
        "type": "number",
        "default": 0,
        "description": "Amount of results to skip for the query",
      },
    ],
    r'$columns': [
      {"name": "json", "type": "string"}
    ],
  };

  EvaluateRawStreamingQueryNode(String path, this.db) : super(path);

  @override
  onCreated() {
    node = link.getNode(new Path(path).parentPath);
  }

  @override
  onInvoke(Map<String, dynamic> params) async* {
    var collectionName = params['collectionName'];
    var query = params['code'];
    var limit = params['limit'] ?? 0;
    var skip = params['skip'] ?? 0;

    var collection = db.collection(collectionName);

    SelectorBuilder sb = new SelectorBuilder();
    sb.raw(JSON.decode(query));
    var c = new Cursor(db, collection, sb);

    try {
      c.limit = limit;
      c.skip = skip;
      await for (var m in c.stream) {
        for (var k in m.keys) {
          if (m[k] is DateTime) {
            m[k] = m[k].toIso8601String();
          }
        }
        var encode = JSON.encode(m);
        yield [
          [encode]
        ];
      }
    } catch (e) {
      rethrow;
    }
  }
}
