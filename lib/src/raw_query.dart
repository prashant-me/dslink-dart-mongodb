import 'dart:convert';
import 'package:dslink/dslink.dart';
import 'package:mongo_dart/mongo_dart.dart';

class EvaluateRawQueryNode extends SimpleNode {
  final Db db;

  static Map<String, dynamic> definition = {
    r"$name": "Evaluate Raw Query",
    r"$is": "evaluateRawQuery",
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

  EvaluateRawQueryNode(String path, this.db) : super(path);

  @override
  onInvoke(Map<String, dynamic> params) async {
    var collectionName = params['collectionName'];
    var query = params['code'];
    var limit = params['limit'] ?? 0;
    var skip = params['skip'] ?? 0;

    var collection = db.collection(collectionName);

    SelectorBuilder sb = new SelectorBuilder();
    sb.raw(JSON.decode(query));
    var c = new Cursor(db, collection, sb);

    var res;
    try {
      c.limit = limit;
      c.skip = skip;
      res = await c.stream.toList();
      for (var m in res) {
        m['date'] = (m['date'] as DateTime).toIso8601String();
      }

      var json = JSON.encode(res);
      return {'success': true, 'message': json};
    } catch (e) {
      print(e);
      return {'success': false, 'message': e.toString()};
    }
  }
}

