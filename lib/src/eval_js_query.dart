import 'dart:convert';
import 'package:dslink/dslink.dart';
import 'package:mongo_dart/mongo_dart.dart';

class EvaluateJavaScriptDatabaseNode extends SimpleNode {
  final Db db;

  static Map<String, dynamic> definition = {
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
      {"name": "key", "type": "string"},
      {"name": "value", "type": "dynamic"}
    ]
  };

  EvaluateJavaScriptDatabaseNode(String path, this.db) : super(path);

  @override
  onInvoke(Map<String, dynamic> params) async {
    var command = new DbCommand(
        db,
        DbCommand.SYSTEM_COMMAND_COLLECTION,
        MongoQueryMessage.OPTS_NO_CURSOR_TIMEOUT,
        0,
        -1,
        {r"$eval": params["code"], r"$nolock": true},
        null);

    var result = await db.executeDbCommand(command);
    if (result["ok"] == 1.0) {
      result = result["retval"];
    }

    var out = [];

    if (result is BsonObject) {
      result = result.value;
    }

    if (result is! Map && result is! List) {
      result = [result];
    }

    if (result is List) {
      if (result.every((x) => x is Map)) {
        var keys = new Set<String>();
        for (Map row in result) {
          keys.addAll(row.keys);
        }

        var out = [];

        for (Map row in result) {
          var n = [];
          for (String key in keys) {
            n.add(row[key]);
          }
          out.add(n);
        }

        var table = new Table(
            keys.map((t) => new TableColumn(t, "dynamic")).toList(), out);

        return table;
      } else {
        var m = {};
        var i = 0;
        result.forEach((n) => m[i++] = n);
        result = m;
      }
    }

    for (var key in result.keys) {
      var value = result[key];

      if (value is List || value is Map) {
        value = const JsonEncoder().convert(encodeMongoObject(value));
      }

      out.add([key, value]);
    }

    return out;
  }
}

dynamic encodeMongoObject(input) {
  if (input is ObjectId) {
    return input.toHexString();
  } else if (input is DateTime) {
    return input.toIso8601String();
  } else if (input is BsonBinary) {
    input.makeByteList();
    return input.byteArray;
  } else if (input is Map &&
      input.keys.length == 2 &&
      input["type"] == "Point" &&
      input.keys.contains("coordinates") &&
      input["coordinates"] is List &&
      input["coordinates"].length == 2) {
    var list = input["coordinates"];
    return {"lat": list[1], "lng": list[0]};
  } else if (input is Map) {
    for (var key in input.keys.toList()) {
      input[key] = encodeMongoObject(input[key]);
    }
  } else if (input is List) {
    for (var i = 0; i < input.length; i++) {
      input[i] = encodeMongoObject(input[i]);
    }
  } else if (input is BsonCode) {
    return input.data;
  }

  return input;
}

