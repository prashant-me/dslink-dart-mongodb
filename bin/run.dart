import "dart:async";
import "dart:convert";

import "package:mongo_dart/mongo_dart.dart";
import "package:dslink/dslink.dart";
import "package:dslink/utils.dart" show logger;
import "package:dslink/nodes.dart";

import "package:logging/logging.dart";

LinkProvider link;

main(List<String> args) async {
  link = new LinkProvider(
      args,
      "MongoDB-",
      defaultNodes: {
        "Create_Connection": {
          r"$name": "Create Connection",
          r"$is": "createConnection",
          r"$invokable": "write",
          r"$params": [
            {
              "name": "name",
              "type": "string",
              "description": "Connection Name",
              "placeholder": "mongo"
            },
            {
              "name": "url",
              "type": "string",
              "description": "Connection Url",
              "placeholder": "mongodb://user:password@localhost:8080/mydb"
            }
          ]
        }
      },
      profiles: {
        "connection": (String path) => new ConnectionNode(path),
        "deleteConnection": (String path) => new DeleteConnectionNode(path),
        "editConnection": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
          var name = params["name"];
          var oldName = path.split("/")[1];
          ConnectionNode conn = link["/${oldName}"];
          if (name != null && name != oldName) {
            if ((link.provider as SimpleNodeProvider).nodes.containsKey("/${name}")) {
              return {
                "success": false,
                "message": "Connection '${name}' already exists."
              };
            } else {
              var n = conn.serialize(false);
              link.removeNode("/${oldName}");
              link.addNode("/${name}", n);
              (link.provider as SimpleNodeProvider).nodes.remove("/${oldName}");
              conn = link["/${name}"];
            }
          }

          link.save();

          var url = params["url"];
          var oldUrl = conn.configs[r"$$mongo_url"];

          if (url != null && oldUrl != url) {
            conn.configs[r"$$mongo_url"] = url;
            try {
              await conn.setup();
            } catch (e) {
              return {
                "success": false,
                "message": "Failed to connect to database: ${e}"
              };
            }
          }

          link.save();

          return {
            "success": true,
            "message": "Success!"
          };
        }, link.provider),
        "createConnection": (String path) => new CreateConnectionNode(path),
        "listCollections": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
          var db = dbForPath(path);
          return (await db.getCollectionNames()).map((x) => [x]);
        }, link.provider),
        "insertIntoCollection": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
          var collection = params["collection"];
          var object = params["object"];

          if (object is String) {
            try {
              object = JSON.decode(object);
            } catch (e) {
              object = {};
            }
          } else if (object is! Map) {
            object = {};
          }

          var db = dbForPath(path);
          return await db.collection(collection).insert(object);
        }, link.provider),
        "getCollection": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) {
          var r = new AsyncTableResult();
          new Future(() async {
            var db = dbForPath(path);
            var limit = params["limit"];
            var sortByField = params["sortByField"];
            var sortDirection = params["sortDirection"];
            var fields = params["fields"];
            var explain = params["explain"];

            if (fields == null) {
              fields = [];
            }

            if (explain == null) {
              explain = false;
            }

            if (fields is String) {
              fields = fields.split(",");
            }

            var descending = false;

            if (sortDirection == "descending") {
              descending = true;
            }

            var builder = new SelectorBuilder();
            if (limit != null) {
              builder.limit(limit);
            }

            if (fields != null && fields.isNotEmpty) {
              builder.fields(fields);
            }

            if (explain) {
              builder.explain();
            }

            if (sortByField != null) {
              if (sortByField == "") {
                sortByField = null;
              } else {
                builder.sortBy(sortByField, descending: descending);
              }
            }

            db.collection(params["collection"]).find(builder).toList().then((data) {
              var keys = data.map((x) => x.keys).expand((it) => it).toSet();

              r.columns = keys.map((it) => {
                "name": it,
                "type": "dynamic"
              }).toList();
              var output = data.map((x) => x.values.map((it) => it is ObjectId ? (it as ObjectId).toHexString() : it).toList()).toList();
              r.update(output);
              r.close();
            });
          });
          return r;
        }),
        "removeObject": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
          var db = dbForPath(path);
          await db.collection(params["collection"]).remove(new SelectorBuilder().eq("_id", params["id"]));
          return {};
        }, link.provider),
        "evaluateJavaScript": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
          var code = params["code"];
          var db = dbForPath(path);
          var command = new DbCommand(db, DbCommand.SYSTEM_COMMAND_COLLECTION, MongoQueryMessage.OPTS_NONE, 0, -1, {
            r"$eval": code
          }, null);

          var result = await db.executeDbCommand(command);

          if (result["ok"] != 1.0) {
            return [];
          }

          result = result["retval"];

          var out = [];

          for (var key in result.keys) {
            var value = result[key];

            if (value is List || value is Map) {
              value = JSON.encode(value);
            }

            out.add([key, value]);
          }

          return out;
        }, link.provider),
        "dropCollection": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
          var db = dbForPath(path);
          await db.collection(params["collection"]).drop();
          return {};
        }, link.provider)
      },
      autoInitialize: false,
      encodePrettyJson: true
  );

  link.init();
  link.connect();
  link.save();

  Logger.root.level = logger.level;
  for (var log in Logger.root.children.values) {
    log.level = logger.level;
  }
}

class CreateConnectionNode extends SimpleNode {
  CreateConnectionNode(String path) : super(path, link.provider);

  @override
  onInvoke(Map<String, dynamic> params) async {
    link.addNode("/${params["name"]}", {
      r"$is": "connection",
      r"$$mongo_url": params["url"]
    });

    link.save();

    return {};
  }
}

class ConnectionNode extends SimpleNode {
  ConnectionNode(String path) : super(path, link.provider);

  @override
  void onCreated() {
    setup();
  }

  setup() async {
    var name = new Path(path).name;

    if (dbs.containsKey(name)) {
      await dbs[name].close();
      dbs.remove(name);
    }

    var url = configs[r"$$mongo_url"];
    Db db = new Db(url);

    await db.open();

    for (var log in Logger.root.children.values) {
      log.level = logger.level;
    }

    dbs[name] = db;

    var x = {
      "Insert_Object": {
        r"$name": "Insert Object",
        r"$is": "insertIntoCollection",
        r"$invokable": "write",
        r"$params": [
          {
            "name": "collection",
            "type": "string",
            "description": "Database Collection",
            "placeholder": "people"
          },
          {
            "name": "object",
            "type": "map",
            "description": "Object to Insert",
            "placeholder": '{"name": "Bob"}'
          }
        ]
      },
      "Remove_Object": {
        r"$name": "Remove Object",
        r"$is": "removeObject",
        r"$invokable": "write",
        r"$params": [
          {
            "name": "collection",
            "type": "string",
            "description": "Database Collection",
            "placeholder": "people"
          },
          {
            "name": "id",
            "type": "string",
            "description": "Object Id (_id column)",
            "placeholder": "507f191e810c19729de860ea"
          }
        ]
      },
      "Evaluate_JavaScript": {
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
      },
      "Get_Collection": {
        r"$name": "Find Objects",
        r"$is": "getCollection",
        r"$invokable": "write",
        r"$result": "table",
        r"$params": [
          {
            "name": "collection",
            "type": "string",
            "description": "Database Collection",
            "placeholder": "people"
          },
          {
            "name": "limit",
            "type": "number",
            "description": "Max Number of Results",
            "placeholder": "40"
          },
          {
            "name": "fields",
            "type": "array",
            "description": "Specific Fields to Access"
          },
          {
            "name": "sortByField",
            "type": "string",
            "description": "Field to Sort By",
            "placeholder": "timestamp"
          },
          {
            "name": "sortDirection",
            "type": "enum[ascending,descending]",
            "description": "Sort Direction"
          },
          {
            "name": "explain",
            "type": "bool",
            "description": "Explains what happened"
          }
        ],
        r"$columns": []
      },
      "Drop_Collection": {
        r"$name": "Drop Collection",
        r"$is": "dropCollection",
        r"$invokable": "write",
        r"$result": "values",
        r"$params": [
          {
            "name": "collection",
            "type": "string",
            "description": "Database Collection",
            "placeholder": "people"
          }
        ]
      },
      "List_Collections": {
        r"$name": "List Collections",
        r"$is": "listCollections",
        r"$invokable": "write",
        r"$result": "table",
        r"$columns": [
          {
            "name": "name",
            "type": "string"
          }
        ]
      },
      "Edit_Connection": {
        r"$name": "Edit Connection",
        r"$is": "editConnection",
        r"$invokable": "write",
        r"$result": "values",
        r"$params": [
          {
            "name": "name",
            "type": "string",
            "default": name
          },
          {
            "name": "url",
            "type": "string",
            "default": url
          }
        ],
        r"$columns": [
          {
            "name": "success",
            "type": "bool"
          },
          {
            "name": "message",
            "type": "string"
          }
        ]
      },
      "Delete_Connection": {
        r"$name": "Delete Connection",
        r"$is": "deleteConnection",
        r"$invokable": "write",
        r"$result": "values",
        r"$params": [],
        r"$columns": []
      }
    };

    for (var a in x.keys) {
      link.removeNode("${path}/${a}");
      link.addNode("${path}/${a}", x[a]);
    }
  }
}

class DeleteConnectionNode extends SimpleNode {
  DeleteConnectionNode(String path) : super(path, link.provider);

  @override
  onInvoke(Map<String, dynamic> params) {
    link.removeNode(new Path(path).parentPath);
    link.save();
    return {};
  }
}

Map<String, Db> dbs = {};

Db dbForPath(String path) => dbs[path.split("/").take(2).last];
