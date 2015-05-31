import "dart:convert";

import "package:mongo_dart/mongo_dart.dart";
import "package:dslink/dslink.dart";
import "package:dslink/nodes.dart";

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
              "type": "string"
            },
            {
              "name": "url",
              "type": "string"
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
        }),
        "createConnection": (String path) => new CreateConnectionNode(path),
        "listCollections": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
          var db = dbForPath(path);
          return (await db.getCollectionNames()).map((x) => [x]);
        }),
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
        }),
        "getCollection": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) {
          var r = new AsyncTableResult();
          var db = dbForPath(path);
          db.collection(params["collection"]).find().toList().then((data) {
            var keys = data.map((x) => x.keys).expand((it) => it).toSet();

            r.columns = keys.map((it) => {
              "name": it,
              "type": "dynamic"
            }).toList();
            r.update(data.map((x) => x.values.map((it) => it is ObjectId ? (it as ObjectId).toHexString() : it).toList()).toList());
            r.close();
          });
          return r;
        }),
        "removeObject": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
          var db = dbForPath(path);
          await db.collection(params["collection"]).remove(new SelectorBuilder().eq("_id", params["id"]));
          return {};
        }),
        "dropCollection": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
          var db = dbForPath(path);
          await db.collection(params["collection"]).drop();
          return {};
        })
      },
      autoInitialize: false,
      encodePrettyJson: true
  );

  link.init();
  link.connect();
  link.save();
}

class CreateConnectionNode extends SimpleNode {
  CreateConnectionNode(String path) : super(path);

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
  ConnectionNode(String path) : super(path);

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

    dbs[name] = db;

    var x = {
      "Insert_Object": {
        r"$name": "Insert Object",
        r"$is": "insertIntoCollection",
        r"$invokable": "write",
        r"$params": [
          {
            "name": "collection",
            "type": "string"
          },
          {
            "name": "object",
            "type": "map"
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
            "type": "string"
          },
          {
            "name": "id",
            "type": "string"
          }
        ]
      },
      "Get_Collection": {
        r"$name": "Get Collection",
        r"$is": "getCollection",
        r"$invokable": "write",
        r"$result": "table",
        r"$params": [
          {
            "name": "collection",
            "type": "string"
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
            "type": "string"
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
  DeleteConnectionNode(String path) : super(path);

  @override
  onInvoke(Map<String, dynamic> params) {
    link.removeNode(new Path(path).parentPath);
    link.save();
    return {};
  }
}

Map<String, Db> dbs = {};

Db dbForPath(String path) => dbs[path.split("/").take(2).last];
