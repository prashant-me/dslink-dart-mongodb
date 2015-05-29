import "dart:async";
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
            keys.remove("_id");
            r.columns = keys.map((it) => {
              "name": it,
              "type": "dynamic"
            }).toList();
            r.update(data.map((x) => x.values.where((it) => it is! ObjectId).toList()).toList());
            r.close();
          });
          return r;
        }),
        "dropCollection": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
          var db = dbForPath(path);
          await db.collection(params["collection"]).drop();
          return {};
        })
      },
      autoInitialize: false
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
    var url = configs[r"$$mongo_url"];
    Db db = new Db(url);

    await db.open();

    dbs[new Path(path).name] = db;

    var x = {
      "Insert_into_Collection": {
        r"$name": "Insert into Collection",
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
