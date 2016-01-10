import "package:mongo_dart/mongo_dart.dart";

main() async {
  Db db = new Db("mongodb://127.0.0.1/mdb");

  await db.open();
  await db.close();
}
