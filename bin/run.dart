import "dart:async";

import "package:dslink/dslink.dart";
import "package:dslink/historian.dart";

import 'package:dslink_mongodb/mongo_historian.dart';

main(List<String> args) async {
  new Future.delayed(const Duration(seconds: 5), () async {
    if (link != null) {
      link.save();
    }
  });

  var adapter = new MongoHistorianAdapter();
  var result = await historianMain(args, "MongoDB", adapter);
  adapter.provider = link.provider;
  return result;
}


class ColumnsMarker {
  List columns;
  List rows;
}

SimpleNodeProvider get provider => link.provider;

