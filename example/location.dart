import "dart:async";

import "package:dslink/dslink.dart";
import "package:dslink/io.dart";

final String KEY = "AIzaSyC9dyRtyQr1QJZuYlN_2gFsHfRw7COza2s";
final String ORIGIN = "Atlanta";
final String DESTINATION = "San+Francisco";
final String BASE = "https://maps.googleapis.com/maps/api/directions/json";

final String URL = "${BASE}?origin=${ORIGIN}&destination=${DESTINATION}&key=${KEY}";

main() async {
  var json = await HttpHelper.fetchJSON(URL);
  var routes = json["routes"];
  var out = [];
  var route = routes[0];
  var steps = route["legs"][0]["steps"];
  for (var step in steps) {
    var start_loc = step["start_location"];
    var end_loc = step["end_location"];
    out.add(start_loc);
    out.add(end_loc);
  }

  var link = new LinkProvider([], "LocationTest-", defaultNodes: {
    "location": {
      r"$name": "Location",
      r"$type": "array",
      "@geo": true
    }
  }, loadNodesJson: false, isResponder: true);

  link.connect();

  while (true) {
    for (Map m in out) {
      link.updateValue("/location", m);
      await new Future.delayed(const Duration(seconds: 1));
    }

    out = out.reversed.toList();
  }
}
