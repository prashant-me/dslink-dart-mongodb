# MongoDB DSLink

A DSLink for MongoDB.

## Usage

Ensure you have MongoDB installed somewhere. This can be on the server on which the link runs, or on a remote machine. See [this page](https://docs.mongodb.org/manual/installation/) for how to install MongoDB.

Invoke `Add Database` and input a name and the URL. This would be something like `mongodb://127.0.0.1/dsa` if the you installed the server locally.

From there, you can create watch groups which allow you to setup a group of paths to subscribe.

Data is stored in the database with collection names in the format of `{group}:{path}`.
For a logged point in the group `example` with a path of `/downstream/Example/message`, the collection that stores the history would be `example:/downstream/Example/message`.

## JavaScript Examples

The `Evaluate JavaScript` action allows you to perform JavaScript evaluation server-side.
This is the same thing that the mongo command line client utilizes.

### Fetch all data in a collection

The following will fetch all the data in the `example:/downstream/Example/message` collection.

```js
var cursor = db.getCollection("example:/downstream/Example/message").find();

return cursor.toArray();
```

### Fetch data that has a value less than 50

The following will fetch all the data in the `system:/downstream/System/CPU_Usage` collection where the value is less than 50.

```js
var cursor = db.getCollection("system:/downstream/System/CPU_Usage").find({
  "value": {
    "$lte": 50
  }
});
return cursor.toArray();
```
