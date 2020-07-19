var mongo = require("../");
var MongoClient = mongo.MongoClient;
const base_test = require("./base.test");
mongo.max_delay = 0;
MongoClient.persist = "mongo.json";
console.error = () => {};

describe("mock tests", function () {
  /** @type {import('./base.test').TestState} */
  const s = {
    client: null,
    db: null,
    coll: null,
  };

  before(function (done) {
    MongoClient.connect("mongodb://someserver/test_database", function (err, client) {
      s.client = client;
      s.db = client.db();
      s.coll = s.db.collection("users");
      done();
    });
  });
  after(function (done) {
    s.client.close().then(done).catch(done);
  });

  base_test(s);
});
