// @ts-check
var mongo = require("mongodb");
var MongoClient = mongo.MongoClient;
const base_test = require("./base.test");
console.error = () => {};

describe("mock tests", function () {
  /** @type {import('./base.test').TestState} */
  const s = {
    client: null,
    db: null,
    coll: null,
  };

  before(function (done) {
    MongoClient.connect("mongodb://localhost/test_database", async function (err, client) {
      s.client = client;
      s.db = client.db();
      s.db.dropDatabase(function (err, result) {
        s.coll = s.db.collection("users");
        done();
      });
    });
  });
  after(function (done) {
    s.client.close().then(done).catch(done);
  });

  base_test(s);
});
