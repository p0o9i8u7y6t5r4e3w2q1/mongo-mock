const should = require("should");
const _ = require("lodash");
const ObjectID = require("bson-objectid").default;
const id = new ObjectID();

// this number is used in all the query/find tests, so it's easier to add more docs
var EXPECTED_TOTAL_TEST_DOCS = 13;

/** 
 * @typedef TestState 
 * @property {import('mongodb').MongoClient} client
 * @property {import('mongodb').Db} db
 * @property {import('mongodb').Collection} coll
 */

/** @param {TestState} s */
module.exports = function (s) {
  describe("databases", function () {
    it("should list collections", function (done) {
      var listCollectionName = "test_databases_listCollections_collection";
      s.db.createCollection(listCollectionName, function (err, listCollection) {
        if (err) return done(err);
        s.db.listCollections().toArray(function (err, items) {
          if (err) return done(err);
          var instance = _.find(items, {
            name: listCollectionName,
          });
          instance.should.not.be.undefined;
          done();
        });
      });
    });

    it("should drop collection", function (done) {
      var dropCollectionName = "test_databases_dropCollection_collection";
      s.db.createCollection(dropCollectionName, function (err, dropCollection) {
        if (err) return done(err);
        s.db.dropCollection(dropCollectionName, function (err, result) {
          if (err) return done(err);
          s.db.listCollections().toArray(function (err, items) {
            var instance = _.find(items, {
              name: dropCollectionName,
            });
            (instance === undefined).should.be.true;
            done();
          });
        });
      });
    });

    it("should drop collection by promise", function (done) {
      var dropCollectionName = "test_databases_dropCollection_collection_promise";
      s.db
        .createCollection(dropCollectionName)
        .then((collection) => {
          return s.db.dropCollection(dropCollectionName);
        })
        .then((result) => {
          return s.db.listCollections().toArray();
        })
        .then((items) => {
          var instance = _.find(items, {
            name: dropCollectionName,
          });
          (instance === undefined).should.be.true;
          done();
        });
    });

    it("should load another db", function (done) {
      var otherCollectionName = "someOtherCollection";
      var otherDb = s.client.db("some_other_mock_database");
      otherDb.createCollection(otherCollectionName, function (err, otherCollection) {
        if (err) return done(err);
        s.db.listCollections().toArray(function (err, mainCollections) {
          if (err) return done(err);
          otherDb.listCollections().toArray(function (err, otherCollections) {
            // otherDb should have a separate list of collections
            if (err) return done(err);
            var otherInstance = _.find(otherCollections, {
              name: otherCollectionName,
            });
            otherInstance.should.not.be.undefined;
            var mainInstance = _.find(mainCollections, {
              name: otherCollectionName,
            });
            (mainInstance === undefined).should.be.true;
            done();
          });
        });
      });
    });
  });

  describe("indexes", function () {
    it("should create a unique index", function (done) {
      s.coll.createIndex({ test: 1 }, { unique: true }, function (err, name) {
        if (err) return done(err);
        name.should.equal("test_1");
        done();
      });
    });

    it("should deny unique constraint violations on insert", function (done) {
      s.coll.insertMany(
        [{ test: 333 }, { test: 444 }, { test: 555, baz: 1 }, { test: 555, baz: 2 }],
        function (err, result) {
          (!!err).should.be.true;
          (!!result).should.be.false;
          err.message.should.equal(
            "E11000 duplicate key error collection: test_database.users index: test_1 dup key: { test: 555 }"
          );

          //the first one should succeed
          s.coll.findOne({ test: 555 }, function (err, doc) {
            if (err) return done(err);
            (!!doc).should.be.true;
            doc.should.have.property("baz", 1);
            done();
          });
        }
      );
    });
    it("should deny unique constraint violations on update", function (done) {
      s.coll.update({ test: 333 }, { $set: { test: 444, baz: 2 } }, function (err, result) {
        (!!err).should.be.true;
        (!!result).should.be.false;
        err.message.should.equal(
          "E11000 duplicate key error collection: test_database.users index: test_1 dup key: { test: 444 }"
        );

        //make sure it didn't update the data
        s.coll.findOne({ test: 333 }, function (err, doc) {
          if (err) return done(err);
          (!!doc).should.be.true;
          doc.should.not.have.property("baz");
          done();
        });
      });
    });

    it("should create a non-unique index", function (done) {
      s.coll.createIndex({ test_nonunique: 1 }, { unique: false }, function (err, name) {
        if (err) return done(err);
        name.should.equal("test_nonunique_1");
        done();
      });
    });

    it("should create a non-unique index by default", function (done) {
      s.coll.createIndex({ test_nonunique_default: 1 }, {}, function (err, name) {
        if (err) return done(err);
        s.coll.indexInformation({ full: true }, function (err, indexes) {
          if (err) return done(err);
          var index = _.filter(indexes, { name: "test_nonunique_default_1" })[0];
          (!!index.unique).should.be.false;
          done();
        });
      });
    });

    it("should allow insert with same non-unique index property", function (done) {
      s.coll.insertMany(
        [
          { test: 3333, test_nonunique: 3333 },
          { test: 4444, test_nonunique: 4444 },
          { test: 5555, test_nonunique: 3333 },
        ],
        function (err, result) {
          (!!err).should.be.false;
          result.result.ok.should.be.eql(1);
          result.result.n.should.eql(3);
          done();
        }
      );
    });
    it("should allow update with same non-unique index property", function (done) {
      s.coll.update({ test: 4444 }, { $set: { test_nonunique: 3333 } }, function (err, result) {
        (!!err).should.be.false;
        result.result.n.should.eql(1);
        done();
      });
    });
  });

  describe("collections", function () {
    "drop,insert,findOne,findOneAndUpdate,update,updateOne,updateMany,remove,deleteOne,deleteMany,save"
      .split(",")
      .forEach(function (key) {
        it("should have a '" + key + "' function", function () {
          s.coll.should.have.property(key);
          s.coll[key].should.be.type("function");
        });
      });

    it("should insert data", function (done) {
      s.coll.insertOne({ test: 123 }, function (err, result) {
        if (err) return done(err);
        (!!result.ops).should.be.true;
        (!!result.ops[0]).should.be.true;
        (!!result.ops[0]._id).should.be.true;
        result.ops[0]._id.toString().should.have.length(24);
        result.ops[0].should.have.property("test", 123);
        result.should.have.property("insertedId");
        result.should.have.property("insertedCount");
        done();
      });
    });
    it("should allow _id to be defined", function (done) {
      s.coll.insert({ _id: id, test: 456, foo: true }, function (err, result) {
        if (err) return done(err);
        (!!result.ops).should.be.true;
        (!!result.ops[0]).should.be.true;
        (!!result.ops[0]._id).should.be.true;
        result.ops[0]._id.toString().should.have.length(24);
        result.ops[0].should.have.property("test", 456);
        done();
      });
    });

    it("should findOne by a property", function (done) {
      s.coll.findOne({ test: 123 }, function (err, doc) {
        if (err) return done(err);
        (!!doc).should.be.true;
        doc.should.have.property("_id");
        doc._id.toString().should.have.length(24); //auto generated _id
        doc.should.have.property("test", 123);
        done();
      });
    });
    it("should return only the fields specified by field projection", () =>
      s.coll.findOne({ test: 456 }, { projection: { foo: 1 } }).then((doc) => {
        (!!doc).should.be.true;
        Object.keys(doc).sort().should.eql(["foo", "_id"].sort());
      }));
    it("should accept undefined fields", function (done) {
      s.coll.findOne({ test: 456 }, undefined, function (err, doc) {
        if (err) return done(err);
        (!!doc).should.be.true;
        doc.should.have.property("_id");
        doc._id.toString().should.have.length(24); //auto generated _id
        doc.should.have.property("test", 456);
        doc.should.have.property("foo", true);
        done();
      });
    });
    it("should find by an ObjectID", function (done) {
      s.coll.find({ _id: new ObjectID(id.toHexString()) }).toArray(function (err, results) {
        if (err) return done(err);
        (!!results).should.be.true;
        results.should.have.length(1);
        var doc = results[0];
        doc.should.have.property("_id");
        id.toHexString().should.eql(doc._id.toHexString());
        doc.should.have.property("test", 456);
        doc.should.have.property("foo", true);
        done();
      });
    });
    it("should findOne by an ObjectID", function (done) {
      s.coll.findOne({ _id: id }, function (err, doc) {
        if (err) return done(err);
        (!!doc).should.be.true;
        doc.should.have.property("_id");
        id.toHexString().should.eql(doc._id.toHexString());
        doc.should.have.property("test", 456);
        done();
      });
    });
    it("should NOT findOne if it does not exist", function (done) {
      s.coll.findOne({ _id: "asdfasdf" }, function (err, doc) {
        if (err) return done(err);
        (!!doc).should.be.false;
        done();
      });
    });

    it("should NOT findOne if the collection has just been created", function (done) {
      var collection = s.db.collection("some_brand_new_collection");
      collection.findOne({ _id: "asdfasdf" }, function (err, doc) {
        if (err) return done(err);
        (!!doc).should.be.false;
        done();
      });
    });

    it("should find document where nulled property exists", function (done) {
      s.coll.insert({ _id: "g5f6h2df6g46j", a: true, b: null }, function (errInsert, resultInsert) {
        if (errInsert) return done(errInsert);
        (!!resultInsert.ops).should.be.true;
        s.coll.find({ b: { $exists: true } }).toArray(function (errFind, resultFind) {
          if (errFind) return done(errFind);
          s.coll.remove({ _id: "g5f6h2df6g46j" }, function (errRemove) {
            if (errRemove) return done(errRemove);
            resultFind.length.should.equal(1);
            resultFind[0].should.have.property("b", null);
            done();
          });
        });
      });
    });

    it("should not find document where property does not exists", function (done) {
      s.coll.insert({ _id: "weg8h7rt6h5weg69", a: 37 }, function (errInsert, resultInsert) {
        if (errInsert) return done(errInsert);
        (!!resultInsert.ops).should.be.true;
        s.coll
          .find({ _id: "weg8h7rt6h5weg69", b: { $exists: false } })
          .toArray(function (errFind, resultFind) {
            if (errFind) return done(errFind);
            s.coll.remove({ _id: "weg8h7rt6h5weg69" }, function (errRemove) {
              if (errRemove) return done(errRemove);
              resultFind.length.should.equal(1);
              resultFind[0].should.have.property("a", 37);
              done();
            });
          });
      });
    });

    it("should find document with nulled property and exists false", function (done) {
      s.coll.insert({ _id: "iuk51hf34", a: true, b: null }, function (errInsert, resultInsert) {
        if (errInsert) return done(errInsert);
        (!!resultInsert.ops).should.be.true;
        s.coll.find({ _id: "iuk51hf34", b: { $exists: false } }).toArray(function (errFind, resultFind) {
          if (errFind) return done(errFind);
          s.coll.remove({ _id: "iuk51hf34" }, function (errRemove) {
            if (errRemove) return done(errRemove);
            resultFind.length.should.equal(0);
            done();
          });
        });
      });
    });

    it("should update one (updateOne)", function (done) {
      //query, data, options, callback
      s.coll.updateOne({ test: 123 }, { $set: { foo: { bar: "buzz", fang: "dang" } } }, function (
        err,
        opResult
      ) {
        if (err) return done(err);
        opResult.result.n.should.equal(1);

        s.coll.findOne({ test: 123 }, function (err, doc) {
          if (err) return done(err);
          (!!doc).should.be.true;
          doc.should.have.property("foo", { bar: "buzz", fang: "dang" });
          done();
        });
      });
    });

    it("should update one (updateOne) with shallow overwrite", function (done) {
      //query, data, options, callback
      s.coll.updateOne({ test: 123 }, { $set: { foo: { newValue: "bar" } } }, function (err, opResult) {
        if (err) return done(err);
        opResult.result.n.should.equal(1);

        s.coll.findOne({ test: 123 }, function (err, doc) {
          if (err) return done(err);
          (!!doc).should.be.true;
          doc.should.have.property("foo", { newValue: "bar" });
          done();
        });
      });
    });

    it("should update one (findOneAndUpdate)", function (done) {
      //query, data, options, callback
      s.coll.findOneAndUpdate({ test: 123 }, { $set: { foo: "john" } }, function (err, opResult) {
        if (err) return done(err);
        opResult.should.have.properties("ok", "lastErrorObject", "value");
        opResult.ok.should.equal(1);
        opResult.value.should.have.property("foo", { newValue: "bar" });

        s.coll.findOne({ test: 123 }, function (err, doc) {
          if (err) return done(err);
          (!!doc).should.be.true;
          doc.should.have.property("foo", "john");
          done();
        });
      });
    });

    it("should create one (findOneAndUpdate) with upsert and no document found", function (done) {
      //query, data, options, callback
      s.coll.findOneAndUpdate(
        { test: 1689 },
        { $set: { foo: "john" }, $setOnInsert: { bar: "dang" } },
        { upsert: true, returnOriginal: false },
        function (err, opResult) {
          if (err) return done(err);
          opResult.should.have.properties("ok", "lastErrorObject", "value");
          opResult.lastErrorObject.should.have.property("upserted");
          opResult.lastErrorObject.should.have.property("updatedExisting", false);
          opResult.lastErrorObject.should.have.property("n", 1);

          opResult.value.should.have.property("foo", "john");
          opResult.value.should.have.property("bar", "dang");

          s.coll.findOne({ test: 1689 }, function (err, doc) {
            if (err) return done(err);
            (!!doc).should.be.true;
            doc.should.have.property("foo", "john");
            doc.should.have.property("bar", "dang");
            done();
          });
        }
      );
    });

    it("should update one (findOneAndUpdate) with upsert and matching document found", function (done) {
      //query, data, options, callback
      s.coll.findOneAndUpdate(
        { test: 1689 },
        { $set: { foo: "john" }, $setOnInsert: { bar: "dang" } },
        { upsert: true },
        function (err, opResult) {
          if (err) return done(err);
          opResult.should.have.properties("ok", "lastErrorObject", "value");
          opResult.lastErrorObject.should.have.property("updatedExisting", true);
          opResult.lastErrorObject.should.have.property("n", 1);

          opResult.value.should.have.property("foo", "john");

          s.coll.findOne({ test: 1689 }, function (err, doc) {
            if (err) return done(err);
            (!!doc).should.be.true;
            doc.should.have.property("foo", "john");
            done();
          });
        }
      );
    });

    it("should create one (findOneAndUpdate) with upsert and no document found, complex filter", function (done) {
      //query, data, options, callback
      s.coll.findOneAndUpdate(
        { $and: [{ test: 1690 }, { another_test: 1691 }] },
        { $set: { foo: "alice" }, $setOnInsert: { bar: "bob" } },
        { upsert: true },
        function (err, opResult) {
          if (err) return done(err);
          opResult.should.have.properties("ok", "lastErrorObject", "value");
          opResult.lastErrorObject.should.have.property("upserted");
          opResult.lastErrorObject.should.have.property("updatedExisting", false);
          opResult.lastErrorObject.should.have.property("n", 1);

          (!!opResult.value).should.be.false;

          s.coll.findOne({ test: 1690, another_test: 1691 }, function (err, doc) {
            if (err) return done(err);
            (!!doc).should.be.true;
            doc.should.have.property("foo", "alice");
            doc.should.have.property("bar", "bob");
            done();
          });
        }
      );
    });

    it("should update one (findOneAndUpdate) with upsert and matching document found, complex filter", function (done) {
      //query, data, options, callback
      s.coll.findOneAndUpdate(
        { $and: [{ test: 1690 }, { another_test: 1691 }] },
        { $set: { foo: "alice2" }, $setOnInsert: { bar: "bob2" } },
        { upsert: true, returnOriginal: false },
        function (err, opResult) {
          if (err) return done(err);

          opResult.should.have.properties("ok", "lastErrorObject", "value");
          opResult.lastErrorObject.should.have.property("updatedExisting", true);
          opResult.lastErrorObject.should.have.property("n", 1);

          opResult.value.should.have.property("foo", "alice2");
          opResult.value.should.have.property("bar", "bob"); // Update here, no insertion.

          function cleanup(cb) {
            s.coll.remove({ test: 1690, another_test: 1691 }, cb);
          }

          s.coll.findOne({ test: 1690, another_test: 1691 }, function (err, doc) {
            if (err) {
              return cleanup(function () {
                done(err);
              });
            }
            (!!doc).should.be.true;
            doc.should.have.property("foo", "alice2");

            cleanup(done);
          });
        }
      );
    });

    it("should create one (findOneAndUpdate) with upsert and no document found, more complex filter", function (done) {
      //query, data, options, callback
      s.coll.findOneAndUpdate(
        { $and: [{ test: 1790 }, { timestamp: { $lt: 1 } }] },
        { $set: { foo: "alice", timestamp: 1 }, $setOnInsert: { bar: "bob" } },
        { upsert: true },
        function (err, opResult) {
          if (err) return done(err);
          opResult.should.have.properties("ok", "lastErrorObject", "value");
          opResult.lastErrorObject.should.have.property("upserted");
          opResult.lastErrorObject.should.have.property("updatedExisting", false);
          opResult.lastErrorObject.should.have.property("n", 1);

          (!!opResult.value).should.be.false;

          s.coll.findOne({ test: 1790 }, function (err, doc) {
            if (err) return done(err);
            (!!doc).should.be.true;
            doc.should.have.property("test");
            doc.should.have.property("foo", "alice");
            doc.should.have.property("bar", "bob");
            doc.should.have.property("timestamp", 1);
            done();
          });
        }
      );
    });

    it("should not update one (findOneAndUpdate) with upsert and no matching document found, more complex filter", function (done) {
      //query, data, options, callback
      s.coll.findOneAndUpdate(
        { $and: [{ test: 1790 }, { timestamp: { $lt: 1 } }] },
        { $set: { foo: "alice2", timestamp: 1 }, $setOnInsert: { bar: "bob2" } },
        { upsert: true },
        function (err, opResult) {
          if (err) {
            if (err.code !== 11000) {
              return done(err);
            }
          }

          done();
        }
      );
    });

    it("should update one (findOneAndUpdate) with upsert and document found, more complex filter", function (done) {
      //query, data, options, callback
      s.coll.findOneAndUpdate(
        { $and: [{ test: 1790 }, { timestamp: { $lt: 2 } }] },
        { $set: { foo: "alice3", timestamp: 2 }, $setOnInsert: { bar: "bob3" } },
        { upsert: true, returnOriginal: false },
        function (err, opResult) {
          if (err) return done(err);
          opResult.should.have.properties("ok", "lastErrorObject", "value");
          opResult.lastErrorObject.should.have.property("updatedExisting", true);
          opResult.lastErrorObject.should.have.property("n", 1);

          opResult.value.should.have.property("test");
          opResult.value.should.have.property("foo", "alice3");
          opResult.value.should.have.property("bar", "bob");

          function cleanup(cb) {
            s.coll.remove({ test: 1790 }, cb);
          }

          s.coll.findOne({ test: 1790 }, function (err, doc) {
            if (err) {
              return cleanup(function () {
                done(err);
              });
            }
            (!!doc).should.be.true;
            doc.should.have.property("foo", "alice3");
            doc.should.have.property("bar", "bob");
            doc.should.have.property("timestamp", 2);
            cleanup(done);
          });
        }
      );
    });

    it("should create one (findOneAndUpdate) with upsert and no document found, using correct _id", function (done) {
      //query, data, options, callback
      s.coll.findOneAndUpdate(
        { $and: [{ _id: 123 }, { timestamp: 1 }] },
        { $set: { foo: "alice" } },
        { upsert: true, returnOriginal: false },
        function (err, opResult) {
          if (err) return done(err);
          opResult.value.should.have.property("_id", 123);
          opResult.value.should.have.property("foo", "alice");

          s.coll.remove({ _id: 123 }, function () {
            done();
          });
        }
      );
    });

    it("should update nothing (findOneAndUpdate) without upsert and no document found", function (done) {
      //query, data, options, callback
      s.coll.findOneAndUpdate(
        { $and: [{ _id: 123 }, { timestamp: 1 }] },
        { $set: { foo: "alice" } },
        { upsert: false },
        function (err, opResult) {
          if (err) return done(err);
          (!!opResult.value).should.be.false;
          done();
        }
      );
    });

    it("should update one (default)", function (done) {
      //query, data, options, callback
      s.coll.update({ test: 123 }, { $set: { foo: "bar" } }, function (err, opResult) {
        if (err) return done(err);
        opResult.result.n.should.equal(1);

        s.coll.findOne({ test: 123 }, function (err, doc) {
          if (err) return done(err);
          (!!doc).should.be.true;
          doc.should.have.property("foo", "bar");
          done();
        });
      });
    });
    it("should update multi", function (done) {
      s.coll.update({}, { $set: { foo: "bar" } }, { multi: true }, function (err, opResult) {
        if (err) return done(err);
        opResult.result.n.should.equal(9);

        s.coll.find({ foo: "bar" }).count(function (err, n) {
          if (err) return done(err);
          n.should.equal(9);
          done();
        });
      });
    });
    it("should updateMany", function (done) {
      s.coll.updateMany({}, { $set: { updateMany: "bar" } }, function (err, opResult) {
        if (err) return done(err);
        opResult.result.n.should.equal(9);
        opResult.result.nModified.should.equal(9);
        opResult.matchedCount.should.equal(9);
        opResult.modifiedCount.should.equal(9);

        s.coll.find({ updateMany: "bar" }).count(function (err, n) {
          if (err) return done(err);
          n.should.equal(9);
          done();
        });
      });
    });
    it("should update subdocs in dot notation", function (done) {
      s.coll.update({}, { $set: { "update.subdocument": true } }, function (err, opResult) {
        if (err) return done(err);
        opResult.result.n.should.equal(1);

        s.coll.find({ "update.subdocument": true }).count(function (err, n) {
          if (err) return done(err);
          n.should.equal(1);
          done();
        });
      });
    });
    it("should update subdoc arrays in dot notation", function (done) {
      s.coll.update({}, { $set: { "update.arr.0": true } }, function (err, opResult) {
        if (err) return done(err);
        opResult.result.n.should.equal(1);

        s.coll.find({ "update.arr.0": true }).count(function (err, n) {
          if (err) return done(err);
          n.should.equal(1);
          done();
        });
      });
    });
    it("should $unset", function (done) {
      var original = {
        test: 237,
        parent0: 999,
        parent1: { child1: 111, child2: 222, child3: 333, child4: { child5: 555 } },
      };
      var expected = '{"test":237,"parent1":{"child1":111,"child3":333,"child4":{}}}';

      s.coll
        .insert(original)
        .then((r1) =>
          s.coll
            .update(
              { test: 237 },
              { $unset: { parent0: 1, "parent1.child2": 1, "parent1.child4.child5": 1 } }
            )
            .then((r2) =>
              s.coll.findOne({ test: 237 }).then((doc) => {
                let copy = _.clone(doc);
                delete copy._id;
                JSON.stringify(copy).should.eql(expected);
              })
            )
        )
        .then(done)
        .catch(done);
    });
    it("should upsert", function (done) {
      //prove it isn't there...
      s.coll.findOne({ test: 1 }, function (err, doc) {
        if (err) return done(err);
        (!!doc).should.be.false;

        s.coll.update({ test: 1 }, { test: 1, bar: "none" }, { upsert: true }, function (err, opResult) {
          if (err) return done(err);
          opResult.result.n.should.equal(1);

          s.coll.find({ test: 1 }).count(function (err, n) {
            if (err) return done(err);
            n.should.equal(1);
            done();
          });
        });
      });
    });
    it("should upsert (updateMany)", function (done) {
      //prove it isn't there...
      s.coll.findOne({ upsertMany: 1 }, function (err, doc) {
        if (err) return done(err);
        (!!doc).should.be.false;

        s.coll.updateMany(
          { upsertMany: 1 },
          { $set: { upsertMany: 1, bar: "none" } },
          { upsert: true },
          function (err, opResult) {
            if (err) return done(err);
            opResult.result.n.should.equal(1);

            s.coll.find({ upsertMany: 1 }).count(function (err, n) {
              if (err) return done(err);
              n.should.equal(1);
              done();
            });
          }
        );
      });
    });
    it("should save (no _id)", function (done) {
      //prove it isn't there...
      s.coll.findOne({ test: 2 }, function (err, doc) {
        if (err) return done(err);
        (!!doc).should.be.false;

        s.coll.save({ test: 2, bar: "none" }, function (err, opResult) {
          if (err) return done(err);
          opResult.result.n.should.equal(1);

          s.coll.find({ test: 2 }).count(function (err, n) {
            if (err) return done(err);
            n.should.equal(1);
            done();
          });
        });
      });
    });
    it("should save (with _id)", function (done) {
      //prove it isn't there...
      s.coll.findOne({ test: 2 }, function (err, doc) {
        if (err) return done(err);
        (!doc).should.be.false;

        s.coll.save({ _id: doc._id, test: 3, bar: "none" }, function (err, opResult) {
          if (err) return done(err);
          opResult.result.n.should.equal(1);

          s.coll.find({ test: 3 }).count(function (err, n) {
            if (err) return done(err);
            n.should.equal(1);
            done();
          });
        });
      });
    });
    /***************/
    it("should delete one", function (done) {
      //query, data, options, callback
      s.coll.insert({ test: 967, delete: true }, function (err, result) {
        if (err) return done(err);
        (!!result.ops).should.be.true;

        s.coll.deleteOne({ test: 967 }, function (err, result) {
          if (err) return done(err);
          result.result.n.should.equal(1);

          s.coll.findOne({ test: 967 }, function (err, doc) {
            if (err) return done(err);
            (!!doc).should.be.false;
            done();
          });
        });
      });
    });
    it("should delete one and only one", function (done) {
      //query, data, options, callback
      s.coll.insertMany(
        [
          { test: 967, delete: true },
          { test: 5309, delete: true },
        ],
        function (err, result) {
          if (err) return done(err);
          (!!result.ops).should.be.true;

          s.coll.deleteOne({ delete: true }, function (err, result) {
            if (err) return done(err);
            result.result.n.should.equal(1);

            s.coll.find({ delete: true }).count(function (err, n) {
              if (err) return done(err);
              n.should.equal(1);
              done();
            });
          });
        }
      );
    });
    it("should delete many", function (done) {
      //query, data, options, callback
      s.coll.insertOne({ test: 967, delete: true }, function (err, result) {
        if (err) return done(err);
        (!!result.ops).should.be.true;
        s.coll.find({ delete: true }).count(function (err, n) {
          if (err) return done(err);
          n.should.equal(2);

          s.coll.deleteMany({ delete: true }, function (err, result) {
            if (err) return done(err);
            result.result.n.should.equal(2);

            s.coll.find({ delete: true }).count(function (err, n) {
              if (err) return done(err);
              n.should.equal(0);
              done();
            });
          });
        });
      });
    });
    it("should return a promise for deleteMany", function (done) {
      const prom = s.coll.deleteMany({ shouldNeverMatchAnythingImportant: true });
      prom.should.be.instanceOf(Promise);
      done();
    });
    it("should delete many using the $in symbol", function (done) {
      //query, data, options, callback
      s.coll.insertMany(
        [
          { test: 967, delete: true },
          { test: 418, delete: true },
        ],
        function (err, result) {
          if (err) return done(err);
          (!!result.ops).should.be.true;
          s.coll.find({ test: { $in: [418, 967] } }).count(function (err, n) {
            if (err) return done(err);
            n.should.equal(2);

            s.coll.deleteMany({ test: { $in: [418, 967] } }, function (err, result) {
              if (err) return done(err);
              result.result.n.should.equal(2);

              s.coll.find({ delete: true }).count(function (err, n) {
                if (err) return done(err);
                n.should.equal(0);
                done();
              });
            });
          });
        }
      );
    });
    it("should add to set (default)", function (done) {
      s.coll.update({ test: 123 }, { $addToSet: { boo: "bar" } }, function (err, opResult) {
        if (err) return done(err);
        opResult.result.n.should.equal(1);
        s.coll.findOne({ test: 123 }, function (err, doc) {
          if (err) return done(err);
          doc.should.have.property("boo", ["bar"]);
          done();
        });
      });
    });
    it("should add to set", function (done) {
      s.coll.update({ test: 123 }, { $addToSet: { boo: "foo" } }, function (err, opResult) {
        if (err) return done(err);
        opResult.result.n.should.equal(1);
        s.coll.findOne({ test: 123 }, function (err, doc) {
          if (err) return done(err);
          doc.should.have.property("boo", ["bar", "foo"]);
          done();
        });
      });
    });
    it("should not add to set already existing item", function (done) {
      s.coll.update({ test: 123 }, { $addToSet: { boo: "bar" } }, function (err, opResult) {
        if (err) return done(err);
        opResult.result.n.should.equal(1);
        s.coll.findOne({ test: 123 }, function (err, doc) {
          if (err) return done(err);
          doc.should.have.property("boo", ["bar", "foo"]);
          done();
        });
      });
    });
    it("should increment a number", function (done) {
      // add some fields to increment
      s.coll.update({ test: 333 }, { $set: { incTest: 1, multiIncTest: { foo: 1 } } }, function (
        err,
        result
      ) {
        if (err) done(err);
        s.coll.update({ test: 333 }, { $inc: { incTest: 1, "multiIncTest.foo": 2 } }, function (
          err,
          opResult
        ) {
          if (err) done(err);
          opResult.result.n.should.equal(1);
          s.coll.findOne({ test: 333 }, function (err, doc) {
            if (err) done(err);
            doc.incTest.should.equal(2);
            doc.multiIncTest.foo.should.equal(3);
            done();
          });
        });
      });
    });
    it("should decrement a number", function (done) {
      s.coll.update(
        { test: 333 },
        { $inc: { incTest: -1, "multiIncTest.foo": -2, "some.new.key": 42 } },
        function (err, opResult) {
          if (err) done(err);
          opResult.result.n.should.equal(1);
          s.coll.findOne({ test: 333 }, function (err, doc) {
            if (err) done(err);
            doc.incTest.should.equal(1);
            doc.multiIncTest.foo.should.equal(1);
            doc.some.new.key.should.equal(42);
            done();
          });
        }
      );
    });
    it("should push item into array", function (done) {
      s.coll.update({ test: 333 }, { $set: { pushTest: [] } }, function (err, result) {
        if (err) done(err);
        s.coll.update({ test: 333 }, { $push: { pushTest: { $each: [2] } } }, function (err, opResult) {
          if (err) done(err);
          opResult.result.n.should.equal(1);
          s.coll.findOne({ test: 333 }, function (err, doc) {
            if (err) done(err);
            doc.pushTest.should.have.length(1);
            doc.pushTest.should.containEql(2);
            done();
          });
        });
      });
    });
    it("should push item into array that does not yet exist on the doc", function (done) {
      s.coll.update({ test: 333 }, { $push: { newPushTest: { $each: [2] } } }, function (err, opResult) {
        if (err) done(err);
        opResult.result.n.should.equal(1);
        s.coll.findOne({ test: 333 }, function (err, doc) {
          if (err) done(err);
          doc.newPushTest.should.have.length(1);
          doc.newPushTest.should.containEql(2);
          done();
        });
      });
    });
    it("should push item into array + $slice", function (done) {
      s.coll.update({ test: 333 }, { $set: { pushTest: [] } }, function (err, result) {
        if (err) done(err);
        s.coll.update(
          { test: 333 },
          { $push: { pushTest: { $each: [1, 2, 3, 4], $slice: -2 } } },
          function (err, opResult) {
            if (err) done(err);
            opResult.result.n.should.equal(1);
            s.coll.findOne({ test: 333 }, function (err, doc) {
              if (err) done(err);
              doc.pushTest.should.have.length(2);
              doc.pushTest.should.containEql(3, 4);
              done();
            });
          }
        );
      });
    });
    it("should count the number of items in the collection - count method", function (done) {
      s.coll.should.have.property("count");
      s.coll.count({}, function (err, cnt) {
        if (err) done(err);
        cnt.should.equal(EXPECTED_TOTAL_TEST_DOCS);

        s.coll.count({ test: 333 }, function (err, singleCnt) {
          if (err) done(err);
          singleCnt.should.equal(1);
          done();
        });
      });
    });
    it("should count the number of items in the collection - countDocuments method", function (done) {
      s.coll.should.have.property("countDocuments");
      s.coll.countDocuments({}, function (err, cnt) {
        if (err) done(err);
        cnt.should.equal(EXPECTED_TOTAL_TEST_DOCS);

        s.coll.countDocuments({ test: 333 }, function (err, singleCnt) {
          if (err) done(err);
          singleCnt.should.equal(1);
          done();
        });
      });
    });
    it("should count the number of items in the collection - estimatedDocumentCount method", function (done) {
      s.coll.should.have.property("estimatedDocumentCount");
      s.coll.estimatedDocumentCount({}, function (err, cnt) {
        if (err) done(err);
        cnt.should.equal(EXPECTED_TOTAL_TEST_DOCS);
        done();
      });
    });
    it("should drop themselves", function (done) {
      var dropCollectionName = "test_collections_drop_collection";
      s.db.createCollection(dropCollectionName, function (err, dropCollection) {
        if (err) return done(err);
        dropCollection.drop(function (err, reply) {
          if (err) return done(err);
          s.db.listCollections().toArray(function (err, items) {
            if (err) return done(err);
            var instance = _.find(items, { name: dropCollectionName });
            (instance === undefined).should.be.true;
            done();
          });
        });
      });
    });
    it("should drop themselves by promise", function (done) {
      var dropCollectionName = "test_collections_drop_collection_promise";
      s.db
        .createCollection(dropCollectionName)
        .then((dropCollection) => {
          return dropCollection.drop();
        })
        .then(() => {
          return s.db.listCollections().toArray();
        })
        .then((items) => {
          var instance = _.find(items, { name: dropCollectionName });
          (instance === undefined).should.be.true;
          done();
        });
    });

    it("should have bulk operations", function (done) {
      s.coll.should.have.property("initializeOrderedBulkOp");
      s.coll.should.have.property("initializeUnorderedBulkOp");

      done();
    });

    it("should have bulk find", function (done) {
      var bulk = s.coll.initializeOrderedBulkOp();
      bulk.should.have.property("find");
      done();
    });

    it("should have bulk upsert", function (done) {
      var bulk = s.coll.initializeOrderedBulkOp();
      var findOps = bulk.find({});

      findOps.should.have.property("upsert");
      done();
    });

    it("should bulk updateOne", function (done) {
      var bulk = s.coll.initializeOrderedBulkOp();
      bulk.find({ test: { $exists: true } }).updateOne({
        $set: {
          bulkUpdate: true,
        },
      });
      bulk.execute().then(() => {
        s.coll.findOne({ bulkUpdate: true }).then((doc) => {
          if (doc && doc.bulkUpdate) {
            done();
          } else {
            done(new Error("Bulk operation did not updateOne"));
          }
        });
      });
    }).timeout(0);

    it("should bulk update", function (done) {
      var bulk = s.coll.initializeOrderedBulkOp();

      bulk.find({ test: { $exists: true } }).update({
        $set: {
          bulkUpdate: true,
        },
      });
      bulk.execute().then(() => {
        s.coll
          .find({ bulkUpdate: true })
          .toArray()
          .then((docs) => {
            if (docs.every((val) => val.bulkUpdate)) {
              done();
            } else {
              done(new Error("Bulk operation did not update"));
            }
          });
      });
    }).timeout(0);

    it("should bulk insert", function (done) {
      var bulk = s.coll.initializeOrderedBulkOp();

      bulk.insert([
        {
          test: 5353,
          bulkTest: true,
        },
        {
          test: 5454,
          bulkTest: true,
        },
      ]);

      bulk.execute().then(() => {
        s.coll.findOne({ test: 5353 }).then((doc) => {
          if (doc.bulkTest) {
            done();
          } else {
            done(new Error("Doc didn't get inserted"));
          }
        });
      });
    }).timeout(0);

    it("should bulk removeOne", function (done) {
      var bulk = s.coll.initializeOrderedBulkOp();

      bulk.find({ bulkTest: true }).removeOne();

      bulk.execute().then(() => {
        s.coll.findOne({ test: 5353 }).then((doc) => {
          if (doc) {
            done(new Error("Doc didn't get removed"));
          } else {
            done();
          }
        });
      });
    }).timeout(0);

    it("should bulk remove", function (done) {
      var bulk = s.coll.initializeOrderedBulkOp();

      bulk.find({ bulkTest: true }).remove();

      bulk.execute().then(() => {
        s.coll
          .find({ bulkTest: true })
          .toArray()
          .then((docs) => {
            if (docs.length > 0) {
              done(new Error("Docs didn't get removed"));
            } else {
              done();
            }
          });
      });
    }).timeout(0);
  });

  describe("cursors", function () {
    it("should return a count of found items", function (done) {
      var crsr = s.coll.find({});
      crsr.should.have.property("count");
      crsr.count(function (err, cnt) {
        cnt.should.equal(EXPECTED_TOTAL_TEST_DOCS);
        done();
      });
    });

    it("should limit the fields in the documents using project", function (done) {
      var crsr = s.coll.find({});
      crsr.should.have.property("project");
      crsr.project({ _id: 1 }).toArray(function (err, res) {
        res.length.should.equal(EXPECTED_TOTAL_TEST_DOCS);
        res.forEach(function (doc) {
          Object.keys(doc).should.eql(["_id"]);
        });
        done();
      });
    });

    it("should remove property/properites from the documents", function (done) {
      var crsr = s.coll.find({});
      crsr.should.have.property("project");
      crsr.project({ _id: 0, foo: 0 }).toArray(function (err, res) {
        res.length.should.equal(EXPECTED_TOTAL_TEST_DOCS);
        res.forEach(function (doc) {
          doc.should.not.have.keys("_id", "foo");
        });
        done();
      });
    });

    it("should skip 1 item", function (done) {
      var crsr = s.coll.find({});
      crsr.should.have.property("skip");
      crsr.skip(1).toArray(function (err, res) {
        res.length.should.equal(EXPECTED_TOTAL_TEST_DOCS - 1);
        done();
      });
    });

    it("should limit to 3 items", function (done) {
      var crsr = s.coll.find({});
      crsr.should.have.property("limit");
      crsr.limit(3).toArray(function (err, res) {
        res.length.should.equal(3);
        done();
      });
    });

    it("should skip 1 item, limit to 3 items", function (done) {
      var crsr = s.coll.find({});
      crsr.should.have.property("limit");
      crsr
        .skip(1)
        .limit(3)
        .toArray(function (err, res) {
          res.length.should.equal(3);
          done();
        });
    });

    it("should count all items regardless of skip/limit", function (done) {
      var crsr = s.coll.find({});
      crsr
        .skip(1)
        .limit(3)
        .count(false, function (err, cnt) {
          cnt.should.equal(EXPECTED_TOTAL_TEST_DOCS);
          done();
        });
    });

    it("should count only skip/limit results", function (done) {
      var crsr = s.coll.find({});
      crsr
        .skip(1)
        .limit(3)
        .count(true, function (err, cnt) {
          cnt.should.equal(3);
          done();
        });
    });

    it("should toggle count applySkipLimit and not", function (done) {
      var crsr = s.coll.find({}).skip(1).limit(3);
      crsr.count(false, function (err, cnt) {
        cnt.should.equal(EXPECTED_TOTAL_TEST_DOCS);
        crsr.count(function (err, cnt) {
          cnt.should.equal(3);
          done();
        });
      });
    });

    it("should count only skip/limit results but return actual count if less than limit", function (done) {
      var crsr = s.coll.find({});
      crsr
        .skip(4)
        .limit(6)
        .count(true, function (err, cnt) {
          cnt.should.equal(6);
          done();
        });
    });

    /*
    describe("sort", function () {
      var sort_db;
      var sort_collection;
      var date = new Date();
      var docs = [
        { sortField: null, otherField: 6 }, // null
        { sortField: [1], otherField: 4 }, // array
        { sortField: 42, otherField: 1 }, // number
        { sortField: true, otherField: 5 }, // boolean
        { sortField: "foo", otherField: 2 }, // string
        { sortField: /foo/, otherField: 7 }, // regex
        { sortField: { foo: "bar" }, otherField: 8 }, // object
        { sortField: date, otherField: 3 }, // date
      ];

      function stripIds(result) {
        return result.map(function (x) {
          delete x._id;
          return x;
        });
      }

      let sort_client = null;
      before(function (done) {
        MongoClient.connect("mongodb://localhost/sort_mock_database", function (err, client) {
          sort_client = client;
          sort_db = client.db();
          sort_collection = s.db.collection("sorting");
          sort_s.coll.insertMany(docs).then(function () {
            done();
          });
        });
      });
      after(function (done) {
        sort_client.close().then(done).catch(done);
      });

      it("should sort results by type order", function (done) {
        var sortedDocs = [
          { sortField: null, otherField: 6 }, // null
          { sortField: 42, otherField: 1 }, // number
          { sortField: "foo", otherField: 2 }, // string
          { sortField: { foo: "bar" }, otherField: 8 }, // object
          { sortField: [1], otherField: 4 }, // array
          { sortField: true, otherField: 5 }, // boolean
          { sortField: date, otherField: 3 }, // date
          { sortField: /foo/, otherField: 7 }, // regex
        ];

        var crsr = sort_s.coll.find({});
        crsr.should.have.property("sort");
        crsr.sort({ sortField: 1 }).toArray(function (err, sortRes) {
          if (err) done(err);
          stripIds(sortRes).should.eql(sortedDocs);
          done();
        });
      });

      it("should sort results by type order (reversed)", function (done) {
        var sortedDocs = [
          { sortField: /foo/, otherField: 7 }, // regex
          { sortField: date, otherField: 3 }, // date
          { sortField: true, otherField: 5 }, // boolean
          { sortField: [1], otherField: 4 }, // array
          { sortField: { foo: "bar" }, otherField: 8 }, // object
          { sortField: "foo", otherField: 2 }, // string
          { sortField: 42, otherField: 1 }, // number
          { sortField: null, otherField: 6 }, // null
        ];

        var crsr = sort_s.coll.find({});
        crsr.should.have.property("sort");
        crsr.sort({ sortField: -1 }).toArray(function (err, sortRes) {
          if (err) done(err);
          stripIds(sortRes).should.eql(sortedDocs);
          done();
        });
      });

      it("should sort results by value", function (done) {
        var sortedDocs = [
          { sortField: 42, otherField: 1 }, // number
          { sortField: "foo", otherField: 2 }, // string
          { sortField: date, otherField: 3 }, // date
          { sortField: [1], otherField: 4 }, // array
          { sortField: true, otherField: 5 }, // boolean
          { sortField: null, otherField: 6 }, // null
          { sortField: /foo/, otherField: 7 }, // regex
          { sortField: { foo: "bar" }, otherField: 8 }, // object
        ];

        var crsr = sort_s.coll.find({});
        crsr.should.have.property("sort");
        crsr.sort({ otherField: 1 }).toArray(function (err, sortRes) {
          if (err) done(err);
          stripIds(sortRes).should.eql(sortedDocs);
          done();
        });
      });
    });
    */

    it("should map results", function (done) {
      var crsr = s.coll.find({});
      crsr.should.have.property("map");
      crsr
        .map((c) => c.test)
        .toArray(function (err, res) {
          if (err) done(err);
          var sampleTest = 333;
          res.should.containEql(sampleTest);
          done();
        });
    });
    it("should return stream of documents", function (done) {
      var results = [];
      var crsr = s.coll.find({});
      crsr.should.have.property("on");
      crsr
        .on("data", function (data) {
          results.push(data);
        })
        .on("end", function () {
          results.length.should.equal(EXPECTED_TOTAL_TEST_DOCS);
          return done();
        });
    });

    it("should compute total with forEach", function (done) {
      s.coll.insertMany(
        [
          { testForEach: 1111111, value: 1, test: -1 },
          { testForEach: 1111111, value: 2, test: -2 },
          { testForEach: 1111111, value: 3, test: -3 },
          { testForEach: 1111111, value: 4, test: -4 },
        ],
        function (err) {
          if (err) done(err);
          var crsr = s.coll.find({ testForEach: 1111111 });
          crsr.should.have.property("forEach");

          var total = 0;
          crsr.forEach(
            function (doc) {
              total += doc.value;
            },
            function () {
              total.should.equal(10);
              done();
            }
          );
        }
      );
    });
  });
};
