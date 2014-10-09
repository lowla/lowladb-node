var should = require('chai').should();
var _ = require('lodash');
var _prom = require('../lib/promiseImpl.js');
var Datastore = require('../lib/datastore/datastore.js').Datastore;
var util = require('./testUtil');


util.enableQLongStackSupport();

var _db;
var _ds = new Datastore({mongoUrl:'mongodb://127.0.0.1/lowladbtest'});

describe('Datastore', function () {

  before(function (done) {
    util.mongo.openDatabase('mongodb://127.0.0.1/lowladbtest').then(function(db){
      _db = db;
      done();
    });
  });

  after(function (done) {
    if (_db) {
      _db.close();
      done();
    }
  });

  beforeEach(function (done) {
    util.mongo.removeAllCollections(_db)
      .then(function(){return done();})
  });

  describe('creates and modfies', function () {

    it('creates a document', function () {
      var ops = {
        $set: {
          a: 98,
          b: 7
        }
      }
      var ObjectID = require('mongodb').ObjectID;
      return _ds.updateDocumentByOperations('TestCollection', new ObjectID(), undefined, ops)
        .then(function (newDoc) {
          newDoc.a.should.equal(98);
          newDoc.b.should.equal(7);
          return util.mongo.findDocs(_db, 'TestCollection', {});

        }).then(function (docs) {
          docs.length.should.equal(1);
          docs[0].a.should.equal(98);
          docs[0].b.should.equal(7);

        });
    });

    it('modifies a document', function () {
      return util.mongo.insertDocs(_db, "TestCollection", util.createDocs("foo", 1))
        .then(function() {
          return util.mongo.findDocs(_db, 'TestCollection', {});
        }).then(function(docs){
          docs.length.should.equal(1);
          docs[0].a.should.equal(1);
          docs[0].b.should.equal(2);
          docs[0]._version.should.equal(1);
          var ops = {
            $set: {
              a: 99,
              b: 5
            }
          }
          return _ds.updateDocumentByOperations('TestCollection', docs[0]._id, docs[0]._version,  ops);
        }).then(function(newDoc){
          newDoc.a.should.equal(99);
          newDoc.b.should.equal(5);
          return util.mongo.findDocs(_db, 'TestCollection', {});
        }).then(function(docs) {
          docs.length.should.equal(1);
          docs[0].a.should.equal(99);
          docs[0].b.should.equal(5);
        });
    });

    it('creates a conflict', function () {  //TODO conflict not handled, update ignored
      var seeds = util.createDocs("foo", 1);
      seeds[0]._version=2;
      return util.mongo.insertDocs(_db, "TestCollection", seeds)
        .then(function() {
          return util.mongo.findDocs(_db, 'TestCollection', {});
        }).then(function(docs){
          docs.length.should.equal(1);
          docs[0].a.should.equal(1);
          docs[0].b.should.equal(2);
          docs[0]._version.should.equal(2);
          var oldVers = 1;
          var ops = {
            $set: {
              a: 99,
              b: 5
            }
          }
          return _ds.updateDocumentByOperations('TestCollection', docs[0]._id, oldVers,  ops);
        }).then(function(newDoc){
          should.not.exist(newDoc);
          return util.mongo.findDocs(_db, 'TestCollection', {});
        }).then(function(docs) {
          docs.length.should.equal(1);
          docs[0].a.should.equal(1);
          docs[0].b.should.equal(2);
        });
    });

    it('deletes a document', function () {
      return util.mongo.insertDocs(_db, "TestCollection", util.createDocs("foo", 1))
        .then(function() {
          return util.mongo.findDocs(_db, 'TestCollection', {});
        }).then(function(docs){
          docs.length.should.equal(1);
          return _ds.removeDocument('TestCollection', docs[0]._id)
        }).then(function(numRemoved){
          //numRemoved.should.equal(1);
          return util.mongo.findDocs(_db, 'TestCollection', {});
        }).then(function(docs) {
          docs.length.should.equal(0);
        });
    });

  });


  describe('retrieve documents', function () {

    beforeEach(function (done) {
      util.mongo.insertDocs(_db, "TestCollection", util.createDocs("TestCollection_", 10))
        .then(util.mongo.insertDocs(_db, "TestCollection2", util.createDocs("TestCollection2_", 10)))
        .then(util.mongo.insertDocs(_db, "TestCollection3", util.createDocs("TestCollection3_", 10)))
        .then(function () {
          done();
        });
    });

    it('gets a doc by id', function () {
      var id;
      return util.mongo.getIds(_db, 'TestCollection')
        .then(function (ids) {
          id=ids[2];
          return _ds.getDocument('TestCollection', ids[2]);
        })
        .then(function (doc) {
          doc.name.should.equal('TestCollection_3');
          doc.a.should.equal(3);
        });
    });

    it('gets all docs from all collections', function () {

      h = createResultHandler()
      h.start();

      return _ds.getAllDocuments(h).then(function (result) {

        h.end();

        result.length.should.equal(3);

        for (i in result) {
          result[i].sent.should.equal(10);
          ["lowladbtest.TestCollection",
            "lowladbtest.TestCollection2",
            "lowladbtest.TestCollection3"].should.include(result[i].namespace);
        }

        var docs = h.getDocuments();
        var collections = {};
        for (i = 0; i < docs.length; i++) {
          docs.length.should.equal(30)
          collections[docs[i].collection]=true;
          docs[i].doc.name.should.equal(docs[i].collection + "_" + docs[i].doc.a)
        }
      });

    });

  });

  //util

  var createResultHandler = function(){
    var startCalled = 0;
    var endCalled = 0
    var writeCalled = 0;
    var documents = [];
    return {
      start: function(){
        endCalled.should.be.lessThan(1);
        ++startCalled
      },
      write: function (dbName, collectionName, doc, deleted) {
        startCalled.should.be.greaterThan(0);
        documents.push({db:dbName, collection:collectionName, deleted:deleted, doc:doc})
        ++writeCalled;
      },
      end: function(){
        writeCalled.should.be.greaterThan(0)
        startCalled.should.be.greaterThan(0);
        ++endCalled
      },
      getDocuments: function(){
        return documents;
      }
    }
  }


});