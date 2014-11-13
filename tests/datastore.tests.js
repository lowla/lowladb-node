var chai = require('chai');
var should = chai.should();
chai.use(require('chai-things'));
var sinon = require('sinon');
var _ = require('lodash');
var Datastore = require('../lib/datastore/datastore.js').Datastore;
var testUtil = require('./testUtil');
var Binary = require('mongodb').Binary;
var LowlaId = require('../lib/datastore/lowlaId.js').LowlaId;


testUtil.enableLongStackSupport();

var _db;
var _ds;


describe('Datastore', function () {

  before(function (done) {
    _ds = new Datastore({mongoUrl:'mongodb://127.0.0.1/lowladbtest', logger: console }); //testUtil.NullLogger});
    _ds.ready.then(function() {
      testUtil.mongo.openDatabase('mongodb://127.0.0.1/lowladbtest').then(function (db) {
        _db = db;
        done();
      });
    });
  });

  after(function (done) {
    if (_db) {
      _db.close();
      done();
    }
  });

  beforeEach(function (done) {
    testUtil.mongo.removeAllCollections(_db)
      .then(function () {
        return done();
      })
  });

  describe('Special data types', function () {

    it('encodes a date', function () {
      var msDate = 132215400000;
      var doc = { _id: '1234', a: 1, _version:1, date: new Date(msDate)};
      return testUtil.mongo.insertDocs(_db, "TestCollection", doc)
        .then(function () {
          return testUtil.mongo.findDocs(_db, 'TestCollection', {});
        }).then(function (docs) {
          var d = docs[0];
          d = _ds.encodeSpecialTypes(d);
          var date = d.date;
          date.should.not.be.instanceOf(Date);
          date.should.have.property('_bsonType');
          date.should.have.property('millis');
          date._bsonType.should.equal('Date');
          date.millis.should.equal(msDate);

        });
    });

    it('decodes a date', function () {
      var msDate = 132215400000;
      var doc = { _id: '1234', a: 1, _version:1, date: {_bsonType: 'Date', millis: 132215400000 }};
      var d = _ds.decodeSpecialTypes(doc);
      var date = d.date;
      date.should.be.instanceOf(Date);
      date.getTime().should.equal(msDate);
    });

    it('encodes embedded docs containing dates', function () {
      var msDate = 132215400000;
      var doc = { _id: '1234', a: 1, _version: 1,
        date: new Date(msDate),
        embed1:{ a: 1, date: new Date(msDate),
          embed2:{ a: 1, date: new Date(msDate),
            embed3:{a: 1, date: new Date(msDate)}
          }
        },
        end:true
      };
      return testUtil.mongo.insertDocs(_db, "TestCollection", doc)
        .then(function () {
          return testUtil.mongo.findDocs(_db, 'TestCollection', {});
        }).then(function (docs) {
          var d = docs[0];
          d = _ds.encodeSpecialTypes(d);
          d.date.should.not.be.instanceOf(Date);
          d.date.should.have.property('_bsonType');
          d.date.should.have.property('millis');
          d.date._bsonType.should.equal('Date');
          d.date.millis.should.equal(msDate);
          d.embed1.date.should.not.be.instanceOf(Date);
          d.embed1.date.should.have.property('_bsonType');
          d.embed1.date.should.have.property('millis');
          d.embed1.date._bsonType.should.equal('Date');
          d.embed1.date.millis.should.equal(msDate);
          d.embed1.embed2.date.should.not.be.instanceOf(Date);
          d.embed1.embed2.date.should.have.property('_bsonType');
          d.embed1.embed2.date.should.have.property('millis');
          d.embed1.embed2.date._bsonType.should.equal('Date');
          d.embed1.embed2.date.millis.should.equal(msDate);
          d.embed1.embed2.embed3.date.should.not.be.instanceOf(Date);
          d.embed1.embed2.embed3.date.should.have.property('_bsonType');
          d.embed1.embed2.embed3.date.should.have.property('millis');
          d.embed1.embed2.embed3.date._bsonType.should.equal('Date');
          d.embed1.embed2.embed3.date.millis.should.equal(msDate);

        });
    });

    it('decodes embedded docs containing dates', function () {
      var msDate = 132215400000;
      var dateField = {_bsonType: 'Date', millis: 132215400000 };
      var doc = { _id: '1234', a: 1, _version: 1,
        date: dateField,
        embed1:{ a: 1, date: dateField,
          embed2:{ a: 1, date: dateField,
            embed3:{a: 1, date: dateField}
          }
        },
        end:true
      };
      var d = _ds.decodeSpecialTypes(doc);
      d.date.should.be.instanceOf(Date);
      d.date.getTime().should.equal(msDate);
      d.embed1.date.should.be.instanceOf(Date);
      d.embed1.date.getTime().should.equal(msDate);
      d.embed1.embed2.date.should.be.instanceOf(Date);
      d.embed1.embed2.date.getTime().should.equal(msDate);
      d.embed1.embed2.embed3.date.should.be.instanceOf(Date);
      d.embed1.embed2.embed3.date.getTime().should.equal(msDate);
    });

    it('encodes a binary (text)', function () {
      var txt = 'Encoded String';
      var bin = new Binary(txt);
      var doc = { _id: '1234', a: 1, _version:1, val: bin};
      var d = _ds.encodeSpecialTypes(doc);
      d.val.encoded.should.equal('RW5jb2RlZCBTdHJpbmc=');
      d.val.encoded.should.equal(bin.toString('base64'));
    });

    it('decodes a binary (text)', function () {
      var doc = { _id: '1234', a: 1, _version:1, val: { _bsonType: 'Binary', type: 0, encoded: 'RW5jb2RlZCBTdHJpbmc=' }};
      var d = _ds.decodeSpecialTypes(doc);
      var val = d.val.toString('utf-8');
      val.should.equal('Encoded String');
    });

    it('decodes a binary (image)', function () {
      return testUtil.readFile('test.png').then(function(filedata) {
        var bin = new Binary(filedata);
        var doc = { _id: '1234', a: 1, _version: 1, val: { _bsonType: 'Binary', type: 0, encoded: bin.toString('base64') }};
        var d = _ds.decodeSpecialTypes(doc);
        d.val.should.have.property('_bsontype');
        d.val.should.have.property('buffer');
        d.val._bsontype.should.equal('Binary');
        d.val.toString('base64').should.equal(bin.toString('base64'));
      });
    });

    it('encodes a binary (image)', function () {
      return testUtil.readFile('test.png').then(function(filedata){
        var bin = new Binary(filedata);
        var doc = { _id: '1234', a: 1, _version:1, val:bin};
        return testUtil.mongo.insertDocs(_db, "TestCollection", doc)
          .then(function () {
            return testUtil.mongo.findDocs(_db, 'TestCollection', {});
          }).then(function (docs) {
            var d = _ds.encodeSpecialTypes(docs[0]);
            d.val.should.have.property('_bsonType');
            d.val.should.have.property('encoded');
            d.val._bsonType.should.equal('Binary');
            d.val.encoded.should.equal(bin.toString('base64'));
          });
      });
    });

    it('decodes embedded docs containing binaries (image)', function () {
      return testUtil.readFile('test.png').then(function(filedata) {
        var bin = new Binary(filedata);
        var binField = { _bsonType: 'Binary', type: 0, encoded: bin.toString('base64') };
        var doc = { _id: '1234', a: 1, _version: 1,
          val: binField,
          embed1:{ a: 1, val: binField,
            embed2:{ a: 1, val: binField,
              embed3:{a: 1, val: binField}
            }
          },
          end:true
        };
        var d = _ds.decodeSpecialTypes(doc);
        d.val.should.have.property('_bsontype');
        d.val.should.have.property('buffer');
        d.val._bsontype.should.equal('Binary');
        d.val.toString('base64').should.equal(bin.toString('base64'));

        d.embed1.val.should.have.property('_bsontype');
        d.embed1.val.should.have.property('buffer');
        d.embed1.val._bsontype.should.equal('Binary');
        d.embed1.val.toString('base64').should.equal(bin.toString('base64'));
        d.embed1.embed2.val.should.have.property('_bsontype');
        d.embed1.embed2.val.should.have.property('buffer');
        d.embed1.embed2.val._bsontype.should.equal('Binary');
        d.embed1.embed2.val.toString('base64').should.equal(bin.toString('base64'));
        d.embed1.embed2.embed3.val.should.have.property('_bsontype');
        d.embed1.embed2.embed3.val.should.have.property('buffer');
        d.embed1.embed2.embed3.val._bsontype.should.equal('Binary');
        d.embed1.embed2.embed3.val.toString('base64').should.equal(bin.toString('base64'));

      });
    });

    it('encodes embedded docs containing binaries (image)', function () {
      return testUtil.readFile('test.png').then(function(filedata){
        var bin = new Binary(filedata);
        var doc = { _id: '1234', a: 1, _version:1, val:bin, embed1:{a: 1, val:bin, embed2:{a: 1, val:bin, embed3:{a: 1, val:bin}}}, end:true};
        return testUtil.mongo.insertDocs(_db, "TestCollection", doc)
          .then(function () {
            return testUtil.mongo.findDocs(_db, 'TestCollection', {});
          }).then(function (docs) {
            var d = _ds.encodeSpecialTypes(docs[0]);
            d.val.should.have.property('_bsonType');
            d.val.should.have.property('encoded');
            d.val._bsonType.should.equal('Binary');
            d.val.encoded.should.equal(bin.toString('base64'));
            d.embed1.val.should.have.property('_bsonType');
            d.embed1.val.should.have.property('encoded');
            d.embed1.val._bsonType.should.equal('Binary');
            d.embed1.val.encoded.should.equal(bin.toString('base64'));
            d.embed1.embed2.val.should.have.property('_bsonType');
            d.embed1.embed2.val.should.have.property('encoded');
            d.embed1.embed2.val._bsonType.should.equal('Binary');
            d.embed1.embed2.val.encoded.should.equal(bin.toString('base64'));
            d.embed1.embed2.embed3.val.should.have.property('_bsonType');
            d.embed1.embed2.embed3.val.should.have.property('encoded');
            d.embed1.embed2.embed3.val._bsonType.should.equal('Binary');
            d.embed1.embed2.embed3.val.encoded.should.equal(bin.toString('base64'));
          });
      });
    });

    it("modifies a document but not it's binary", function () {
      var bin;
      return testUtil.readFile('test.txt').then(function (filedata) {
        bin = new Binary(filedata);
      }).then(function () {
        var doc = { _id: '1234', a: 1, b: 2, _version: 1, val: bin};
        return testUtil.mongo.insertDocs(_db, "TestCollection", doc)
          .then(function () {
            return testUtil.mongo.findDocs(_db, 'TestCollection', {});
          }).then(function (docs) {
            docs.length.should.equal(1);
            var d = docs[0];
            d.a.should.equal(1);
            d.b.should.equal(2);
            d._version.should.equal(1);
            d.val.should.have.property('_bsontype');
            d.val.should.have.property('buffer');
            d.val._bsontype.should.equal('Binary');
            d.val.toString('base64').should.equal(bin.toString('base64'));
            var ops = {
              $set: {
                a: 99,
                b: 5
              }
            };
            return _ds.updateDocumentByOperations(createLowlaId('TestCollection', docs[0]._id), docs[0]._version, ops);
          }).then(function (newDoc) {
            newDoc.a.should.equal(99);
            newDoc.b.should.equal(5);
            newDoc.val.should.have.property('_bsontype');
            newDoc.val.should.have.property('buffer');
            newDoc.val._bsontype.should.equal('Binary');
            newDoc.val.toString('base64').should.equal(bin.toString('base64'));
            return testUtil.mongo.findDocs(_db, 'TestCollection', {});
          }).then(function (docs) {

            docs.length.should.equal(1);
            var d = docs[0];
            d.a.should.equal(99);
            d.b.should.equal(5);
            d.val.should.have.property('_bsontype');
            d.val.should.have.property('buffer');
            d.val._bsontype.should.equal('Binary');
            d.val.toString('base64').should.equal(bin.toString('base64'));
          });
      });
    });


    it("modifies a document and it's binary", function () {
      var bin;
      var bin2;
      return testUtil.readFile('test.txt').then(function (filedata) {
        bin = new Binary(filedata);
      }).then(function () {
        return testUtil.readFile('test.png').then(function (filedata) {
          bin2 = new Binary(filedata);
        });
      }).then(function () {
        var doc = { _id: '1234', a: 1, b: 2, _version: 1, val: bin};
        return testUtil.mongo.insertDocs(_db, "TestCollection", doc)
          .then(function () {
            return testUtil.mongo.findDocs(_db, 'TestCollection', {});
          }).then(function (docs) {
            docs.length.should.equal(1);
            var d = docs[0];
            d.a.should.equal(1);
            d.b.should.equal(2);
            d._version.should.equal(1);
            d.val.should.have.property('_bsontype');
            d.val.should.have.property('buffer');
            d.val._bsontype.should.equal('Binary');
            d.val.toString('base64').should.equal(bin.toString('base64'));
            var ops = {
              $set: {
                a: 99,
                b: 5,
                val: { _bsonType: 'Binary', type: 0, encoded: bin2.toString('base64') }
              }
            };
            ops = _ds.decodeSpecialTypes(ops);
            return _ds.updateDocumentByOperations(createLowlaId('TestCollection', docs[0]._id), docs[0]._version, ops);
          }).then(function (newDoc) {
            newDoc.a.should.equal(99);
            newDoc.b.should.equal(5);
            newDoc.val.should.have.property('_bsontype');
            newDoc.val.should.have.property('buffer');
            newDoc.val._bsontype.should.equal('Binary');
            newDoc.val.toString('base64').should.equal(bin2.toString('base64'));
            return testUtil.mongo.findDocs(_db, 'TestCollection', {});
          }).then(function (docs) {

            docs.length.should.equal(1);
            var d = docs[0];
            d.a.should.equal(99);
            d.b.should.equal(5);
            d.val.should.have.property('_bsontype');
            d.val.should.have.property('buffer');
            d.val._bsontype.should.equal('Binary');
            d.val.toString('base64').should.equal(bin2.toString('base64'));
          });
      });
    });

  });

  describe('Creates and modifies documents', function () {

    it('creates a document', function () {
      var ops = {
        $set: {
          a: 98,
          b: 7
        }
      };
      var ObjectID = require('mongodb').ObjectID;
      return _ds.updateDocumentByOperations(createLowlaId('TestCollection', new ObjectID()), undefined, ops)
        .then(function (newDoc) {
          newDoc.a.should.equal(98);
          newDoc.b.should.equal(7);
          return testUtil.mongo.findDocs(_db, 'TestCollection', {});

        }).then(function (docs) {
          docs.length.should.equal(1);
          docs[0].a.should.equal(98);
          docs[0].b.should.equal(7);

        });
    });

    it('rejects updates without IDs', function(){
      var ops = {
        $set: {
          a: 98,
          b: 7
        }
      };
      var lowlaId = createLowlaId('TestCollection', '123');
      delete lowlaId.id;
      return _ds.updateDocumentByOperations(lowlaId, undefined, ops)
        .then(function (newDoc) {
          should.not.exist(newDoc);
        }, function(err){
          err.message.should.equal('Datastore.updateDocumentByOperations: id must be specified')
        })
    });

    it('modifies a document', function () {
      return testUtil.mongo.insertDocs(_db, "TestCollection", testUtil.createDocs("foo", 1))
        .then(function() {
          return testUtil.mongo.findDocs(_db, 'TestCollection', {});
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
          };
          return _ds.updateDocumentByOperations(createLowlaId('TestCollection', docs[0]._id), docs[0]._version,  ops);
        }).then(function(newDoc){
          newDoc.a.should.equal(99);
          newDoc.b.should.equal(5);
          return testUtil.mongo.findDocs(_db, 'TestCollection', {});
        }).then(function(docs) {
          docs.length.should.equal(1);
          docs[0].a.should.equal(99);
          docs[0].b.should.equal(5);
        });
    });

    it('creates a conflict', function () {  //TODO conflict not handled, update ignored
      var seeds = testUtil.createDocs("foo", 1);
      seeds[0]._version=2;
      return testUtil.mongo.insertDocs(_db, "TestCollection", seeds)
        .then(function() {
          return testUtil.mongo.findDocs(_db, 'TestCollection', {});
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
          };
          return _ds.updateDocumentByOperations(createLowlaId('TestCollection', docs[0]._id), oldVers,  ops);
        }).then(null, function(result){
          result.isConflict.should.be.true;
          should.not.exist(result.document);
          return testUtil.mongo.findDocs(_db, 'TestCollection', {});
        }).then(function(docs) {
          docs.length.should.equal(1);
          docs[0].a.should.equal(1);
          docs[0].b.should.equal(2);
        });
    });

    it('deletes a document', function () {
      return testUtil.mongo.insertDocs(_db, "TestCollection", testUtil.createDocs("foo", 1))
        .then(function() {
          return testUtil.mongo.findDocs(_db, 'TestCollection', {});
        }).then(function(docs){
          docs.length.should.equal(1);
          return _ds.removeDocument(createLowlaId('TestCollection', docs[0]._id))
        }).then(function(numRemoved){
          //numRemoved.should.equal(1);
          return testUtil.mongo.findDocs(_db, 'TestCollection', {});
        }).then(function(docs) {
          docs.length.should.equal(0);
        });
    });

  });


  describe('Retrieves documents', function () {

    beforeEach(function (done) {
      testUtil.mongo.insertDocs(_db, "TestCollection", testUtil.createDocs("TestCollection_", 10))
        .then(testUtil.mongo.insertDocs(_db, "TestCollection2", testUtil.createDocs("TestCollection2_", 10)))
        .then(testUtil.mongo.insertDocs(_db, "TestCollection3", testUtil.createDocs("TestCollection3_", 10)))
        .then(function () {
          done();
        });
    });

    it('gets a doc by id', function () {
      var id;
      return testUtil.mongo.getIds(_db, 'TestCollection')
        .then(function (ids) {
          id=ids[2];
          return _ds.getDocument(createLowlaId('TestCollection', ids[2]));
        })
        .then(function (doc) {
          doc.name.should.equal('TestCollection_3');
          doc.a.should.equal(3);
        });
    });

    it('gets all docs from all collections', function () {
      var h = createResultHandler();
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
        var results = h.getResults();
        var collections = {};
        for (i = 0; i < results.length; i++) {
          results.length.should.equal(30);
          collections[results[i].lowlaId.collectionName]=true;
          results[i].doc.name.should.equal(results[i].lowlaId.collectionName + "_" + results[i].doc.a)
        }
        collections["TestCollection"].should.be.true;
        collections["TestCollection2"].should.be.true;
        collections["TestCollection3"].should.be.true;
      });
    });

  });

  describe('Basics', function(){

    it('gets collection names', function(){
      return testUtil.mongo.insertDocs(_db, "aCollection", testUtil.createDocs("TestCollection_", 1))
        .then(testUtil.mongo.insertDocs(_db, "bCollection", testUtil.createDocs("TestCollection2_", 1)))
        .then(testUtil.mongo.insertDocs(_db, "cCollection", testUtil.createDocs("TestCollection3_", 1)))
        .then(function () {
          return _ds.getCollectionNames().then(function(names){
            should.exist(names);
            names.length.should.equal(3);
            names.should.contain('aCollection');
            names.should.contain('bCollection');
            names.should.contain('cCollection');
          });
        });
    });

    it('gets a collection promise', function(){
      return _ds.getCollection("TestCollection").then(function(collection){
        should.exist(collection);
        collection.db.databaseName.should.equal(_ds.config.db.databaseName);
        collection.collectionName.should.equal('TestCollection');
      });
    });

    it('finds in collection', function(){
      return testUtil.mongo.insertDocs(_db, "TestCollection", testUtil.createDocs("TestCollection_", 3))
        .then(function(){
          return _ds.findInCollection('TestCollection', {a:2})
            .then(function(cursor){
              return _ds.cursorToArray(cursor);
            }).then(function(docs){
              console.log(docs);
            });
        })
    });

    it('streams a cursor', function(){
      var h = createResultHandler();
      h.start();
      return testUtil.mongo.insertDocs(_db, "TestCollection", testUtil.createDocs("TestCollection_", 3))
        .then(function(){
          return _ds.findInCollection('TestCollection', {})
            .then(function(cursor){
              return _ds.streamCursor(cursor, 'TestCollection', h);
            }).then(function(res){
              h.end();
              var results = h.getResults();
              console.log(results);
              results.length.should.equal(3);
              results.should.all.have.property('lowlaId');
              results.should.all.have.property('deleted');
              results.should.all.have.property('doc');
            });
        })
    });

  });

  describe('Error Handling', function(){

    afterEach(function(){
      sinon.sandbox.restore();
    });

    it('handles cb->err in getCollectionNames', function(){
      sinon.sandbox.stub(_ds.config.db, 'collectionNames', function(callback){
        callback(Error("Error loading collectionNames"), null)
      });
      return testUtil.mongo.insertDocs(_db, "aCollection", testUtil.createDocs("TestCollection_", 1))
        .then(testUtil.mongo.insertDocs(_db, "bCollection", testUtil.createDocs("TestCollection2_", 1)))
        .then(testUtil.mongo.insertDocs(_db, "cCollection", testUtil.createDocs("TestCollection3_", 1)))
        .then(function () {
          return _ds.getCollectionNames().then(function(names) {
            should.not.exist(names);
          }, function(err){
            err.message.should.equal('Error loading collectionNames')
          });
        });
    });

    it('catches throw in getCollectionNames', function(){
      sinon.sandbox.stub(_ds.config.db, 'collectionNames').throws(Error('Error loading collectionNames'));
      return testUtil.mongo.insertDocs(_db, "aCollection", testUtil.createDocs("TestCollection_", 1))
        .then(testUtil.mongo.insertDocs(_db, "bCollection", testUtil.createDocs("TestCollection2_", 1)))
        .then(testUtil.mongo.insertDocs(_db, "cCollection", testUtil.createDocs("TestCollection3_", 1)))
        .then(function () {
          return _ds.getCollectionNames().then(function(names) {
            should.not.exist(names);
          }, function(err){
            err.message.should.equal('Error loading collectionNames')
          });
        });
    });

    it('handles cb->err in getCollection', function(){
      sinon.sandbox.stub(_ds.config.db, 'collection', function(name, callback){
        callback(Error("Error loading collection"), null)
      });
      return _ds.getCollection("TestCollection").then(function(collection){
        should.not.exist(collection);
      }, function(err){
        err.message.should.equal('Error loading collection')
      });
    });

    it('catches throw in getCollection', function(){
      sinon.sandbox.stub(_ds.config.db, 'collection').throws(Error('Error loading collection'));
      return _ds.getCollection("TestCollection").then(function(collection){
        should.not.exist(collection);
      }, function(err){
        err.message.should.equal('Error loading collection')
      });
    });

    it('handles cb->err in updateByOperations', function(){
      hookGetCollectionCall(function (collection) {
        sinon.sandbox.stub(collection, 'findAndModify', function(query, sort, updateOps, opts, callback){
          callback(Error("findAndModify returns callback error"), null)
        });
      });
      var ops = {
        $set: {
          a: 98,
          b: 7
        }
      };
      var ObjectID = require('mongodb').ObjectID;
      return _ds.updateDocumentByOperations(createLowlaId('TestCollection', new ObjectID()), undefined, ops)
        .then(function (newDoc) {
          should.not.exist(newDoc);
        }, function(err){
          err.message.should.equal('findAndModify returns callback error')
        })
    });

    it('catches throw in updateByOperations', function(){
      hookGetCollectionCall(function (collection) {
        sinon.sandbox.stub(collection, 'findAndModify').throws(new Error('findAndModify throws'));
      });
      var ops = {
        $set: {
          a: 98,
          b: 7
        }
      };
      var ObjectID = require('mongodb').ObjectID;
      return _ds.updateDocumentByOperations(createLowlaId('TestCollection', new ObjectID()), undefined, ops)
        .then(function (newDoc) {
          should.not.exist(newDoc);
        }, function(err){
          err.message.should.equal('findAndModify throws');
        })
    });

    it('catches throw in removeDocument', function () {
      hookGetCollectionCall(function (collection) {
        sinon.sandbox.stub(collection, 'remove').throws(new Error('remove throws'));
      });
      return _ds.removeDocument(createLowlaId('TestCollection', '123'))
        .then(function(numRemoved){
          should.not.exist(numRemoved);
        }, function(err){
          err.message.should.equal('remove throws');
        })
    });

    it('handles cb->err in removeDocument', function () {
      hookGetCollectionCall(function (collection) {
        sinon.sandbox.stub(collection, 'remove', function(id, callback){
          callback(Error("remove returns callback error"), null)
        });
      });

      return _ds.removeDocument(createLowlaId('TestCollection', '123'))
        .then(function(numRemoved){
          should.not.exist(numRemoved);
        }, function(err){
          err.message.should.equal('remove returns callback error');
        })
    });

    it('catches throw in findInCollection', function(){
      hookGetCollectionCall(function (collection) {
        sinon.sandbox.stub(collection, 'find').throws(new Error('find throws'));
      });
      return testUtil.mongo.insertDocs(_db, "TestCollection", testUtil.createDocs("TestCollection_", 3))
        .then(function(){
          return _ds.findInCollection('TestCollection', {a:2})
            .then(function(cursor){
              should.not.exist(cursor);
            }, function(err){
              err.message.should.equal('find throws');
            });
        })
    });

    it('handles cb->err in findInCollection', function(){
      hookGetCollectionCall(function (collection) {
        sinon.sandbox.stub(collection, 'find', function(id, callback){
          callback(Error("find returns callback error"), null)
        });
      });
      return testUtil.mongo.insertDocs(_db, "TestCollection", testUtil.createDocs("TestCollection_", 3))
        .then(function(){
          return _ds.findInCollection('TestCollection', {a:2})
            .then(function(cursor){
              should.not.exist(cursor);
            }, function(err){
              err.message.should.equal('find returns callback error');
            });
        })
    });

    it('catches throw in streamCursor', function(){
      var h = createResultHandler();
      h.start();
      return testUtil.mongo.insertDocs(_db, "TestCollection", testUtil.createDocs("TestCollection_", 3))
        .then(function(){
          return _ds.findInCollection('TestCollection', {})
            .then(function(cursor){
              sinon.sandbox.stub(cursor, 'stream').throws(new Error('cursor.stream error'));
              return _ds.streamCursor(cursor, 'TestCollection', h);
            }).then(function(result){
              should.not.exist(result);
            }, function(err){
              err.message.should.equal("cursor.stream error");
            });
        })
    });

    it('catches throw in getAllDocuments', function(){
      var h = createResultHandler();
      h.start();
      return testUtil.mongo.insertDocs(_db, "TestCollection", testUtil.createDocs("TestCollection_", 3))
        .then(function(){
          sinon.sandbox.stub(_ds, 'streamCursor').throws(new Error('cursor.stream error'));
          return _ds.getAllDocuments(h)
            .then(function(result){
              should.not.exist(result);
            }, function(err){
              err.message.should.equal("cursor.stream error");
            });
        })
    });

    it('catches throw in getAllDocuments', function(){
      var h = createResultHandler();
      h.start();
      return testUtil.mongo.insertDocs(_db, "TestCollection", testUtil.createDocs("TestCollection_", 3))
        .then(function(){
          sinon.sandbox.stub(_ds, 'findInCollection').throws(new Error('findInCollection error'));
          return _ds.getAllDocuments(h)
            .then(function(result){
              should.not.exist(result);
            }, function(err){
              err.message.should.equal("findInCollection error");
            });
        })
    });

  });

  //util
  var createLowlaId = function(collectionName, id){
    var lowlaId = new LowlaId();
    lowlaId.fromComponents(_db.databaseName, collectionName, id);
    return lowlaId;
  };

  var hookGetCollectionCall = function(fnHook){
    var origFunction = _ds.getCollection;
    sinon.sandbox.stub(_ds, 'getCollection', function (collectionName) {
      return origFunction.call(_ds, collectionName).then(function(collection){
        fnHook(collection);
        return collection;
      });
    });
  };

  var createResultHandler = function(){
    var startCalled = 0;
    var endCalled = 0;
    var writeCalled = 0;
    var results = [];
    return {
      start: function(){
        endCalled.should.be.lessThan(1);
        ++startCalled
      },
      write: function (lowlaId, version, deleted, doc) {
        startCalled.should.be.greaterThan(0);
        results.push({lowlaId: lowlaId, deleted:deleted, doc:doc});
        ++writeCalled;
      },
      end: function(){
        writeCalled.should.be.greaterThan(0);
        startCalled.should.be.greaterThan(0);
        ++endCalled
      },
      getResults: function(){
        return results;
      }
    }
  };




});