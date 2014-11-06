var should = require('chai').should();
var _ = require('lodash');
var _prom = require('../lib/promiseImpl.js');
var Datastore = require('../lib/datastore/datastore.js').Datastore;
var util = require('./testUtil');
var Binary = require('mongodb').Binary;
var LowlaId = require('../lib/datastore/lowlaId.js').LowlaId;


util.enableLongStackSupport();

var _db;
var _ds;
var nonOp = function(){};

describe('Datastore', function () {

  before(function (done) {
    _ds = new Datastore({mongoUrl:'mongodb://127.0.0.1/lowladbtest', logger:{log:nonOp, debug:nonOp, info:nonOp, warn:nonOp, error:nonOp}});
    _ds.ready.then(function() {
      util.mongo.openDatabase('mongodb://127.0.0.1/lowladbtest').then(function (db) {
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
    util.mongo.removeAllCollections(_db)
      .then(function () {
        return done();
      })
  });

  describe('Special data types', function () {

    it('encodes a date', function () {
      var msDate = 132215400000;
      var doc = { _id: '1234', a: 1, _version:1, date: new Date(msDate)};
      return util.mongo.insertDocs(_db, "TestCollection", doc)
        .then(function () {
          return util.mongo.findDocs(_db, 'TestCollection', {});
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
      return util.mongo.insertDocs(_db, "TestCollection", doc)
        .then(function () {
          return util.mongo.findDocs(_db, 'TestCollection', {});
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
      var dateField = {_bsonType: 'Date', millis: 132215400000 }
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
      return util.readFile('test.png').then(function(filedata) {
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
      return util.readFile('test.png').then(function(filedata){
        var bin = new Binary(filedata);
        var doc = { _id: '1234', a: 1, _version:1, val:bin};
        return util.mongo.insertDocs(_db, "TestCollection", doc)
          .then(function () {
            return util.mongo.findDocs(_db, 'TestCollection', {});
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
      return util.readFile('test.png').then(function(filedata) {
        var bin = new Binary(filedata);
        var binField = { _bsonType: 'Binary', type: 0, encoded: bin.toString('base64') }
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
      return util.readFile('test.png').then(function(filedata){
        var bin = new Binary(filedata);
        var binx = new Binary("this is crap");
        var doc = { _id: '1234', a: 1, _version:1, val:bin, embed1:{a: 1, val:bin, embed2:{a: 1, val:bin, embed3:{a: 1, val:bin}}}, end:true};
        return util.mongo.insertDocs(_db, "TestCollection", doc)
          .then(function () {
            return util.mongo.findDocs(_db, 'TestCollection', {});
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
      return util.readFile('test.txt').then(function (filedata) {
        bin = new Binary(filedata);
      }).then(function () {
        var doc = { _id: '1234', a: 1, b: 2, _version: 1, val: bin};
        return util.mongo.insertDocs(_db, "TestCollection", doc)
          .then(function () {
            return util.mongo.findDocs(_db, 'TestCollection', {});
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
          }).then(function (result) {
            var newDoc  = result.document;
            newDoc.a.should.equal(99);
            newDoc.b.should.equal(5);
            newDoc.val.should.have.property('_bsontype');
            newDoc.val.should.have.property('buffer');
            newDoc.val._bsontype.should.equal('Binary');
            newDoc.val.toString('base64').should.equal(bin.toString('base64'));
            return util.mongo.findDocs(_db, 'TestCollection', {});
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
      return util.readFile('test.txt').then(function (filedata) {
        bin = new Binary(filedata);
      }).then(function () {
        return util.readFile('test.png').then(function (filedata) {
          bin2 = new Binary(filedata);
        });
      }).then(function () {
        var doc = { _id: '1234', a: 1, b: 2, _version: 1, val: bin};
        return util.mongo.insertDocs(_db, "TestCollection", doc)
          .then(function () {
            return util.mongo.findDocs(_db, 'TestCollection', {});
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
          }).then(function (result) {
            var newDoc  = result.document;
            newDoc.a.should.equal(99);
            newDoc.b.should.equal(5);
            newDoc.val.should.have.property('_bsontype');
            newDoc.val.should.have.property('buffer');
            newDoc.val._bsontype.should.equal('Binary');
            newDoc.val.toString('base64').should.equal(bin2.toString('base64'));
            return util.mongo.findDocs(_db, 'TestCollection', {});
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
        .then(function (result) {
          var newDoc  = result.document;
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
          };
          return _ds.updateDocumentByOperations(createLowlaId('TestCollection', docs[0]._id), docs[0]._version,  ops);
        }).then(function(result){
          var newDoc  = result.document;
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
          };
          return _ds.updateDocumentByOperations(createLowlaId('TestCollection', docs[0]._id), oldVers,  ops);
        }).then(function(result){
          result.isConflict.should.be.true;
          should.not.exist(result.document);
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
          return _ds.removeDocument(createLowlaId('TestCollection', docs[0]._id))
        }).then(function(numRemoved){
          //numRemoved.should.equal(1);
          return util.mongo.findDocs(_db, 'TestCollection', {});
        }).then(function(docs) {
          docs.length.should.equal(0);
        });
    });

  });


  describe('Retrieves documents', function () {

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
          return _ds.getDocument(createLowlaId('TestCollection', ids[2]));
        })
        .then(function (result) {
          var doc = result.document;
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

        var results = h.getResults();
        var collections = {};
        for (i = 0; i < results.length; i++) {
          results.length.should.equal(30)
          collections[results[i].lowlaId.collectionName]=true;
          results[i].document.name.should.equal(results[i].lowlaId.collectionName + "_" + results[i].document.a)
        }
        collections["TestCollection"].should.be.true;
        collections["TestCollection2"].should.be.true;
        collections["TestCollection3"].should.be.true;
    });

    });

  });

  //util
  var createLowlaId = function(collectionName, id){
    var lowlaId = new LowlaId();
    lowlaId.fromComponents(_db.databaseName, collectionName, id);
    return lowlaId;
  }


  var createResultHandler = function(){
    var startCalled = 0;
    var endCalled = 0
    var writeCalled = 0;
    var results = [];
    return {
      start: function(){
        endCalled.should.be.lessThan(1);
        ++startCalled
      },
      write: function (result) {
        startCalled.should.be.greaterThan(0);
        results.push(result)
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
  }


});