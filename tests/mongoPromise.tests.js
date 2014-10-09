
// *** NOT IN USE: checked in for history only.  See note in mongoPromises.js (same commit) *** //

var should = require('chai').should();
var _ = require('lodash');
var _prom = require('../lib/promiseImpl.js');
var mongoP = require('../lib/datastore/mongoPromises.js');
var MongoClient = require('mongodb').MongoClient;
var util = require('./testUtil');

util.enableQLongStackSupport();

var _db;

describe('Mongo Promised', function () {

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
    .done(function(){
      done();
    });
  });

  describe('mongoPromises', function(){

    it('test the connection', function () {
      return mongoP.connect('mongodb://127.0.0.1/lowladbtest')
        .then(function (db) {
          return db.collectionNames();
        }).then(function (names) {
          names.length.should.greaterThan(1);
        });
    });

    it('inserts and finds a document', function () {
      var coll;
      return mongoP.connect('mongodb://127.0.0.1/lowladbtest')
        .then(function (db) {
          return db.collection('TestCollection');
        }).then(function (collection) {
          coll = collection;
          return collection.insert({name: 'foo', one: 1, two: 2});
        }).then(function (doc) {
          return coll.find({name: 'foo'});
        }).then(mongoP.cursor)
        .then(function (cursor) {
          return cursor.toArray();
        }).then(function (docArr) {
          docArr.length.should.equal(1);
          docArr[0].name.should.equal('foo');
        });
    });


    it('gets all the documents in all the collections', function () {

      var createDocs = function(rootName, num){
        var docs = [];
        for(i=0; i<num; i++){
          docs.push({name: rootName, one: i, two: 2*i})
        }
        return docs;
      }


      var testDb, coll1, coll2;

      return mongoP.connect('mongodb://127.0.0.1/lowladbtest')
        .then(function (db) {
          testDb = db;
          return testDb.collection('TestCollection');
        }).then(function (collection) {
          coll1 = collection;
          return collection.insert(createDocs("bar", 3));
        }).then(function () {
          return testDb.collection('TestCollection2');
        }).then(function (collection) {
          coll2 = collection;
          return collection.insert(createDocs("foo", 3));
        }).then(function(res){
          return testDb.collectionNames();
        }).then(function (colNames) {
          var promises = [];
          colNames.forEach(function (colname) {
            if (-1 == colname.name.indexOf('.system.')) {
              var collectionName = colname.name.substr(1 + colname.name.indexOf('.'))

              var p = testDb.collection(collectionName).then(function(coll){
                return coll.find({})
              }).then(mongoP.cursor)
                .then(function (cursor) {
                  return cursor.toArray();
                });
              promises.push(p);
            }
          });
          return _prom.all(promises);
        }).then(function (docArr) {
          var docs = [];
          for(i in docArr){
            for (j in docArr[i]){
              docs.push(docArr[i][j]);
            }
          }
          docs.length.should.equal(6);
          for(i in docs) {
            ['foo', 'bar'].should.contain(docs[i].name);
          }
        });
    });

  });

  describe('perf', function () {

    before(function(){
      if(_prom.hasOwnProperty('longStackSupport')){  //disable Q stack traces if we're using Q...
        _prom.longStackSupport = false;
        console.log("Q longStack support disabled... \n");
      }
    });

    after(function(){
        util.enableQLongStackSupport();
    });

    it('inserts and finds a document - PROMISE', function () {
      var coll;
      return mongoP.connect('mongodb://127.0.0.1/lowladbtest')
        .then(function (db) {
          return db.collection('TestCollection');
        }).then(function (collection) {
          coll = collection;
          return collection.insert({name: 'foo', one: 1, two: 2});
        }).then(function (doc) {
          return coll.find({name: 'foo'});
        }).then(mongoP.cursor)
        .then(function (cursor) {
          return cursor.toArray();
        }).then(function (docArr) {
          docArr.length.should.equal(1);
          docArr[0].name.should.equal('foo');
        });
    });

    it('inserts and finds a document - CALLBACK', function () {
      var testMC = new MongoClient();
      var testDb, testColl
      testMC.connect('mongodb://127.0.0.1/lowladbtest', function (err, db) {
        if (err) {
          throw err;
        }
        testDb = db;
        testDb.collection('TestCollection', function (err, coll) {
          if (err) {
            throw err;
          }
          coll.insert({name: 'bar', one: 1, two: 2}, function (err, doc) {
            if (err) {
              throw err;
            }
            coll.find({name: 'bar'}, function (err, cursor) {
              if (err) {
                throw err;
              }
              cursor.toArray(function (err, docArr) {
                if (err) {
                  throw err;
                }
                docArr.length.should.equal(1);
                docArr[0].name.should.equal('bar');
              })
            })
          })
        });
      });
    });

    it('repeatedly inserts/finds a document - PROMISE', function () {
      var all = []
      return mongoP.connect('mongodb://127.0.0.1/lowladbtest')
        .then(function (db) {
          for (i = 0; i < 3000; i++) {
            all.push(promisedInsert(db, i));

          }
          return _prom.all(all).then(function(result){
            //console.log(result);
          });
        });

    });

    it('repeatedly inserts/finds a document - CALLBACK', function () {
      var testMC = new MongoClient();
      var all = [];
      var deferred = _prom.defer();
      testMC.connect('mongodb://127.0.0.1/lowladbtest', function (err, db) {
        if (err) {
          deferred.reject(err);
        }
        for (i = 0; i < 3000; i++) {
          all.push(callbackInsert(db, i));
        }
        _prom.all(all).then(function(result){
          deferred.resolve(result);
        });
      });
      return deferred.promise;
    });

  });

  var promisedInsert = function (db, i) {
    var coll;
    return db.collection('TestCollection')
      .then(function (collection) {
        coll = collection;
        return collection.insert({name: 'foo' + i, one: 1, two: 2});
      }).then(function (doc) {
        return coll.find({name: 'foo' + i});
      }).then(mongoP.cursor)
      .then(function (cursor) {
        return cursor.toArray();
      }).then(function (docArr) {
        docArr.length.should.equal(1);
        docArr[0].name.should.equal('foo' + i);
        return docArr[0].name;
      });
  }

  var callbackInsert = function(db, i) {
    var deferred = _prom.defer();

      db.collection('TestCollection', function (err, coll) {
        if (err) {
         return deferred.reject(error);
        }
        coll.insert({name: 'bar' + i, one: 1, two: 2}, function (err, doc) {
          if (err) {
            return deferred.reject(error);
          }
          coll.find({name: 'bar' + i}, function (err, cursor) {
            if (err) {
              return deferred.reject(error);
            }
            cursor.toArray(function (err, docArr) {
              if (err) {
                return deferred.reject(error);
              }
              docArr[0].name.should.equal('bar' + i);
              deferred.resolve(docArr[0].name);
            })
          })
        })
      });

    return deferred.promise;
  }
});