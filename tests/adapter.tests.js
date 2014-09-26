
var should = require('chai').should();
var _ = require('lodash');
var Q = require('q');
var LowlaAdapter = require('../lib/adapter.js').LowlaAdapter;

describe('LowlaAdapter', function() {
  var lowlaDb;
  var testColl;

  var testDocPayload = {
    documents: [ {
      _lowla: {
        id: 'lowladbtest.TestCollection$1234'
      },
      ops: {
        $set: {
          a: 1,
          b: 2
        }
      }
    }]
  };

  beforeEach(function(done) {
    // TODO - use environment var to define test DB?
    lowlaDb = new LowlaAdapter({mongoUrl: 'mongodb://127.0.0.1/lowladbtest'});

    // Clear out the test collection before each test
    lowlaDb.ready.then(function() {
      lowlaDb.config.db.collection('TestCollection', function(err, coll) {
        testColl = coll;
        coll.remove(function() {
          done();
        });
      });
    });
  });

  describe('Push', function() {
    it('should return the pushed document', function () {
      return lowlaDb.pushWithPayload(testDocPayload)
        .then(function (result) {
          should.exist(result);
          result.should.have.length.of(2);
          result[0].id.should.equal('lowladbtest.TestCollection$1234');
          result[0].version.should.equal(1);
          result[0].clientNs.should.equal('lowladbtest.TestCollection');
          result[1].a.should.equal(1);
          result[1].b.should.equal(2);
          result[1]._version.should.equal(1);
        })
    });

    it('should create the document in MongoDB', function(done) {
      lowlaDb.pushWithPayload(testDocPayload)
        .then(function() {
              testColl.find().toArray(function(err, docs) {
            should.not.exist(err);
            should.exist(docs);
            docs.length.should.equal(1);
            docs[0]._id.should.equal('1234');
            docs[0].a.should.equal(1);
            docs[0].b.should.equal(2);
            done();
          });
        });
    });

    it('should update an existing doc', function() {
      return lowlaDb.pushWithPayload(testDocPayload)
        .then(function(result) {
          var newPayload = _.cloneDeep(testDocPayload);
          newPayload.documents[0].ops = { $set: { a: 11, b: 22 }};
          return lowlaDb.pushWithPayload(newPayload);
        })
        .then(function(result) {
          should.exist(result);
          result.should.have.length.of(2);
          result[0].version.should.equal(2);
          result[1].a.should.equal(11);
          result[1].b.should.equal(22);
        })
    });

    it('should delete an existing doc', function(done) {
      lowlaDb.pushWithPayload(testDocPayload)
        .then(function() {
          var newPayload = {
            documents: [ {
              _lowla: {
                id: 'lowladbtest.TestCollection$1234',
                version: 1,
                deleted: true
              }
            }]
          };
          return lowlaDb.pushWithPayload(newPayload);
        })
        .then(function(result) {
          should.exist(result);
          result.should.have.length.of(1);
          result[0].version.should.equal(1);
          result[0].deleted.should.be.true;
          testColl.find({_id: '1234'}).toArray(function(err, res) {
            should.not.exist(err);
            should.exist(res);
            res.should.have.length.of(0);
            done();
          })
        })
        .catch(done);
    });
  })

    describe('Pull', function() {

        it('should return a test document', function () {

            var newDoc = {_id:'1234', _version:1, a:1, b:2 }
            var deferred = Q.defer();
            testColl.insert(newDoc, function(err, result){
                if(err){
                    deferred.reject(err);
                }
                deferred.resolve(result);
            })

            return deferred.promise.then(function(){
                return lowlaDb.pullWithPayload(null)
                    .then(function (result) {
                        should.exist(result);
                        result.should.have.length.of(2);
                        result[0].id.should.equal('lowladbtest.TestCollection$1234');
                        result[0].version.should.equal(1);
                        result[0].clientNs.should.equal('lowladbtest.TestCollection');
                        result[1].a.should.equal(1);
                        result[1].b.should.equal(2);
                        result[1]._version.should.equal(1);
                    })
            });

        });

        it('should return several test documents', function () {
            var docs=[];
            for(i=0; i<=9; i++){
                docs.push({_id:'1234'+i, _version:i, a:1000+i, b:2000+i })
            }

            var deferred = Q.defer();
            testColl.insert(docs, function(err, result){
                if(err){
                    deferred.reject(err);
                }
                deferred.resolve(result);
            });

            return deferred.promise.then(function(){
                return lowlaDb.pullWithPayload(null)
                    .then(function (result) {
                        should.exist(result);
                        result.should.have.length.of(20);
                        var j=0;
                        for(i=0; i<20; i+=2) {
                            result[i].id.should.equal('lowladbtest.TestCollection$1234'+j);
                            result[i].version.should.equal(j);
                            result[i].clientNs.should.equal('lowladbtest.TestCollection');
                            result[i+1].a.should.equal(1000+j);
                            result[i+1].b.should.equal(2000+j);
                            result[i+1]._version.should.equal(j);
                            j++;
                        }

                    });
            });

        });

        it('should return requested documents', function () {
            var docs=[];
            var payload = {};
            payload.ids = [];
            for(i=0; i<=9; i++){
                docs.push({_id:'1234'+i, _version:i, a:1000+i, b:2000+i, testKey:i})
                if(0==i % 2){
                    payload.ids.push('lowladbtest.TestCollection$1234'+i);
                }
            }

            var deferred = Q.defer();
            testColl.insert(docs, function(err, result){
                if(err){
                    deferred.reject(err);
                }
                deferred.resolve(result);
            });

            return deferred.promise.then(function(){
                return lowlaDb.pullWithPayload(payload)
                    .then(function (result) {
                        should.exist(result);
                        result.should.have.length.of(10);
                        var testKey;
                        for(i=0; i<10; i+=2) {
                            testKey = result[i+1].testKey;  //version
                            result[i].id.should.equal('lowladbtest.TestCollection$1234'+testKey);
                            payload.ids.should.include(result[i].id);   //was it requested?
                            result[i].version.should.equal(testKey);
                            result[i].clientNs.should.equal('lowladbtest.TestCollection');
                            result[i+1].a.should.equal(1000+testKey);
                            result[i+1].b.should.equal(2000+testKey);
                            result[i+1]._version.should.equal(testKey);
                        }

                    });
            });

        });


    });


});
