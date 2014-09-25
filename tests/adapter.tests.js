
var should = require('chai').should();
var _ = require('lodash');
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
});
