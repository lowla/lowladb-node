
var should = require('chai').should();
var LowlaSyncer = require('../lib/sync.js').LowlaSyncer;
var testUtil = require('./testUtil');

testUtil.enableLongStackSupport();

describe('LowlaSync', function() {
  var lowlaSync = new LowlaSyncer({
    syncUrl: 'mongodb://127.0.0.1/lowlasynctest'
  });

  var testPayload;

  beforeEach(function() {
    testPayload = {
      modified: [
        {
          id: "testdb.TestCollection$1234",
          version: 1,
          clientNs: "testDb.TestCollection"
        }
      ]
    };
  });

  beforeEach(function(done) {
    lowlaSync.ready.then(function() {
      lowlaSync.atoms.remove({}, function(err) {
        if (err) {
          throw err;
        }
        lowlaSync.sequences.remove({}, function(err) {
          if (err) {
            throw err;
          }
          done();
        });
      });
    })
  });

  describe('Updates', function() {
    it('should return updated sequence', function() {
      return lowlaSync.updateWithPayload(testPayload)
        .then(function(response) {
          should.exist(response);
          response.sequence.should.equal(2);
        })
    });

    it('should keep updating sequences', function() {
      return lowlaSync.updateWithPayload(testPayload)
        .then(function() {
          testPayload.modified[0].version = 3;
          return lowlaSync.updateWithPayload(testPayload);
        })
        .then(function(response) {
          response.sequence.should.equal(4);
        })
    });

    it('should be saving atoms', function(done) {
      lowlaSync.updateWithPayload(testPayload)
        .then(function() {
          lowlaSync.atoms.find().toArray(function(err, atoms) {
            if (err) {
              throw err;
            }

            atoms.should.have.length.of(1);
            atoms[0].remoteKey.should.equal('testdb.TestCollection$1234');
            atoms[0].version.should.equal(1);
            atoms[0].clientNs.should.equal('testDb.TestCollection');
            atoms[0].sequence.should.equal(1);
            atoms[0].deleted.should.be.false;
          })
        })
        .then(done, done);
    });

    it('should be updating existing atoms', function(done) {
      return lowlaSync.updateWithPayload(testPayload)
        .then(function() {
          testPayload.modified[0].version = 3;
          return lowlaSync.updateWithPayload(testPayload);
        })
        .then(function() {
          var cursor = lowlaSync.atoms.find();
          cursor.toArray(function (err, atoms) {
            if (err) {
              done(err);
            }
            atoms.should.have.length.of(1);
            atoms[0].version.should.equal(3);
            atoms[0].sequence.should.equal(3);
          })
        })
        .then(done, done);
    })
  });

  describe('Changes', function() {
    describe('with no atoms', function() {
      it('should have return no sequence', function() {
        return lowlaSync.changesSinceSequence().then(function (result) {
          should.exist(result);
          result.sequence.should.equal(0);
        })
      })
    });

    describe('with atoms', function() {
      beforeEach(function() {
        return lowlaSync.updateWithPayload(testPayload);
      });

      it('should return atoms with same sequence', function() {
        return lowlaSync.changesSinceSequence(1).then(function (result) {
          result.sequence.should.equal(2);
          result.atoms.should.have.length(1);
        })
      });

      it('should return no atoms with smaller sequence', function() {
        return lowlaSync.changesSinceSequence(4).then(function (result) {
          result.sequence.should.equal(2);
          result.atoms.should.have.length(0);
        });
      })
    })
  })
});
