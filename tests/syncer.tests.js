
var should = require('chai').should();
var LowlaSyncer = require('../lib/sync.js').LowlaSyncer;
var testUtil = require('./testUtil');

testUtil.enableLongStackSupport();

describe('LowlaSync', function() {
  var lowlaSync;
  var testPayload;

  beforeEach(function() {
    lowlaSync = new LowlaSyncer();
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

    it('should be saving atoms', function() {
      return lowlaSync.updateWithPayload(testPayload)
        .then(function () {
          return lowlaSync.config.datastore.findAll(lowlaSync.config.atomPrefix, {});
        })
        .then(function (atoms) {
          atoms.length.should.equal(1);
          atoms[0]._id.should.equal('testdb.TestCollection$1234');
          atoms[0].version.should.equal(1);
          atoms[0].clientNs.should.equal('testDb.TestCollection');
          atoms[0].sequence.should.equal(1);
          atoms[0].deleted.should.equal(false);
        })
    });

    it('should be updating existing atoms', function() {
      return lowlaSync.updateWithPayload(testPayload)
        .then(function() {
          testPayload.modified[0].version = 3;
          return lowlaSync.updateWithPayload(testPayload);
        })
        .then(function() {
          return lowlaSync.config.datastore.findAll(lowlaSync.config.atomPrefix, {});
        })
        .then(function(atoms) {
          atoms.length.should.equal(1);
          atoms[0].version.should.equal(3);
          atoms[0].sequence.should.equal(3);
        });
    });
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
