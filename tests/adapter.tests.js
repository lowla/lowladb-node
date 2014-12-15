var chai = require('chai');
var should = chai.should();
chai.use(require('chai-things'));
var sinon = require('sinon');
var _ = require('lodash');
var _prom = require('../lib/promiseImpl.js');
var LowlaAdapter = require('../lib/adapter.js').LowlaAdapter;
var testUtil = require('./testUtil');
var util = require('util');


testUtil.enableLongStackSupport();

describe('LowlaAdapter', function() {
  var lowlaDb;
  var logger;

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

  var testDocPushResult = {
    _id: '1234',
    a: 1,
    b: 2,
    _version: 1
  };

  //TODO tests that verify push notifies syncr.
//  before(function(done){
//    var lowlaConfig = {
//      syncUrl: 'mongodb://127.0.0.1/lowlasynctest',
//      mongoUrl: 'mongodb://127.0.0.1/lowladbtest'
//    };
//
//    var LowlaSync = require('../lib/sync.js').LowlaSyncer;
//    var LowlaAdapter = require('../lib/adapter.js').LowlaAdapter;
//    lowlaSync = new LowlaSync(lowlaConfig);
//
//    lowlaConfig.syncNotifier = lowlaSync.getNotifierFunction();
//    lowlaDb = new LowlaAdapter(lowlaConfig);
//    done();
//  });


  var mockDatastore;

  beforeEach(function(){
    mockDatastore = testUtil.createMockDatastore();

    logger = new testUtil.TestLogger();
    lowlaDb = new LowlaAdapter({datastore: mockDatastore, logger:logger});
  });

  describe('Push', function() {   //full route tests adapter.push(req, res, next) -- see pushWithPayload tests for more detailed tests of adapter behavior
    it('should return the pushed document', function () {

      var res = createMockResponse();
      var req = createMockRequest({'user-agent':'test', 'origin':'test.origin'}, testDocPayload);
      var next = function () {
        throw new Error('Push handler should not be calling next()')
      };

      mockDatastore.updateDocumentByOperations = function() { return _prom.Promise.resolve(testDocPushResult); };

      return lowlaDb.push(req, res, next)
        .then(function (result) {
          should.exist(result);
          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var body = res.getBody();
          body.should.have.length(2);
          body[0].id.should.equal('lowladbtest.TestCollection$1234');
          body[0].version.should.equal(1);
          body[0].clientNs.should.equal('lowladbtest.TestCollection');
          body[1].a.should.equal(1);
          body[1].b.should.equal(2);
          body[1]._version.should.equal(1);
          res.headers.should.have.property('Cache-Control');
          res.headers.should.have.property('Content-Type');
          res.headers['Content-Type'].should.equal('application/json');
          return true;
        });
    });

    describe('Error Handling', function(){
      it('should handle error when promise-returning update function throws outside of promise', function () {
        var res = createMockResponse();
        var req = createMockRequest({'user-agent':'test', 'origin':'test.origin'}, testDocPayload);
        var next = function () {
          throw new Error('Push handler should not be calling next()')
        };
        mockDatastore.updateDocumentByOperations = sinon.stub().throws(Error('Some syntax error'));
        return lowlaDb.push(req, res, next)
          .then(function (result) {
            should.exist(result);
            should.exist(logger.logsByLevel['error']);
            var errs = util.inspect(logger.logsByLevel['error']);
            errs.should.contain('Some syntax error');
            var body = res.getBody();
            body[0].error.message.should.equal('Some syntax error');
            return true;
          });
      });

      it('should handle error when promise-returning update function throws inside of promise', function () {
        var res = createMockResponse();
        var req = createMockRequest({'user-agent':'test', 'origin':'test.origin'}, testDocPayload);
        var next = function () {
          throw new Error('Push handler should not be calling next()')
        };
        mockDatastore.updateDocumentByOperations = function() {
          return new _prom.Promise(function (resolve, reject) {
            foo();
            resolve(true);
          });
        };

        return lowlaDb.push(req, res, next)
          .then(function (result) {
            should.exist(result);
            should.exist(logger.logsByLevel['error']);
            var errs = util.inspect(logger.logsByLevel['error']);
            errs.should.contain('foo is not defined');
            var body = res.getBodyAsText();
            if(lowlaDb.config.sendDocumentLevelErrors) {
              body.should.contain("foo is not defined");
            }else{
              body.should.equal('[]');
            }
            return true;
          });
      });

      it('should handle errors from Datastore updateDocumentByOperations', function () {
        var res = createMockResponse();
        var req = createMockRequest({'user-agent':'test', 'origin':'test.origin'}, testDocPayload);
        var next = function () {
          throw new Error('Push handler should not be calling next()')
        };
        mockDatastore.updateDocumentByOperations = function() {
          return _prom.Promise.reject(Error('Error loading collection'));
        };

        return lowlaDb.push(req, res, next)
          .then(function (result) {
            should.exist(result);
            should.exist(logger.logsByLevel['error']);
            var errs = util.inspect(logger.logsByLevel['error']);
            errs.should.contain("Error loading collection");
            var body = res.getBodyAsText();
            if(lowlaDb.config.sendDocumentLevelErrors) {
              body.should.contain("Error loading collection");
            }else{
              body.should.equal('[]');
            }
            return true;
          });
      });

      it('should handle an update error for 1 of 3 pushed docs', function () {
        var payload = _.cloneDeep(testDocPayload);
        payload.documents.push(createPayloadDoc('1235', {a:77, b:88, $$$badfieldname: 'no'}));
        payload.documents.push(createPayloadDoc('1236', {a:55, b:66}));
        var res = createMockResponse();
        var req = createMockRequest({'user-agent':'test', 'origin':'test.origin'}, payload);
        var next = function () {
          throw new Error('Push handler should not be calling next()')
        };

        mockDatastore.updateDocumentByOperations = function(lowlaId) {
          switch (lowlaId) {
            case 'lowladbtest.TestCollection$1234': return _prom.Promise.resolve({ _id: '1234' });
            case 'lowladbtest.TestCollection$1235': return _prom.Promise.reject(Error('Invalid field $$badfieldname'));
            case 'lowladbtest.TestCollection$1236': return _prom.Promise.resolve({ _id: '1236' });
            default: throw Error('Unexpected lowlaId: ' + lowlaId);
          }
        };

        return lowlaDb.push(req, res, next)
          .then(function (result) {
            var body = res.getBody();
            should.exist(result);
            should.exist(logger.logsByLevel['error']);
            var errs = util.inspect(logger.logsByLevel['error'], { showHidden: true, depth: null });
            errs.should.contain('$$badfieldname');
            body.should.contain.a.thing.with.property('id', 'lowladbtest.TestCollection$1234');
            body.should.contain.a.thing.with.property('_id', '1234');
            body.should.contain.a.thing.with.property('id', 'lowladbtest.TestCollection$1234');
            body.should.contain.a.thing.with.property('_id', '1236');
            if(lowlaDb.config.sendDocumentLevelErrors) {
              body.should.contain.a.thing.with.property('error');
            }
            return true;
          });
      });
    });
  });

  describe('-PushWithPayload', function() {
    var syncNotifier;

    beforeEach(function() {
      syncNotifier = sinon.stub(lowlaDb.config, 'syncNotifier');
    });

    afterEach(function() {
      syncNotifier.restore();
    });

    it('should return the pushed document', function () {
      mockDatastore.updateDocumentByOperations = function() { return _prom.Promise.resolve(testDocPushResult); };
      var out = createOutputStream();
      return lowlaDb.pushWithPayload(testDocPayload, lowlaDb.createResultHandler(out))
        .then(function (result) {
          should.exist(result);
          syncNotifier.callCount.should.equal(1);
          syncNotifier.getCall(0).args[0].should.deep.equal({modified: [ { id: 'lowladbtest.TestCollection$1234', version: 1, clientNs: 'lowladbtest.TestCollection' }], deleted: [] });

          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output = out.getOutput();
          output.should.have.length(2);
          output[0].id.should.equal('lowladbtest.TestCollection$1234');
          output[0].version.should.equal(1);
          output[0].clientNs.should.equal('lowladbtest.TestCollection');
          output[1].a.should.equal(1);
          output[1].b.should.equal(2);
          output[1]._version.should.equal(1);
          return true;
        });
    });

    it('should update an existing doc', function() {
      var out = createOutputStream();
      var out2 = createOutputStream();
      sinon.stub(mockDatastore, 'updateDocumentByOperations')
        .onFirstCall().returns(_prom.Promise.resolve(testDocPushResult))
        .onSecondCall().returns(_prom.Promise.resolve({_id: '1234', a: 11, b: 22, _version: 2}));

      return lowlaDb.pushWithPayload(testDocPayload, lowlaDb.createResultHandler(out))
        .then(function(result) {
          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var newPayload = _.cloneDeep(testDocPayload);
          newPayload.documents[0].ops = { $set: { a: 11, b: 22 }};
          return lowlaDb.pushWithPayload(newPayload, lowlaDb.createResultHandler(out2));
        })
        .then(function(result) {
          should.exist(result);
          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output = out.getOutput();
          output.should.have.length(2);
          output[0].version.should.equal(1);
          output[1].a.should.equal(1);
          output[1].b.should.equal(2);
          var output2 = out2.getOutput();
          output2.should.have.length(2);
          output2[0].version.should.equal(2);
          output2[1].a.should.equal(11);
          output2[1].b.should.equal(22);
        })
    });

    it('should update an existing doc ignoring $SET _version if specified', function() {
      //clients shouldn't send a $set for version but in the event they do we should ignore and use the lowla metadata
      var out = createOutputStream();
      var out2 = createOutputStream();
      var stub = sinon.stub(mockDatastore, 'updateDocumentByOperations');
      stub.onFirstCall().returns(_prom.Promise.resolve(testDocPushResult))
        .onSecondCall().returns(_prom.Promise.resolve({ _id: '1234', a: 11, b:22, _version: 2 }));

      return lowlaDb.pushWithPayload(testDocPayload, lowlaDb.createResultHandler(out))
        .then(function(result) {
          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var newPayload = _.cloneDeep(testDocPayload);
          newPayload.documents[0].ops = { $set: { a: 11, b: 22, _version: 99 }};
          return lowlaDb.pushWithPayload(newPayload, lowlaDb.createResultHandler(out2));
        })
        .then(function(result) {
          // Make sure the second call to updateDocumentByOperations did not include _version:99
          stub.getCall(1).args[2].$set.should.not.have.property('_version');
          stub.getCall(1).args[2].$inc.should.have.property('_version');

          should.exist(result);
          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output = out.getOutput();
          output.should.have.length(2);
          output[0].version.should.equal(1);
          output[1].a.should.equal(1);
          output[1].b.should.equal(2);
          var output2 = out2.getOutput();
          output2.should.have.length(2);
          output2[0].should.not.have.property('error');
          output2[0].version.should.equal(2);
          output2[1].a.should.equal(11);
          output2[1].b.should.equal(22);
        })
    });


    it('should ignore client changes on conflicts by default', function() {
      var out = createOutputStream();
      var out2 = createOutputStream();
      var out3 = createOutputStream();
      var payload = _.cloneDeep(testDocPayload);
      var stub = sinon.stub(mockDatastore, 'updateDocumentByOperations');
      stub.onFirstCall().returns(_prom.Promise.resolve(testDocPushResult))
        .onSecondCall().returns(_prom.Promise.resolve({ _id: '1234', a: 11, b: 22, _version: 2}))
        .onThirdCall().returns(_prom.Promise.reject({isConflict: true }));
      mockDatastore.getDocument = function(id) {
        if ('lowladbtest.TestCollection$1234' === id) {
          return _prom.Promise.resolve({ _id: '1234', a: 11, b: 22, _version: 2});
        }
        return _prom.Promise.reject(Error('Unexpected lowlaId: ' + id));
      };

      return lowlaDb.pushWithPayload(payload, lowlaDb.createResultHandler(out))
        .then(function(result) {
          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output = out.getOutput();
          output.should.have.length(2);
          output[0].version.should.equal(1);
          output[1].a.should.equal(1);
          output[1].b.should.equal(2);
          var newPayload = _.cloneDeep(testDocPayload);
          newPayload.documents[0].ops = { $set: { a: 11, b: 22 }};
          newPayload.documents[0]._lowla.version=1;
          return lowlaDb.pushWithPayload(newPayload, lowlaDb.createResultHandler(out2));
        }).then(function(result) {
          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output2 = out2.getOutput();
          output2.should.have.length(2);
          output2[0].version.should.equal(2);
          output2[1].a.should.equal(11);
          output2[1].b.should.equal(22);
          var newPayload = _.cloneDeep(testDocPayload);
          newPayload.documents[0].ops = { $set: { a: 33, b: 66 }};
          newPayload.documents[0]._lowla.version=1;
          return lowlaDb.pushWithPayload(newPayload, lowlaDb.createResultHandler(out3));
        })
        .then(function(result) {
          should.exist(result);
          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');

          var output3 = out3.getOutput();
          output3.should.have.length(2);
          output3[0].version.should.equal(2);
          output3[1].a.should.equal(11);
          output3[1].b.should.equal(22);
        })
    });

    it('should allow conflict handlers to apply client changes', function() {
      lowlaDb.config.conflictHandler = function(docResolver) {
        docResolver.applyChanges();
      };

      var out = createOutputStream();
      var out2 = createOutputStream();
      var out3 = createOutputStream();
      var payload = _.cloneDeep(testDocPayload);
      var stub = sinon.stub(mockDatastore, 'updateDocumentByOperations');
      stub.onFirstCall().returns(_prom.Promise.resolve(testDocPushResult))
        .onSecondCall().returns(_prom.Promise.resolve({ _id: '1234', a: 11, b: 22, _version: 2}))
        .onThirdCall().returns(_prom.Promise.reject({isConflict: true }))
        .onCall(3).returns(_prom.Promise.resolve({_id: '1234', a: 33, b:66, _version: 3}));

      mockDatastore.getDocument = function(id) {
        if ('lowladbtest.TestCollection$1234' === id) {
          return _prom.Promise.resolve({ _id: '1234', a: 11, b: 22, _version: 2});
        }
        return _prom.Promise.reject(Error('Unexpected lowlaId: ' + id));
      };

      return lowlaDb.pushWithPayload(payload, lowlaDb.createResultHandler(out))
        .then(function(result) {
          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output = out.getOutput();
          output.should.have.length(2);
          output[0].version.should.equal(1);
          output[1].a.should.equal(1);
          output[1].b.should.equal(2);
          var newPayload = _.cloneDeep(testDocPayload);
          newPayload.documents[0].ops = { $set: { a: 11, b: 22 }};
          newPayload.documents[0]._lowla.version=1;
          return lowlaDb.pushWithPayload(newPayload, lowlaDb.createResultHandler(out2));
        }).then(function(result) {
          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output2 = out2.getOutput();
          output2.should.have.length(2);
          output2[0].version.should.equal(2);
          output2[1].a.should.equal(11);
          output2[1].b.should.equal(22);
          var newPayload = _.cloneDeep(testDocPayload);
          newPayload.documents[0].ops = { $set: { a: 33, b: 66 }};
          newPayload.documents[0]._lowla.version=1;
          return lowlaDb.pushWithPayload(newPayload, lowlaDb.createResultHandler(out3));
        })
        .then(function(result) {
          should.exist(result);
          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');

          var output3 = out3.getOutput();
          output3.should.have.length(2);
          output3[0].version.should.equal(3);
          output3[1].a.should.equal(33);
          output3[1].b.should.equal(66);

          stub.getCall(3).args.length.should.equal(4);
          stub.getCall(3).args[3].should.equal(true);
        })
    });

    it('should allow conflict handlers to provide different set ops', function() {
      lowlaDb.config.conflictHandler = function(docResolver, serverDoc, ops) {
        ops.$set['_conflict'] = true;
        docResolver.applyChanges(ops);
      };

      var out = createOutputStream();
      var out2 = createOutputStream();
      var out3 = createOutputStream();
      var payload = _.cloneDeep(testDocPayload);
      var stub = sinon.stub(mockDatastore, 'updateDocumentByOperations');
      stub.onFirstCall().returns(_prom.Promise.resolve(testDocPushResult))
        .onSecondCall().returns(_prom.Promise.resolve({ _id: '1234', a: 11, b: 22, _version: 2}))
        .onThirdCall().returns(_prom.Promise.reject({isConflict: true }))
        .onCall(3).returns(_prom.Promise.resolve({_id: '1234', a: 33, b:66, _conflict: true,  _version: 3}));

      mockDatastore.getDocument = function(id) {
        if ('lowladbtest.TestCollection$1234' === id) {
          return _prom.Promise.resolve({ _id: '1234', a: 11, b: 22, _version: 2});
        }
        return _prom.Promise.reject(Error('Unexpected lowlaId: ' + id));
      };

      return lowlaDb.pushWithPayload(payload, lowlaDb.createResultHandler(out))
        .then(function(result) {
          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output = out.getOutput();
          output.should.have.length(2);
          output[0].version.should.equal(1);
          output[1].a.should.equal(1);
          output[1].b.should.equal(2);
          var newPayload = _.cloneDeep(testDocPayload);
          newPayload.documents[0].ops = { $set: { a: 11, b: 22 }};
          newPayload.documents[0]._lowla.version=1;
          return lowlaDb.pushWithPayload(newPayload, lowlaDb.createResultHandler(out2));
        }).then(function(result) {
          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output2 = out2.getOutput();
          output2.should.have.length(2);
          output2[0].version.should.equal(2);
          output2[1].a.should.equal(11);
          output2[1].b.should.equal(22);
          var newPayload = _.cloneDeep(testDocPayload);
          newPayload.documents[0].ops = { $set: { a: 33, b: 66 }};
          newPayload.documents[0]._lowla.version=1;
          return lowlaDb.pushWithPayload(newPayload, lowlaDb.createResultHandler(out3));
        })
        .then(function(result) {
          should.exist(result);
          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');

          var output3 = out3.getOutput();
          output3.should.have.length(2);
          output3[0].version.should.equal(3);
          output3[1].a.should.equal(33);
          output3[1].b.should.equal(66);
          output3[1]._conflict.should.equal(true);

          stub.getCall(3).args.length.should.equal(4);
          stub.getCall(3).args[2].$set._conflict.should.equal(true);
          stub.getCall(3).args[3].should.equal(true);
        })
    });

    it('should send deletion by default when client edits a doc deleted on the server', function() {
      var out = createOutputStream();

      var updateDoc = sinon.stub(mockDatastore, 'updateDocumentByOperations');
      updateDoc.onFirstCall().returns(_prom.Promise.reject({isConflict: true}));
      var getDoc = sinon.stub(mockDatastore, 'getDocument');
      getDoc.onFirstCall().returns(_prom.Promise.resolve(null));

      var newPayload = _.cloneDeep(testDocPayload);
      newPayload.documents[0].ops = { $set: { a: 11, b: 22 }};
      newPayload.documents[0]._lowla.version = 99;

      return lowlaDb.pushWithPayload(newPayload, lowlaDb.createResultHandler(out))
        .then(function(result) {
          result.length.should.equal(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');

          var output = out.getOutput();
          output.should.have.length(1);
          output[0].version.should.equal(99);
          output[0].deleted.should.equal(true);
        });
    });


    it('should delete an existing doc', function() {
      var out = createOutputStream();
      var out2 = createOutputStream();
      var updateDoc = sinon.stub(mockDatastore, 'updateDocumentByOperations');
      updateDoc.onFirstCall().returns(_prom.Promise.resolve(testDocPushResult));
      var removeDoc = sinon.stub(mockDatastore, 'removeDocument');
      removeDoc.onFirstCall().returns(_prom.Promise.resolve(1));

      return lowlaDb.pushWithPayload(testDocPayload, lowlaDb.createResultHandler(out))
        .then(function(result) {
          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var newPayload = {
            documents: [ {
              _lowla: {
                id: 'lowladbtest.TestCollection$1234',
                version: 1,
                deleted: true
              }
            }]
          };
          return lowlaDb.pushWithPayload(newPayload,lowlaDb.createResultHandler(out2));
        })
        .then(function(result) {
          // Make sure removeDocument was actually called
          removeDoc.callCount.should.equal(1);
          removeDoc.getCall(0).args[0].should.equal('lowladbtest.TestCollection$1234');

          // Make sure update was only called once
          updateDoc.callCount.should.equal(1);

          syncNotifier.callCount.should.equal(2);
          syncNotifier.getCall(1).args[0].should.deep.equal({ modified:[], deleted: ['lowladbtest.TestCollection$1234']});
          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output = out.getOutput();
          output.should.have.length(2);
          output[0].version.should.equal(1);
          output[1].a.should.equal(1);
          output[1].b.should.equal(2);
          var output2 = out2.getOutput();
          output2.should.have.length(1);
          output2[0].version.should.equal(1);
          output2[0].deleted.should.equal(true);
        });
    });

    it('should ignore delete on conflicts by default', function() {
      var out = createOutputStream();
      var out2 = createOutputStream();
      var updateDoc = sinon.stub(mockDatastore, 'updateDocumentByOperations');
      updateDoc.onFirstCall().returns(_prom.Promise.resolve(testDocPushResult));
      var removeDoc = sinon.stub(mockDatastore, 'removeDocument');
      removeDoc.onFirstCall().returns(_prom.Promise.reject({isConflict: true}));
      var getDoc = sinon.stub(mockDatastore, 'getDocument');
      getDoc.onFirstCall().returns(_prom.Promise.resolve(testDocPushResult));

      return lowlaDb.pushWithPayload(testDocPayload, lowlaDb.createResultHandler(out))
        .then(function(result) {
          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var newPayload = {
            documents: [ {
              _lowla: {
                id: 'lowladbtest.TestCollection$1234',
                version: 2,
                deleted: true
              }
            }]
          };
          return lowlaDb.pushWithPayload(newPayload,lowlaDb.createResultHandler(out2));
        })
        .then(function(result) {
          // Make sure removeDocument was actually called
          removeDoc.callCount.should.equal(1);
          removeDoc.getCall(0).args[0].should.equal('lowladbtest.TestCollection$1234');

          // Make sure update was only called once
          updateDoc.callCount.should.equal(1);

          result.should.have.length(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output = out.getOutput();
          output.should.have.length(2);
          output[0].version.should.equal(1);
          output[1].a.should.equal(1);
          output[1].b.should.equal(2);

          // The remove conflicted on version number, so the result back to the client should have the current doc
          // contents
          var output2 = out2.getOutput();
          output2.should.have.length(2);
          output[0].version.should.equal(1);
          output[1].a.should.equal(1);
          output[1].b.should.equal(2);
        });
    });

    it('should attempt to translate special types', function() {
      var spy = sinon.spy(mockDatastore, 'decodeSpecialTypes');
      return lowlaDb.pushWithPayload(testDocPayload, lowlaDb.createResultHandler(createOutputStream()))
        .then(function() {
          spy.callCount.should.equal(1);
        });
    });
  });

  function makeAllDocsStub() {
    var args = Array.prototype.slice.call(arguments);
    if (1 === args.length && args[0] instanceof Array) {
      args = args[0];
    }

    return function(docHandler) {
      var promises = [];
      args.forEach(function(doc) {
        docHandler.write('lowladbtest.TestCollection$' + doc._id, doc._version || 1, false, doc);
        promises.push(_prom.Promise.resolve({ namespace: 'lowladbtest.TestCollection', sent: 1 }));
      });

      return _prom.Promise.all(promises);
    }
  }

  describe('Pull', function() {  //full route tests adapter.pull(req, res, next) -- see pullWithPayload tests for more detailed tests of adapter behavior

    it('should return a test document', function () {
      var newDoc = {_id: '1234', _version: 1, a: 1, b: 2 };
      mockDatastore.getAllDocuments = makeAllDocsStub(newDoc);

      var res = createMockResponse();
      var req = createMockRequest({'user-agent':'test', 'origin':'test.origin'});
      var next = function () {
        throw new Error('Push handler should not be calling next()')
      };
      return lowlaDb.pull(req, res, next)
        .then(function (result) {
          result.should.have.length.greaterThan(0);
          var body = res.getBody();
          body.should.have.length(2);
          body[0].id.should.equal('lowladbtest.TestCollection$1234');
          body[0].version.should.equal(1);
          body[0].clientNs.should.equal('lowladbtest.TestCollection');
          body[1].a.should.equal(1);
          body[1].b.should.equal(2);
          body[1]._version.should.equal(1);
          res.headers.should.have.property('Cache-Control');
          res.headers.should.have.property('Content-Type');
        })
    });


    describe('Error Handling', function() {

      it('should handle error when a promise-returning function throws outside of a promise', function () {
        var res = createMockResponse();
        var req = createMockRequest({'user-agent':'test', 'origin':'test.origin'});
        var next = function () {
          throw new Error('Push handler should not be calling next()');
        };
        sinon.stub(mockDatastore, 'getAllDocuments').throws(Error('Some syntax error'));
        return lowlaDb.pull(req, res, next)
          .then(function (result) {
            should.exist(result);
            should.exist(logger.logsByLevel['error']);
            var errs = util.inspect(logger.logsByLevel['error']);
            errs.should.contain('Some syntax error');
            var body = res.getBody();
            body[0].error.message.should.equal('Some syntax error');
            return true;
          })
      });


      it('should handle error when promise-returning update function throws inside of promise', function () {
        var res = createMockResponse();
        var req = createMockRequest({'user-agent':'test', 'origin':'test.origin'});
        var next = function () {
          throw new Error('Push handler should not be calling next()')
        };
        sinon.stub(mockDatastore, 'getAllDocuments',
          function(){return new _prom.Promise(function(resolve, reject){foo(); resolve(true);});});
        return lowlaDb.pull(req, res, next)
          .then(function (result) {
            should.exist(result);
            should.exist(logger.logsByLevel['error']);
            var errs = util.inspect(logger.logsByLevel['error']);
            errs.should.contain('foo is not defined');
            var body = res.getBody();
            body[0].error.message.should.equal('foo is not defined');
            return true;
          })
      });

      it('should handle an error thrown inside getAllDocuments', function () {
        var res = createMockResponse();
        var req = createMockRequest({});
        var next = function () {
          throw new Error('Push handler should not be calling next()')
        };
        sinon.stub(mockDatastore, 'getAllDocuments').throws(Error("Error finding in collection"));
        return lowlaDb.pull(req, res, next)
          .then(function (result) {
            should.exist(result);
            should.exist(logger.logsByLevel['error']);
            var errs = util.inspect(logger.logsByLevel['error']);
            errs.should.contain('Error finding in collection');
            var body = res.getBody();
            body[0].error.message.should.equal('Error finding in collection');
            return true;
          })
      });

      it('should handle an error thrown inside getDocuments', function () {
        var payload = {ids:['lowladbtest.TestCollection$1234']};
        var res = createMockResponse();
        var req = createMockRequest({'user-agent':'test', 'origin':'test.origin'}, payload);
        var next = function () {
          throw new Error('Push handler should not be calling next()')
        };

        sinon.stub(mockDatastore, 'getDocument').throws(Error("Error get document"));
        return lowlaDb.pull(req, res, next)
          .then(function (result) {
            should.exist(result);
            should.exist(logger.logsByLevel['error']);
            var errs = util.inspect(logger.logsByLevel['error']);
            errs.should.contain('Error get document');
            var body = res.getBody();
            body[0].error.message.should.equal('Error get document');
            return true;
          })
      });


    });

  });

  describe('-PullWithPayload', function() {

    it('should return a test document', function () {
      var newDoc = {_id:'1234', _version:1, a:1, b:2 };
      mockDatastore.getAllDocuments = makeAllDocsStub(newDoc);

      var out = createOutputStream();
      return lowlaDb.pullWithPayload(null, lowlaDb.createResultHandler(out))
        .then(function (result) {
          result.should.have.length.greaterThan(0);
          var output = out.getOutput();
          output.should.have.length(2);
          output[0].id.should.equal('lowladbtest.TestCollection$1234');
          output[0].version.should.equal(1);
          output[0].clientNs.should.equal('lowladbtest.TestCollection');
          output[1].a.should.equal(1);
          output[1].b.should.equal(2);
          output[1]._version.should.equal(1);
        });
    });

    it('should return several test documents', function () {
      var docs=[];
      for(var i=1; i<=10; i++){
        docs.push({_id:'1234'+i, _version:i, a:1000+i, b:2000+i });
      }
      mockDatastore.getAllDocuments = makeAllDocsStub(docs);

      var out = createOutputStream();
      return lowlaDb.pullWithPayload(null, lowlaDb.createResultHandler(out))
        .then(function (result) {
          should.exist(result);
          result.should.have.length.greaterThan(0);
          var output = out.getOutput();
          output.should.have.length(20);
          var j=1;
          for(i=0; i<20; i+=2) {
            output[i].id.should.equal('lowladbtest.TestCollection$1234'+j);
            output[i].version.should.equal(j);
            output[i].clientNs.should.equal('lowladbtest.TestCollection');
            output[i+1].a.should.equal(1000+j);
            output[i+1].b.should.equal(2000+j);
            output[i+1]._version.should.equal(j);
            j++;
          }
        });
    });

    it('should return requested documents', function () {
      var docs={};
      var payload = {};
      payload.ids = [];
      for(var i=0; i<=9; i++){
        docs['lowladbtest.TestCollection$1234'+i] = {_id:'1234'+i, _version:i, a:1000+i, b:2000+i, testKey:i};
        if(0==i % 2){
          payload.ids.push('lowladbtest.TestCollection$1234'+i);
        }
      }
      mockDatastore.getDocument = function(id) {
        return _prom.Promise.resolve(docs[id]);
      };

      var out = createOutputStream();
      return lowlaDb.pullWithPayload(payload, lowlaDb.createResultHandler(out))
        .then(function (result) {
          result.should.have.length(5);

          var output = out.getOutput();
          output.should.have.length(10);
          var testKey;
          for(i=0; i<10; i+=2) {
            testKey = output[i+1].testKey;  //version
            output[i].id.should.equal('lowladbtest.TestCollection$1234'+testKey);
            payload.ids.should.include(output[i].id);   //was it requested?
            output[i].version.should.equal(testKey);
            output[i].clientNs.should.equal('lowladbtest.TestCollection');
            output[i+1].a.should.equal(1000+testKey);
            output[i+1].b.should.equal(2000+testKey);
            output[i+1]._version.should.equal(testKey);
          }
        });
    });

    it('should attempt to translate special types', function() {
      var out = createOutputStream();
      var newDoc = { _id: '1234', a: 1, _version:1 };
      mockDatastore.getAllDocuments = makeAllDocsStub(newDoc);
      var spy = sinon.spy(mockDatastore, 'encodeSpecialTypes');
      return lowlaDb.pullWithPayload(null, lowlaDb.createResultHandler(out))
        .then(function() {
          spy.callCount.should.equal(1);
        });
    });
  });

  describe('internals', function(){

    it('should create a working ResultHandler', function(){
      var out = createOutputStream();
      var rh = lowlaDb.createResultHandler(out);
      should.exist(rh);
      rh.should.respondTo('start');
      rh.should.respondTo('writeError');
      rh.should.respondTo('write');
      rh.should.respondTo('end');
      var lowlaId = testUtil.createLowlaId('fooDb', 'fooColl', '123');
      var doc = {a:1, b:2};
      var err = new Error("something went wrong");
      rh.start();
      rh.write(lowlaId, 3, true);
      lowlaId = testUtil.createLowlaId('fooDb', 'fooColl', '124');
      rh.writeError(err, lowlaId);
      lowlaId = testUtil.createLowlaId('fooDb', 'fooColl', '125');
      rh.write(lowlaId, 2, false, doc);
      rh.end();
      var output = out.getOutput();

      output.should.have.length(4);
      output[0].id.should.equal('fooDb.fooColl$123');
      output[0].version.should.equal(3);
      output[0].clientNs.should.equal('fooDb.fooColl');
      output[1].error.id.should.equal('fooDb.fooColl$124');
      output[1].error.message.should.equal('something went wrong');
      output[2].id.should.equal('fooDb.fooColl$125');
      output[2].version.should.equal(2);
      output[2].clientNs.should.equal('fooDb.fooColl');
      output[3].a.should.equal(1);
      output[3].b.should.equal(2);
    });
  });


  //util

  var createPayloadDoc = function(id, vals){
    return {
      _lowla: {
        id: 'lowladbtest.TestCollection$' + id
      },
      ops: {
        $set: vals
      }
    }
  };

  var createOutputStream = function(){
    var out = '';
    var Writable = require('stream').Writable;
    var outStream = Writable();
    outStream._write = function (chunk, enc, next) {
      out += chunk;
      next();
    };
    outStream.getOutput = function(){return(JSON.parse(out)); };
    outStream.getOutputAsText = function(){return out; };
    return outStream;
  };

  var createMockResponse = function(){
    var mockResponse = createOutputStream();
    mockResponse.headers = {};
    mockResponse.getBody = mockResponse.getOutput;
    mockResponse.getBodyAsText = mockResponse.getOutputAsText;
    mockResponse.setHeader = function(header, value){this.headers[header] = value;};
    return mockResponse;
  };

  var createMockRequest = function(headers, body){
    return {
      headers:headers,
      body:body,
      get:function(name){
        return headers[name];
      }
    }
  }

});
