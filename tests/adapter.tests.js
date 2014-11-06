var should = require('chai').should();
var _ = require('lodash');
var _prom = require('../lib/promiseImpl.js');
var LowlaAdapter = require('../lib/adapter.js').LowlaAdapter;
var util = require('./testUtil');
var Binary = require('mongodb').Binary;

util.enableLongStackSupport();

describe('LowlaAdapter', function() {
  var lowlaDb;
  var testColl;
  var _db;

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

  var logs = {};
  var logF = function(level){
    return function(){
      if(!logs[level]){
        logs[level] = [];
      }
      logs[level].push(arguments);
    };
  };

  before(function(done){
    lowlaDb = new LowlaAdapter({mongoUrl: 'mongodb://127.0.0.1/lowladbtest', logger:{log:logF('log'), debug:logF('debug'), info:logF('info'), warn:logF('warn'), error:logF('error')} });
    done();
  });

  beforeEach(function(done) {
    // TODO - use environment var to define test DB?
    // Clear logs
    logs = {};
    // Clear out the test collection before each test
    return lowlaDb.ready.then(function(){
      _db = lowlaDb.config.datastore.config.db;
      return util.mongo.removeAllCollections(_db);
    }).then(function(ret){
      return util.mongo.getCollection(_db, "TestCollection");
    }).done(function(testCollection){
      testColl = testCollection;
      done();
    });
  });

  describe('push', function() {   //full route tests adapter.push(req, res, next) -- see pushWithPayload tests for more detailed tests

    it('Should return the pushed document', function () {

      var res = createMockResponse();
      var req = createMockRequest([], testDocPayload);
      var next = function () {
        throw new Error('Push handler shouldn\'t be calling next()')
      }

      return lowlaDb.push(req, res, next)
        .then(function (result) {
          should.exist(result);
          result.length.should.equal(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var body = res.getBody();
          body.should.have.length.of(2);
          body[0].id.should.equal('lowladbtest.TestCollection$1234');
          body[0].version.should.equal(1);
          body[0].clientNs.should.equal('lowladbtest.TestCollection');
          body[1].a.should.equal(1);
          body[1].b.should.equal(2);
          body[1]._version.should.equal(1);
          res.headers.should.have.property('Cache-Control');
          res.headers.should.have.property('Content-Type');
          return true;
        });
    });

    it('Should return an error for error thrown after stream is open', function () {  //programmer error

      var res = createMockResponse();
      var req = createMockRequest([], testDocPayload);
      var next = function () {
        throw new Error('Push handler shouldn\'t be calling next()')
      }
      var ds = lowlaDb.config.datastore;
      var dsFunc = ds.updateDocumentByOperations;
      ds.updateDocumentByOperations = function(){return new _prom.promise(function(resolve, reject){foo(); resolve(true);} ) };
      return lowlaDb.push(req, res, next)
        .then(function (result) {
          should.exist(result);
          result.should.be.error;
          should.exist(logs['error']);
          logs.error[0].should.be.error;

          var body = res.getBody();
          body[0].error.message.should.equal('foo is not defined');

          return true;
        }).finally(function(){
          ds.updateDocumentByOperations = dsFunc;
        });
    });

  });

  describe('pushWithPayload', function() {

    it('should return the pushed document', function () {

      var out = createOutputStream();

      return lowlaDb.pushWithPayload(testDocPayload, lowlaDb.createResultHandler(out))
        .then(function (result) {
          should.exist(result);
          result.length.should.equal(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output = out.getOutput();
          output.should.have.length.of(2);
          output[0].id.should.equal('lowladbtest.TestCollection$1234');
          output[0].version.should.equal(1);
          output[0].clientNs.should.equal('lowladbtest.TestCollection');
          output[1].a.should.equal(1);
          output[1].b.should.equal(2);
          output[1]._version.should.equal(1);
          return true;
        });
    });

    it('should create the document in MongoDB', function(done) {
      var out = createOutputStream();
      lowlaDb.pushWithPayload(testDocPayload, lowlaDb.createResultHandler(out))
        .then(function(result) {
          result.length.should.equal(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          testColl.find().toArray(function(err, docs) {
            should.not.exist(err);
            should.exist(docs);
            docs.length.should.equal(1);
            docs[0]._id.should.equal('1234');
            docs[0].a.should.equal(1);
            docs[0].b.should.equal(2);
            return done();
          });
        }).done();
    });

    it('should update an existing doc', function() {
      var out = createOutputStream();
      var out2 = createOutputStream();
      return lowlaDb.pushWithPayload(testDocPayload, lowlaDb.createResultHandler(out))
        .then(function(result) {
          result.length.should.equal(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var newPayload = _.cloneDeep(testDocPayload);
          newPayload.documents[0].ops = { $set: { a: 11, b: 22 }};
          return lowlaDb.pushWithPayload(newPayload, lowlaDb.createResultHandler(out2));
        })
        .then(function(result) {
          should.exist(result);
          result.length.should.equal(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output = out.getOutput();
          output.should.have.length.of(2);
          output[0].version.should.equal(1);
          output[1].a.should.equal(1);
          output[1].b.should.equal(2);
          var output2 = out2.getOutput();
          output2.should.have.length.of(2);
          output2[0].version.should.equal(2);
          output2[1].a.should.equal(11);
          output2[1].b.should.equal(22);
        })
    });

    it('should update an existing doc ignoring $SET _version if specified', function() {
      //clients shouldn't send a $set for version but in the event they do we should ignore and use the lowla metadata
      var out = createOutputStream();
      var out2 = createOutputStream();
      return lowlaDb.pushWithPayload(testDocPayload, lowlaDb.createResultHandler(out))
        .then(function(result) {
          result.length.should.equal(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var newPayload = _.cloneDeep(testDocPayload);
          newPayload.documents[0].ops = { $set: { a: 11, b: 22, _version: 99 }};
          return lowlaDb.pushWithPayload(newPayload, lowlaDb.createResultHandler(out2));
        })
        .then(function(result) {
          should.exist(result);
          result.length.should.equal(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output = out.getOutput();
          output.should.have.length.of(2);
          output[0].version.should.equal(1);
          output[1].a.should.equal(1);
          output[1].b.should.equal(2);
          var output2 = out2.getOutput();
          output2.should.have.length.of(2);
          output2[0].should.not.have.property('error');
          output2[0].version.should.equal(2);
          output2[1].a.should.equal(11);
          output2[1].b.should.equal(22);
        })
    });


    it('server should win on conflict', function() {
      var out = createOutputStream();
      var out2 = createOutputStream();
      var out3 = createOutputStream();
      var payload = _.cloneDeep(testDocPayload);
      return lowlaDb.pushWithPayload(payload, lowlaDb.createResultHandler(out))
        .then(function(result) {
          result.length.should.equal(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output = out.getOutput();
          output.should.have.length.of(2);
          output[0].version.should.equal(1);
          output[1].a.should.equal(1);
          output[1].b.should.equal(2);
          var newPayload = _.cloneDeep(testDocPayload);
          newPayload.documents[0].ops = { $set: { a: 11, b: 22 }};
          return lowlaDb.pushWithPayload(newPayload, lowlaDb.createResultHandler(out2));
        }).then(function(result) {
          result.length.should.equal(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output2 = out2.getOutput();
          output2.should.have.length.of(2);
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
          result.length.should.equal(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');

          var output3 = out3.getOutput();
          output3.should.have.length.of(2);
          output3[0].version.should.equal(2);
          output3[1].a.should.equal(11);
          output3[1].b.should.equal(22);
        })
    });

    it('uses a custom conflict handler', function() {
      var out = createOutputStream();
      var out2 = createOutputStream();
      var out3 = createOutputStream();
      var payload = _.cloneDeep(testDocPayload);
      lowlaDb.setConflictHandler(function(docSent, docCurrent, resolve){
        console.log("Conflict occurred:");
        console.log("Server document, will be kept: ",docCurrent);
        console.log("Conflict from client, will be discarded: ", docSent);
        return util.readFile('test.png').then(function(filedata){
          console.log('read ' + filedata.length + ' bytes from the file system...');
        }).then(function(){
          console.log("handler resolving...");
          resolve();
        });
      });
      return lowlaDb.pushWithPayload(payload, lowlaDb.createResultHandler(out))
        .then(function(result) {
          result.length.should.equal(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output = out.getOutput();
          output.should.have.length.of(2);
          output[0].version.should.equal(1);
          output[1].a.should.equal(1);
          output[1].b.should.equal(2);
          var newPayload = _.cloneDeep(testDocPayload);
          newPayload.documents[0].ops = { $set: { a: 11, b: 22 }};
          return lowlaDb.pushWithPayload(newPayload, lowlaDb.createResultHandler(out2));
        }).then(function(result) {
          result.length.should.equal(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output2 = out2.getOutput();
          output2.should.have.length.of(2);
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
          result.length.should.equal(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');

          var output3 = out3.getOutput();
          output3.should.have.length.of(2);
          output3[0].version.should.equal(2);
          output3[1].a.should.equal(11);
          output3[1].b.should.equal(22);
        }).catch(function(err){
          throw err;
        })
    });

    it('should delete an existing doc', function(done) {
      var out = createOutputStream();
      var out2 = createOutputStream();
      lowlaDb.pushWithPayload(testDocPayload, lowlaDb.createResultHandler(out))
        .then(function(result) {
          result.length.should.equal(1);
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
          result.length.should.equal(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output = out.getOutput();
          output.should.have.length.of(2);
          output[0].version.should.equal(1);
          output[1].a.should.equal(1);
          output[1].b.should.equal(2);
          var output2 = out2.getOutput();
          output2.should.have.length.of(1);
          output2[0].version.should.equal(1);
          output2[0].deleted.should.be.true;
          testColl.find({_id: '1234'}).toArray(function(err, res) {
            should.not.exist(err);
            should.exist(res);
            res.should.have.length.of(0);
            done();
          })
        })
        .catch(done);
    });

    it('should push a date', function () {

      var msDate = 132215400000;
      var out = createOutputStream();

      var payload = _.cloneDeep(testDocPayload);
      payload.documents[0].ops.$set.date = {_bsonType: 'Date', millis: msDate };

      return lowlaDb.pushWithPayload(payload, lowlaDb.createResultHandler(out))
        .then(function (result) {
          should.exist(result);
          result.length.should.equal(1);
          result[0].should.equal('lowladbtest.TestCollection$1234');
          var output = out.getOutput();
          var date = output[1].date;
          date.should.not.be.instanceOf(Date);
          date.should.have.property('_bsonType');
          date.should.have.property('millis');
          date._bsonType.should.equal('Date');
          date.millis.should.equal(msDate);
          return true;
        }).then(function(){
          testColl.find().toArray(function(err, docs) {
            should.not.exist(err);
            should.exist(docs);
            docs.length.should.equal(1);
            docs[0].date.getTime().should.equal(msDate);
            return true;
          });
        });
    });


    it('should push a binary', function () {
      var out = createOutputStream();
      var payload = _.cloneDeep(testDocPayload);
      var bin;
      return util.readFile('test.png').then(function(filedata) {
        bin = new Binary(filedata);
        payload.documents[0].ops.$set.bin = {_bsonType: 'Binary', encoded: bin.toString('base64') };
      }).then(function(){
        return lowlaDb.pushWithPayload(payload, lowlaDb.createResultHandler(out))
          .then(function (result) {
            should.exist(result);
            result.length.should.equal(1);
            result[0].should.equal('lowladbtest.TestCollection$1234');
            var output = out.getOutput();
            var bin2 = output[1].bin;
            bin2.should.not.be.instanceOf(Date);
            bin2.should.have.property('_bsonType');
            bin2.should.have.property('encoded');
            bin2._bsonType.should.equal('Binary');
            bin2.encoded.should.equal(bin.toString('base64'));
            return true;
          }).then(function(){
            testColl.find().toArray(function(err, docs) {
              should.not.exist(err);
              should.exist(docs);
              docs.length.should.equal(1);
              docs[0].bin.toString('base64').should.equal(bin.toString('base64'));
              return true;
            });
          });
      });
    });

  });

  describe('pull', function() {  //full route tests adapter.pull(req, res, next) -- see pullWithPayload tests for more detailed tests

    it('should return a test document', function () {
      var newDoc = {_id: '1234', _version: 1, a: 1, b: 2 };
      return util.mongo.insertDocs(_db, "TestCollection", newDoc)
        .then(function () {
          var res = createMockResponse();
          var req = createMockRequest([]);
          var next = function () {
            throw new Error('Push handler shouldn\'t be calling next()')
          }
          return lowlaDb.pull(req, res, next)
            .then(function (result) {
              result.should.have.length.greaterThan(0);
              var body = res.getBody();
              body.should.have.length.of(2)
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
    });

  });

  describe('pullWithPayload', function() {

    it('should return a test document', function () {
      var newDoc = {_id:'1234', _version:1, a:1, b:2 };
      var out = createOutputStream();
      return util.mongo.insertDocs(_db, "TestCollection", newDoc)
        .then(function(){
          return lowlaDb.pullWithPayload(null, lowlaDb.createResultHandler(out))
            .then(function (result) {
              result.should.have.length.greaterThan(0);
              var output = out.getOutput();
              output.should.have.length.of(2)
              output[0].id.should.equal('lowladbtest.TestCollection$1234');
              output[0].version.should.equal(1);
              output[0].clientNs.should.equal('lowladbtest.TestCollection');
              output[1].a.should.equal(1);
              output[1].b.should.equal(2);
              output[1]._version.should.equal(1);
            })
        });
    });

    it('should return several test documents', function () {
      var docs=[];
      for(i=0; i<=9; i++){
        docs.push({_id:'1234'+i, _version:i, a:1000+i, b:2000+i });
      }
      var out = createOutputStream();
      return util.mongo.insertDocs(_db, "TestCollection", docs)
        .then(function(){
          return lowlaDb.pullWithPayload(null, lowlaDb.createResultHandler(out))
            .then(function (result) {
              should.exist(result);
              result.should.have.length.greaterThan(0);
              var output = out.getOutput();
              output.should.have.length.of(20);
              var j=0;
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

    });

    it('should return requested documents', function () {
      var docs=[];
      var payload = {};
      payload.ids = [];
      for(i=0; i<=9; i++){
        docs.push({_id:'1234'+i, _version:i, a:1000+i, b:2000+i, testKey:i});
        if(0==i % 2){
          payload.ids.push('lowladbtest.TestCollection$1234'+i);
        }
      }
      var out = createOutputStream();
      return util.mongo.insertDocs(_db, "TestCollection", docs)
        .then(function(){
          return lowlaDb.pullWithPayload(payload, lowlaDb.createResultHandler(out))
            .then(function (result) {
              result.length.should.equal(5);

              var output = out.getOutput();
              output.should.have.length.of(10);
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
    });

    it("should pull a date", function(){
      var msDate = 132215400000;
      var newDoc = { _id: '1234', a: 1, _version:1, date: new Date(msDate) };
      var out = createOutputStream();
      return util.mongo.insertDocs(_db, "TestCollection", newDoc)
        .then(function(){
          return lowlaDb.pullWithPayload(null, lowlaDb.createResultHandler(out))
            .then(function (result) {
              result.should.have.length.greaterThan(0);
              var output = out.getOutput();
              var date = output[1].date;
              date.should.not.be.instanceOf(Date);
              date.should.have.property('_bsonType');
              date.should.have.property('millis');
              date._bsonType.should.equal('Date');
              date.millis.should.equal(msDate);
            });
        });
    });

    it('should pull a binary', function () {
      var out = createOutputStream();
      var bin;
      var doc = { _id: '1234', a: 1, _version: 1};
      return util.readFile('test.png').then(function (filedata) {
        bin = new Binary(filedata);
        doc.bin = bin;
      }).then(function () {
        return util.mongo.insertDocs(_db, "TestCollection", doc)
          .then(function () {
            return lowlaDb.pullWithPayload(null, lowlaDb.createResultHandler(out))
              .then(function (result) {
                result.should.have.length.greaterThan(0);
                var output = out.getOutput();
                var bin2 = output[1].bin;
                bin2.should.not.be.instanceOf(Date);
                bin2.should.have.property('_bsonType');
                bin2.should.have.property('encoded');
                bin2._bsonType.should.equal('Binary');
                bin2.encoded.should.equal(bin.toString('base64'));
              });
          });
      });
    });

  });

  //util

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
  }

  var createMockResponse = function(){
    var mockResponse = createOutputStream();
    mockResponse.headers = {};
    mockResponse.getBody = mockResponse.getOutput;
    mockResponse.getBodyAsText = mockResponse.getOutputAsText;
    mockResponse.setHeader = function(header, value){this.headers[header] = value;};
    return mockResponse;
  }

  var createMockRequest = function(headers, body){
    return {
      headers:headers,
      body:body
    }
  }

});
