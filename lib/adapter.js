(function(exports) {
  var _prom = require('./promiseImpl');
  var _ = require('lodash');
  var Datastore = require('./datastore/datastore').Datastore;
  var LowlaId = require('./datastore/lowlaId.js').LowlaId;

  var defaultOptions = {
    datastore:false,
    syncNotifier: function(payload){},
    conflictHandler: function(docSent, docCurrent, resolve){
      this.logger.info("Conflict occurred:");
      this.logger.info("Server document, will be kept: ",docCurrent);
      this.logger.info("Conflict from client, will be discarded: ", docSent);
      //call resolve w/ no param (falsey param) will keep server doc unchanged;
      //call resolve(doc) will write whatever doc is passed and return it to the client //TODO...
      resolve();
    }
  };

  var LowlaAdapter = function(options) {
    var ready = _prom.defer();
    this.ready = ready.promise;
    var config = this.config = _.extend({}, defaultOptions, options);
    if(!config.logger){
      config.logger = console;
      }
    this.logger = config.logger;
    if (!config.datastore) {
      var datastore = config.datastore = new Datastore({mongoUrl:config.mongoUrl, logger:config.logger});
      this.logger.log("Adapter is waiting for Datastore...");
      datastore.ready.then(function(){
        ready.resolve();
      }).done();
      }
  };

  LowlaAdapter.prototype.setConflictHandler = function(conflictHandler){
    this.config.conflictHandler = conflictHandler;
  };

  LowlaAdapter.prototype.configureRoutes = function(app) {
    var adapter = this;
    app.get('/_lowla/pull', function(req, res, next){adapter.pull(req, res, next).done();});
    app.post('/_lowla/pull', function(req, res, next){adapter.pull(req, res, next).done();});
    app.post('/_lowla/push', function(req, res, next){adapter.push(req, res, next).done();});
  };

  LowlaAdapter.prototype.pull = function(req, res, next) {
    var adapter = this;
    res.setHeader('Cache-Control', 'no-cache, private, no-store, must-revalidate, max-stale=0, post-check=0, pre-check=0');
    res.setHeader("Content-Type", "application/json");
    var resultHandler = adapter.createResultHandler(res);

    return adapter.pullWithPayload(req.body, resultHandler ).then(function(results) {
      adapter.logger.info("Pulled: " + JSON.stringify(results) );
      return results;
    }, function(err) {
      adapter.logger.error(err);
      resultHandler.writeError(err);
      resultHandler.end();
      res.end();
      return err;
    });
  };

  LowlaAdapter.prototype.push = function(req, res, next) {
    var adapter = this;
    res.setHeader('Cache-Control', 'no-cache, private, no-store, must-revalidate, max-stale=0, post-check=0, pre-check=0');
    res.setHeader("Content-Type", "application/json");
    var resultHandler = adapter.createResultHandler(res);

    return adapter.pushWithPayload(req.body, resultHandler).then(function(results) {
      adapter.logger.info("Pushed: " + JSON.stringify(results) );
      return results;
    }, function(err) {
      adapter.logger.error(err);
      resultHandler.writeError(err);
      resultHandler.end();
      res.end();
      return err;
    });
  };

  LowlaAdapter.prototype.createResultHandler = function(outputStream){
    //returns a function that knows how to write documents w/meta data in LowlaDb format
    //writes to the provided output stream held in closure.
    var started = false;
    var ended = false;
    var adapter = this;
    return {
      start: function(){outputStream.write('[');},
      writeError: function (err, lowlaId){
        if(ended){adapter.logger.warn("Attempt to write error to resultHandler after end", err); return;};
        var id;
        if( lowlaId ){
          id = lowlaId.getClientId();
        }
        outputStream.write( (started ? ', ' : '') + JSON.stringify({error:{id:id, message:err.message}}));
      },
      write: function (lowlaId, version, deleted, doc) {
        if(ended){adapter.logger.warn("Attempt to write to resultHandler after end", result); return;};
        var meta = {
          clientNs: lowlaId.getClientNs(),
          id: lowlaId.getClientId(),
          deleted: deleted,
          version: version
        };
        if(!deleted) {
          adapter.config.datastore.encodeSpecialTypes(doc);
        }
        outputStream.write( (started ? ', ' : '') + JSON.stringify(meta) + ( deleted ? '' : ',' + JSON.stringify(doc) ) );
        started = true;
      },
      end: function(){if(!ended){ended=true; outputStream.end(']');};}
    }
  };

  LowlaAdapter.prototype.pullWithPayload = function(payload, resultHandler) {
    var adapter = this;
    var _config = adapter.config;
    var promises = [];
    if (!payload || !payload.ids) {
      resultHandler.start();
      return adapter.config.datastore.getAllDocuments(resultHandler)
        .then(function(result){
          resultHandler.end();
          return result;
        });
    }
    else {
      resultHandler.start();
      payload.ids.forEach(function (docId) {
        var lowlaId = new LowlaId(docId);
        promises.push(
          adapter.config.datastore.getDocument(lowlaId)
            .then(function(doc){
              resultHandler.write(lowlaId, doc._version, false, doc);
              return lowlaId.getClientId();  //TODO what do we want to return to the result handler?
            }, function(err){
              if(err.isDeleted){
                resultHandler.write(lowlaId, null, true);
              }
            })
        );

      });
      return _prom.all(promises).then(function(result){
        resultHandler.end();
        return result;
      });
    }
  };

  LowlaAdapter.prototype.pushWithPayload = function(payload, resultHandler) {
    var adapter = this;
    var config = this.config;

    return _prom.Promise.resolve().then(function(){

      resultHandler.start();


      if (!payload || !payload.documents) {
        throw new Error("No payload or documents");
        return;
      }

      var promises = [];
      var result = [];
      var syncPayload = {modified: [], deleted: []};

      payload.documents.forEach(function (doc) {
        if (!doc._lowla) {
          return;
        }

        var versionPreUpdate = doc._lowla.version;
        var lowlaId = new LowlaId(doc._lowla.id);

        if (doc._lowla.deleted) {
          promises.push(
            adapter.config.datastore.removeDocument(lowlaId, versionPreUpdate)
              .then(function (success) {
                syncPayload.deleted.push(lowlaId.getClientId());
                resultHandler.write(lowlaId, versionPreUpdate, true);
                return "Deleted: " + lowlaId.getClientId();
              }, function(err){
                adapter.logger.error(err);
                resultHandler.writeError(err, lowlaId);
                throw new Error("Document delete failed for " + lowlaId.getClientId());
              })
          );
        }
        else {

          if (doc.ops['$inc']) {
            doc.ops['$inc']._version = 1;
          }
          else {
            doc.ops['$inc'] = {_version: 1};
          }
          if (doc.ops['$set']._version) {
            delete doc.ops['$set']._version
          }
          doc = adapter.config.datastore.decodeSpecialTypes(doc);

          promises.push(
            adapter.config.datastore.updateDocumentByOperations(lowlaId, versionPreUpdate, doc.ops)
              .then(null, function(err){
                if(err.isConflict) {
                  return adapter.handleConflict(lowlaId, versionPreUpdate, doc.ops)
                }
                adapter.logger.error(err);
                resultHandler.writeError(err, lowlaId);
                throw new Error("Document update failed for " + lowlaId.getClientId());
              })
              .then(function (docUpdated) {
                syncPayload.modified.push({
                  id: lowlaId.getClientId(),
                  version: docUpdated._version,
                  clientNs: lowlaId.getClientNs()
                });

                resultHandler.write(lowlaId, docUpdated._version, false, docUpdated);

                return lowlaId.getClientId();
              })
          );
        }
      });

      return _prom.settle(promises).then(function(result){
        config.syncNotifier(syncPayload);
        for(r in result){
          if(result[r].state === 'fulfilled'){
            result[r] = result[r].value;
          }else{
            result[r] = result[r].reason;
            adapter.logger.error("Document not written: ");
          }
        }
        resultHandler.end();
        return result;
      })

    });
  };

  LowlaAdapter.prototype.handleConflict = function(lowlaId, versionPreUpdate, ops){
    var adapter = this;
    return _prom.promise.resolve().then(function(){
      if (adapter.config.conflictHandler) {
        return adapter.config.datastore.getDocument(lowlaId).then(function (docServerVersion) {
          //todo deleted conflicts!
          var resolveFunc, rejectFunc;
          var conflictPromise = _prom.Promise(function (resolve, reject) {
            resolveFunc = resolve;
            rejectFunc = reject;
          });
          var ConflictResolver = function () {
            return function (resolutionDoc) {
              if (resolutionDoc) {
                //save and return the resolution doc
                //TODO!
                resolveFunc(resolutionDoc);
              } else {
                resolveFunc(docServerVersion);
              }
            }
          };
          try {
            adapter.config.conflictHandler(ops, docServerVersion, new ConflictResolver());
          } catch (err) {
            rejectFunc(err);
          }
          return conflictPromise.then(function (doc) {
            if (! doc ){
              throw {isDeleted:true}
            }
            return doc;
          });
        });
      }
    })
  };

  exports.LowlaAdapter = LowlaAdapter;
})(module.exports);
