(function(exports) {
  var _prom = require('./promiseImpl');
  var _ = require('lodash');
  var util = require('util');
  var lowlaUtil = require('./util');
  var LowlaNeDB = require('./nedb').Datastore;

  var defaultOptions = {
    datastore:false,
    syncNotifier: function(payload){},
    sendDocumentLevelErrors:false,
    conflictHandler: defaultConflictHandler
  };

  function defaultConflictHandler(docResolver, serverDoc, changeOps) {
    docResolver.ignoreChanges();
  }

  var LowlaAdapter = function(options) {
    var adapter = this;
    adapter.ready = new _prom.Promise(function(resolve, reject){
    var config = adapter.config = _.extend({}, defaultOptions, options);
    if(!config.logger){
      adapter.logger = config.logger = lowlaUtil.loggerSetup(console);
      } else {
      adapter.logger = config.logger = lowlaUtil.loggerSetup(config.logger);
    }
    if (!config.datastore) {
      config.datastore = new LowlaNeDB({dbDir:false});
      adapter.logger.info('LowlaDb adapter is ready.');
      resolve(true);
      }
    });
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

  function getRequestInfo(req){
   return {ip: req.get('origin'), userAgent: req.get('user-agent')};
  }

  LowlaAdapter.prototype.pull = function(req, res, next) {
    var adapter = this;
    adapter.logger.debug("Pull requested: ", getRequestInfo(req));
    adapter.logger.verbose('Pull request:', req.body);
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
    adapter.logger.debug("Push requested: ", getRequestInfo(req));
    adapter.logger.verbose('Push request:', util.inspect(req.body, { showHidden: true, depth: null }));
    res.setHeader('Cache-Control', 'no-cache, private, no-store, must-revalidate, max-stale=0, post-check=0, pre-check=0');
    res.setHeader("Content-Type", "application/json");
    var resultHandler = adapter.createResultHandler(res);

    return adapter.pushWithPayload(req.body, resultHandler).then(function(results) {
      adapter.logger.info("Pushed: " + JSON.stringify(results) );
      return results;
    }).then(null, function(err) {
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
          id = lowlaId;
        }
        outputStream.write( (started ? ', ' : '') + JSON.stringify({error:{id:id, message:err.message}}));
      },
      write: function (lowlaId, version, deleted, doc) {
        if(ended){adapter.logger.warn("Attempt to write to resultHandler after end", result); return;};
        var meta = {
          clientNs: adapter.config.datastore.namespaceFromId(lowlaId),
          id: lowlaId,
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
    return _prom.Promise.resolve().then(function() {
      var promises = [];
      if (!payload || !payload.ids) {
        resultHandler.start();
        return adapter.config.datastore.getAllDocuments(resultHandler)
          .then(function (result) {
            resultHandler.end();
            return result;
          });
      }
      else {
        resultHandler.start();
        payload.ids.forEach(function (docId) {
          var lowlaId = docId;
          promises.push(
            adapter.config.datastore.getDocument(lowlaId)
              .then(function (doc) {
                resultHandler.write(lowlaId, doc._version, false, doc);
                return lowlaId;  //TODO what do we want to return to the result handler?
              }, function (err) {
                if (err.isDeleted) {
                  resultHandler.write(lowlaId, null, true);
                  return;
                }
                adapter.logger.error(err);
                if(adapter.config.sendDocumentLevelErrors){
                  resultHandler.writeError(err, lowlaId);
                }
                throw new Error("Document retrieval failed with error for " + lowlaId);
              })
          );

        });
        return _prom.all(promises).then(function (result) {
          resultHandler.end();
          return result;
        });
      }
    });
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
        var lowlaId = doc._lowla.id;

        if (doc._lowla.deleted) {
          promises.push(
            adapter.config.datastore.removeDocument(lowlaId, versionPreUpdate)
              .then(notifySyncerRemove, function(err){
                if (err && err.isConflict) {
                  return adapter.handleConflict(lowlaId, 'remove')
                    .then(function(resolveType) {
                      switch (resolveType.mode) {
                        case 'ignore':
                          return resolveType.doc;

                        case 'apply':
                          return adapter.removeDocument(lowlaId, versionPreUpdate, true)
                            .then(notifySyncerRemove);
                      }
                    });
                }
                adapter.logger.error(err);
                resultHandler.writeError(err, lowlaId);
                throw new Error("Document delete failed for " + lowlaId);
              })
              .then(processOperationResult)
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
              .then(notifySyncerChange, function(err){
                if(err.isConflict) {
                  return adapter.handleConflict(lowlaId, doc.ops)
                    .then(function(resolveType) {
                      switch (resolveType.mode) {
                        case 'ignore':
                          return resolveType.doc;

                        case 'apply':
                          return adapter.config.datastore.updateDocumentByOperations(lowlaId, versionPreUpdate, resolveType.ops, true)
                            .then(notifySyncerChange);
                      }
                    });
                }
                adapter.logger.error(err);
                if(adapter.config.sendDocumentLevelErrors){
                  resultHandler.writeError(err, lowlaId);
                }
                throw new Error("Document update failed for " + lowlaId);
              })
              .then(processOperationResult)
          );
        }

        function notifySyncerChange(docUpdated) {
          syncPayload.modified.push({
            id: lowlaId,
            version: docUpdated._version,
            clientNs: adapter.config.datastore.namespaceFromId(lowlaId)
          });
          return docUpdated;
        }

        function notifySyncerRemove() {
          syncPayload.deleted.push(lowlaId);
          return null;
        }

        function processOperationResult(docUpdated) {
          if (docUpdated) {
            resultHandler.write(lowlaId, docUpdated._version, false, docUpdated);
          }
          else {
            resultHandler.write(lowlaId, versionPreUpdate, true);
          }

          return lowlaId;
        }
      });

      return _prom.settle(promises).then(function(result){
        config.syncNotifier(syncPayload);
        for(var r in result){
          //bluebird
          if(result[r].isFulfilled()){
            result[r] = result[r].value();
          }else{
            result[r] = result[r].reason();
          }
        }
        resultHandler.end();
        return result;
      });
    });
  };


  LowlaAdapter.prototype.handleConflict = function(lowlaId, ops) {
    var adapter = this;
    return adapter.config.datastore.getDocument(lowlaId)
      .then(function(serverDoc) {
        return new _prom.Promise(function (resolve, reject) {
          var docResolver = {
            ignoreChanges: function () {
              resolve({mode: 'ignore', doc: serverDoc});
            },

            applyChanges: function (newOps) {
              newOps = newOps || ops;
              resolve({mode: 'apply', ops: newOps});
            },

            fail: function(err) {
              reject(err);
            }
          };

          try {
            adapter.config.conflictHandler(docResolver, serverDoc, ops);
          }
          catch (err) {
            reject(err);
          }
        });
      });
  };

  exports.LowlaAdapter = LowlaAdapter;
})(module.exports);
