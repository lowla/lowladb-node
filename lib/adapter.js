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
      this.logger.log("Adapter is waiting for Datastore...")
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
    app.get('/_lowla/pull', function(req, res, next){adapter.pull(req, res, next);});
    app.post('/_lowla/pull', function(req, res, next){adapter.pull(req, res, next);});
    app.post('/_lowla/push', function(req, res, next){adapter.push(req, res, next);});
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
      //next({ statusCode: 500, message: err });  //TODO ERR HANDLING AFTER RES BODY
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
      //next({ statusCode: 500, message: err });  //TODO ERR HANDLING AFTER RES BODY
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
    var adapter = this;
    return {
      start: function(){outputStream.write('[')},
      writeError: function (err){
        outputStream.write( (started ? ', ' : '') + JSON.stringify({error:{message:err.message}}));
      },
      write: function (result) {
        var meta = {
          clientNs: result.lowlaId.getClientNs(),
          id: result.lowlaId.getClientId(),
          deleted: result.deleted,
          version: result.version
        };
        if(!result.deleted) {
          adapter.config.datastore.encodeSpecialTypes(result.document);
        }
        outputStream.write( (started ? ', ' : '') + JSON.stringify(meta) + ( result.deleted ? '' : ',' + JSON.stringify(result.document) ) );
        started = true;
      },
      end: function(){outputStream.end(']')}
    }
  }

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
            .then(function(result){
              resultHandler.write(result);
              return docId;  //TODO what do we want to return to the result handler?
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

    resultHandler.start();


    if (!payload || !payload.documents) {
      reject("No payload or documents");
      return;
    }

    var promises = [];
    var result = [];
    var syncPayload = { modified: [], deleted: [] };

    payload.documents.forEach(function(doc) {
      if (!doc._lowla) {
        return;
      }

      var versionPreUpdate = doc._lowla.version;
      var lowlaId = new LowlaId(doc._lowla.id);

      if (doc._lowla.deleted) {
        promises.push(
          adapter.config.datastore.removeDocument(lowlaId, versionPreUpdate)
            .then( function (result){
              if (result.error) {
                throw Error('Document not removed: ' + result._lowla.getClientId() + '; Error: ' + result.error);
              }
              syncPayload.deleted.push(doc._lowla.id);
              resultHandler.write(result);
              return doc._lowla.id;
            })
        );
      }
      else {

        if (doc.ops['$inc']) {
          doc.ops['$inc']._version = 1;
        }
        else {
          doc.ops['$inc'] = { _version: 1 };
        }
        if(doc.ops['$set']._version){
          delete doc.ops['$set']._version
        }
        doc = adapter.config.datastore.decodeSpecialTypes(doc);

        //TODO verbose logging?
        //adapter.logger.log("Updating: ", doc);

        promises.push(
          adapter.config.datastore.updateDocumentByOperations(lowlaId, versionPreUpdate,  doc.ops)
            .then(function(result) {
              if (result.isConflict) {
                if(config.conflictHandler){
                  return adapter.config.datastore.getDocument(lowlaId).then(function(serverDocResult){
                    var docServerVersion = serverDocResult.document;  //todo deleted conflicts!
                    var resolveFunc, rejectFunc;
                    var conflictPromise = _prom.Promise(function (resolve, reject) {
                      resolveFunc=resolve;
                      rejectFunc=reject;
                    });
                    var ConflictResolver = function(){
                      return function(resolutionDoc){
                        if(resolutionDoc) {
                          //save and return the resolution doc
                          //TODO!
                          resolveFunc(resolutionDoc);
                        }else{
                          resolveFunc(docServerVersion);
                        }
                      }
                    };
                    try{
                      config.conflictHandler(result.document, docServerVersion, new ConflictResolver());
                    }catch(err){
                      rejectFunc(err);
                    }
                    return conflictPromise.then(function(document){
                      result.document = document;
                      result.version = document._version;
                      result.conflict = false;
                      if(!result.document){
                        result.isDeleted = true;
                      }
                      return result;
                    });
                  });
                }
              }
              return result;
            }).then(function(result){
              syncPayload.modified.push({  //push to syncr
                id: result.lowlaId.getClientId(),
                version: result.version,
                clientNs: result.lowlaId.getClientNs()
              });

              resultHandler.write(result);

              return lowlaId.getClientId();
            })
        );
      }
    });

    return _prom.all(promises).then(function(result) {

      config.syncNotifier(syncPayload);

      resultHandler.end();

      return result;
    });

  };

  exports.LowlaAdapter = LowlaAdapter;
})(module.exports);
