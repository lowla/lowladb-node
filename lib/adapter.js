(function(exports) {
  var _prom = require('./promiseImpl');
  var _ = require('lodash');
  var Datastore = require('./datastore/datastore').Datastore;

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
      write: function (dbName, collectionName, doc, deleted) {
        var meta = {
          clientNs: dbName + '.' + collectionName,
          deleted: deleted
        };
        if(deleted) {
          meta.id = doc._lowla.id;
          meta.version = doc._lowla.version;
        }else{
          meta.id = dbName + '.' + collectionName + '$' + doc._id;
          meta.version = doc._version;
          doc = adapter.config.datastore.encodeSpecialTypes(doc);
        }
        outputStream.write( (started ? ', ' : '') + JSON.stringify(meta) + ( deleted ? '' : ',' + JSON.stringify(doc) ) );
        started = true;
      },
      end: function(){outputStream.end(']')}
    }
  }

  LowlaAdapter.prototype.parseLowlaId = function(lowlaId){
    var dot = lowlaId.indexOf('.');
    var dbName = lowlaId.substring(0, dot);

    var dollar = lowlaId.indexOf('$');
    var id = null;
    var work = lowlaId;
    if (-1 != dollar) {
      id = lowlaId.substring(dollar + 1);
      work = lowlaId.substring(0, dollar);
    }
    var collectionName = work.substring(dot + 1);
    return {
      dbName: dbName,
      collectionName: collectionName,
      id: id,
      clientNs: dbName + '.' + collectionName,
      fullId: lowlaId
    };
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

        var docIdExpanded = adapter.parseLowlaId(docId);
        promises.push(
          adapter.config.datastore.getDocument(docIdExpanded.collectionName, docIdExpanded.id)
            .then(function(doc){
              resultHandler.write(docIdExpanded.dbName, docIdExpanded.collectionName, doc, false); //TODO deleted
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

      var docIdExpanded = adapter.parseLowlaId(doc._lowla.id);

      if (doc._lowla.deleted) {
        promises.push(
          adapter.config.datastore.removeDocument(docIdExpanded.collectionName, docIdExpanded.id)
            .then( function (numRemoved){
              if (numRemoved!=1) {
                throw Error('Document not removed: ' + doc._lowla.id);
              }

              syncPayload.deleted.push(doc._lowla.id);
              resultHandler.write(docIdExpanded.dbName, docIdExpanded.collectionName, doc, true);
              return doc._lowla.id;
            })
        );
      }
      else {
        var versionPreUpdate = doc._lowla.version;
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
          adapter.config.datastore.updateDocumentByOperations(docIdExpanded.collectionName, docIdExpanded.id, versionPreUpdate,  doc.ops)
            .then(function(docNew) {
              var docUpdated = docNew;

              if (!docNew) {  //a conflict occurred
                if(config.conflictHandler){
                  return adapter.config.datastore.getDocument(docIdExpanded.collectionName, docIdExpanded.id).then(function(docServerVersion){
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
                      config.conflictHandler(doc, docServerVersion, new ConflictResolver());
                    }catch(err){
                      rejectFunc(err);
                    }
                    return conflictPromise;
                  });
                }
              }
              return docUpdated;
            }).then(function(docNew){
              syncPayload.modified.push({  //push to syncr
                id: docIdExpanded.fullId,
                version: docNew._version,
                clientNs: docIdExpanded.clientNs
              });

              resultHandler.write(docIdExpanded.dbName, docIdExpanded.collectionName, docNew, false);

              return docIdExpanded.fullId;
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
