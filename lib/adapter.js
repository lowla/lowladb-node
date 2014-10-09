(function(exports) {
  var _prom = require('./promiseImpl');
  var _ = require('lodash');
  var Datastore = require('./datastore/datastore').Datastore;

  var defaultOptions = {
    datastore:false,
    syncNotifier: function(payload) { }
  };

  var LowlaAdapter = function(options) {
    var ready = _prom.defer();
    this.ready = ready.promise;

    var config = this.config = _.extend({}, defaultOptions, options);
    if (!config.datastore) {

      var datastore = config.datastore = new Datastore(_.cloneDeep(config));

      datastore.ready.then(function(){
        ready.resolve();
      }).done();
    }
  };

  LowlaAdapter.prototype.configureRoutes = function(app) {
    var adapter = this;
    var pullFunc = function(req, res, next) {
      res.setHeader('Cache-Control', 'no-cache, private, no-store, must-revalidate, max-stale=0, post-check=0, pre-check=0');
      res.setHeader("Content-Type:", "application/json");

      var resultHandler = adapter.createResultHandler(res);

      adapter.pullWithPayload(req.body, resultHandler ).then(function(results) {

        console.log("Pulled: " + JSON.stringify(results) );


      }, function(err) {
        next({ statusCode: 500, message: err });  //TODO ERR HANDLING AFTER RES BODY
      }).done();
    };

    app.get('/lowla/pull', pullFunc);
    app.post('/lowla/pull', pullFunc);

    app.post('/lowla/push', function(req, res, next) {
      res.setHeader('Cache-Control', 'no-cache, private, no-store, must-revalidate, max-stale=0, post-check=0, pre-check=0');
      res.setHeader("Content-Type:", "application/json");

      var resultHandler = adapter.createResultHandler(res);

        adapter.pushWithPayload(req.body, resultHandler).then(function(results) {

        console.log("Pushed: " + JSON.stringify(results) );

      }, function(err) {
        next({ statusCode: 500, message: err });  //TODO ERR HANDLING AFTER RES BODY
      })
    });
  };

  LowlaAdapter.prototype.createResultHandler = function(outputStream){
    //returns a function that knows how to write documents w/meta data in LowlaDb format
    //writes to the provided output stream held in closure.
    var started = false;
    return {
      start: function(){outputStream.write('[')},
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
  }

  LowlaAdapter.prototype.pullWithPayload = function(payload, resultHandler,  callback) {
    var adapter = this;
    var _config = adapter.config;

    var promises = [];

    if (!payload || !payload.ids) {
      resultHandler.start();
      return adapter.config.datastore.getAllDocuments(resultHandler)
        .then(function(result){
          resultHandler.end();
          if(callback){
            callback(result);
          }
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
        if(callback){
          callback(result);
        }
        return result;
      });
    }
  };

  LowlaAdapter.prototype.pushWithPayload = function(payload, resultHandler, callback) {
    var adapter = this;
    var config = this.config;

    resultHandler.start();


    if (!payload || !payload.documents) {
      if (callback) {
        callback("No payload or documents", null);
      }
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

        promises.push(
          adapter.config.datastore.updateDocumentByOperations(docIdExpanded.collectionName, docIdExpanded.id, versionPreUpdate,  doc.ops)
            .then(function(doc) {
              if (!doc) {
                Error('Document could not be updated due to conflict: ' + docIdExpanded.fullId); //TODO null = no upsert b/c doc._lowla.version existed, invoke conflict handler.
              }

              syncPayload.modified.push({  //push to syncr
                id: docIdExpanded.fullId,
                version: doc._version,
                clientNs: docIdExpanded.clientNs
              });

              resultHandler.write(docIdExpanded.dbName, docIdExpanded.collectionName, doc, false);

              return docIdExpanded.fullId;
            })
        );
      }
    });

    return _prom.all(promises).then(function(result) {

      config.syncNotifier(syncPayload);
      //this change didn't work for me after my changes, 'this' binds to the function. Ended up wrapping the function in closure in syncr method to return notifier
      //config.syncNotifier.call(config.syncNotifier, syncPayload);

      resultHandler.end();

      if (callback) {
        callback(null, result);
      }

      return result;
    });

  };

  exports.LowlaAdapter = LowlaAdapter;
})(module.exports);
