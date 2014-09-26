(function(exports) {
  var Q = require('q');
  var _ = require('lodash');
  var MongoClient = require('mongodb').MongoClient;

  var defaultOptions = {
    db: false,
    mongoUrl: false,
    collections: false,
    syncNotifier: function(payload) { }
  };

  var LowlaAdapter = function(options) {
    var ready = Q.defer();
    this.ready = ready.promise;

    var config = this.config = _.extend({}, defaultOptions, options);
    if (!config.db) {
      if (!config.mongoUrl) {
        throw new Error('Must specify either db or mongoUrl in LowlaAdapter options');
      }

      MongoClient.connect(config.mongoUrl, function (err, db) {
        if (err) {
          throw err;
        }

        config.db = db;
        if (!config.collections) {
          db.collectionNames(function(err, collectionNames) {
            if (err) {
              throw err;
            }
            config.collections = collectionNames;
            ready.resolve();
          });
        }
        else {
          ready.resolve();
        }
      });
    }
  };

  LowlaAdapter.prototype.configureRoutes = function(app) {
    var adapter = this;
    var pullFunc = function(req, res, next) {
      adapter.pullWithPayload(req.body).then(function(results) {
        res.setHeader('Cache-Control', 'no-cache, private, no-store, must-revalidate, max-stale=0, post-check=0, pre-check=0');
        res.setHeader("Content-Type:", "application/json");
        res.send( JSON.stringify(results) );
      }, function(err) {
        next({ statusCode: 500, message: err });
      })
    };

    app.get('/lowla/pull', pullFunc);
    app.post('/lowla/pull', pullFunc);

    app.post('/lowla/push', function(req, res, next) {
      adapter.pushWithPayload(req.body).then(function(results) {
        res.setHeader('Cache-Control', 'no-cache, private, no-store, must-revalidate, max-stale=0, post-check=0, pre-check=0');
        res.setHeader("Content-Type:", "application/json");
        res.send( JSON.stringify(results) );
      }, function(err) {
        next({ statusCode: 500, message: err });
      })
    });
  };

  var _populateResult = function(dbName, collectionName, docs, result, deleted) {
    var docArr = docs instanceof Array ? docs : [ docs ];
    docArr.forEach(function(doc) {
      var meta = {
        id: dbName + '.' + collectionName + '$' + doc._id,
        version: doc._version,
        clientNs: dbName + '.' + collectionName,
        deleted: deleted ? true : false
      };
      result.push(meta);
      result.push(doc);
    });
  };

  var collectionHash = {};

  LowlaAdapter.prototype._openCollection = function(clientId) {
    var adapter = this;
    var dot = clientId.indexOf('.');
    var dbName = clientId.substring(0, dot);

    var dollar = clientId.indexOf('$');
    var id = null;
    if (-1 != dollar) {
      id = clientId.substring(dollar + 1);
      clientId = clientId.substring(0, dollar);
    }
    var collectionName = clientId.substring(dot + 1);
    var lowlaDocId = {
      dbName: dbName,
      collectionName: collectionName,
      collection: null,
      id: id,
      clientNs: dbName + '.' + collectionName,
      fullId: clientId
    };
    if (collectionHash[clientId]) {
      //return collectionHash[clientId];  //TODO store only the collection promise not the whole lowla ID - compose it after
      return collectionHash[clientId].then(function(lowlaDocIdOther){
        lowlaDocId.collection = lowlaDocIdOther.collection;  //hack / when pull w/ requested IDs we can't return the orig promise w it's doc ids.
        return lowlaDocId;
      })
    }

    // For now, only support the one configured DB
    return collectionHash[clientId] = Q.Promise(function(resolve, reject) {
      adapter.config.db.collection(collectionName, function(err, collection) {
        if (err) {
          reject(err);
        }
        else {
          lowlaDocId.collection = collection;
          resolve(lowlaDocId);
        }
      });
    });
  };

  LowlaAdapter.prototype.pullWithPayload = function(payload, callback) {
    var adapter = this;
    var _config = adapter.config;

    return Q.Promise(function (resolve, reject) {
      var promises = [];
      var result = [];

      if (!payload || !payload.ids) {
        _config.collections.forEach(function (collectionObj) {

          if (-1 == collectionObj.name.indexOf('.system.')) {  //TODO confirm this is the best way

            var deferred = Q.defer();
            promises.push(deferred.promise);

            var collectionName = collectionObj.name.substr(1 + collectionObj.name.indexOf('.'))

            adapter.config.db.collection(collectionName, function (err, collection) {
              if (err) {
                deferred.reject(err);
              }
              collection.find({}, function (err, docs) {
                if (err) {
                  deferred.reject(err);
                }
                docs.toArray(function (err, docArray) {
                  if (err) {
                    deferred.reject(err);
                  }
                  _populateResult(_config.db.databaseName, collectionName, docArray, result);
                  deferred.resolve();
                });
              })
            });
          }
        });

      }
      else {
        payload.ids.forEach(function (docId) {
          var deferred = Q.defer();
          promises.push(deferred.promise);
          adapter._openCollection(docId).then(function (lowlaDocId) {
            lowlaDocId.collection.findOne({_id: lowlaDocId.id}, function (err, doc) {
              if (err) {
                deferred.reject(err);
              }
              _populateResult(lowlaDocId.dbName, lowlaDocId.collectionName, doc, result);
              deferred.resolve();
              return lowlaDocId;
            });
          });
        })
      }

      Q.all(promises).then(function () {
        if (callback) {
          callback(null, result);
        }
        resolve(result);
      }, function (err) {
        if (callback) {
          callback(err);
        }
        reject(err);
      });
    });
  };


  LowlaAdapter.prototype.pushWithPayload = function(payload, callback) {
    var adapter = this;
    var config = this.config;

    return Q.Promise(function(resolve, reject) {
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

        var deferred = Q.defer();
        promises.push(deferred.promise);

        adapter._openCollection(doc._lowla.id).then(function(coll) {
          if (doc._lowla.deleted) {
            coll.collection.remove({_id: coll.id}, function(err, numRemoved) {
              if (err) {
                reject(err);
                return;
              }

              syncPayload.deleted.push(doc._lowla.id);
              result.push({
                id: coll.clientId,
                deleted: true,
                version: doc._lowla.version,
                clientNs: coll.clientNs
              });
              deferred.resolve();
            })
          }
          else {
            if (doc.ops['$inc']) {
              doc.ops['$inc']._version = 1;
            }
            else {
              doc.ops['$inc'] = { _version: 1 };
            }

            coll.collection.findAndModify({_id: coll.id}, [['_id', 1]], doc.ops, {upsert: true, new: true}, function(err, doc) {   //TODO _version check -> conflict handling
              if (err) {
                reject(err);
                return;
              }

              syncPayload.modified.push({  //push to syncr
                id: coll.clientId,
                version: doc._version,
                clientNs: coll.clientNs
              });
              _populateResult(coll.dbName, coll.collectionName, doc, result);
              deferred.resolve();
            })
          }
        });
      });

      // Invoke all the update promises
      Q.all(promises).then(function() {
        config.syncNotifier(syncPayload);

        if (callback) {
          callback(null, result);
        }
        resolve(result);

      });

    });
  };

  exports.LowlaAdapter = LowlaAdapter;
})(module.exports);
