(function (exports) {
  var _prom = require('./../promiseImpl');
  var _ = require('lodash');
  var MongoClient = require('mongodb').MongoClient;
  var Binary = require('mongodb').Binary;
  var LowlaId = require('./lowlaId').LowlaId;

  var defaultOptions = {
    db: false,
    mongoUrl: false,
    collections: false
  };

  var Datastore = function (options) {
    var ready = _prom.defer();
    this.ready = ready.promise;

    var config = this.config = _.extend({}, defaultOptions, options);
    this.logger = config.logger;
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
          db.collectionNames(function (err, collectionNames) {
            if (err) {
              throw err;
            }
            config.collections = collectionNames;
            config.logger.info("Datastore ready. Connected to: " + config.db.databaseName);
            ready.resolve();
          });
        }
        else {
          ready.resolve();
        }
      });
    }
  };

  var createResult = function(lowlaId, doc){
    return {
      document: doc,
      lowlaId: lowlaId,
      deleted: false,
      error:false
    }
  }

  Datastore.prototype.databaseName = function () {
    return this.config.db.databaseName;
  };

  Datastore.prototype.getCollectionNames = function () {
    var deferred = _prom.defer();
    var ret = [];
    var colls = this.config.collections;  //TODO what if collections are added? should we always query these?
    for (col in colls) {
      var colname = colls[col].name;
      if (-1 == colname.indexOf('.system.')) {
        var collectionName = colname.substr(1 + colname.indexOf('.'));
        ret.push(collectionName);
      }
    }
    deferred.resolve(ret);
    return deferred.promise;
  };

  Datastore.prototype.getCollection = function (collectionName) {
    //TODO implement a cache of collections like the original version had in adapter.js
    var datastore = this;
    var deferred = _prom.defer();
    datastore.config.db.collection(collectionName, function (err, collection) {
      if (err) {
        return deferred.reject(err);
      }
      deferred.resolve(collection);
    });
    return deferred.promise;
  };

  Datastore.prototype.updateDocumentByOperations = function (lowlaId, versionPreUpdate, updateOps) {
    var datastore = this;
    var deferred = _prom.defer();
    datastore.config.db.collection(lowlaId.collectionName, function (err, collection) {
      if (err) {
        return deferred.reject(err);
      }
      var isNew = false;
      if(!lowlaId.id){
        deferred.reject(new Error('updateDocumentByOperations: id must be specified'));
      }
      var query = {_id: lowlaId.id};
      if(versionPreUpdate){
        query._version = versionPreUpdate;
      }else{
        isNew = true;
      }
      collection.findAndModify(query, [
        ['_id', 1]
      ], updateOps, {upsert: isNew, new: true}, function (err, doc) {
        var result = createResult(lowlaId, doc);
        if (err) {                      // error: resolve with the error for downstream error handling
          result.error = err;
          result.updateOperations = updateOps;
          result.version = versionPreUpdate;
        } else if(!doc) {               // conflict (regular or due to deletion), resolve for downstream handling
          result.isConflict = true;
          result.updateOperations = updateOps;
          result.version = versionPreUpdate;
        }else{
          result.version = doc._version;
        }
        deferred.resolve(result);
      });
    });
    return deferred.promise;
  };

  Datastore.prototype.removeDocument = function (lowlaId, versionPreDelete) {  //todo versions for conflicts
    var datastore = this;
    var deferred = _prom.defer();
    datastore.config.db.collection(lowlaId.collectionName, function (err, collection) {
      if (err) {
        return deferred.reject(err);
      }
      collection.remove({_id: lowlaId.id}, function(err, numRemoved) {
        var result = createResult(lowlaId);
        result.deleted = true;
        result.version = versionPreDelete;
        if (err) {
          result.error = err;
        }
        if(1>numRemoved) {
          result.error = "Document was not removed but MongoDb did not return an error.  Already deleted?"
          //result.isConflict = true; //todo
        }
        deferred.resolve(result);
      });
    });
    return deferred.promise;
  };

  Datastore.prototype.getDocument = function (lowlaId) {
    var datastore = this;
    var deferred = _prom.defer();
    datastore.config.db.collection(lowlaId.collectionName, function (err, collection) {
      if (err) {
        return deferred.reject(err);
      }
      collection.findOne({_id:lowlaId.id}, function (err, doc) {
        var result = createResult(lowlaId, doc);
        if (err) {                      // error: resolve with the error for downstream error handling
          result.error = err;
        } else if(!doc) {               // deleted
          result.deleted = true;
        }else{
          result.version = doc._version;
        }
        deferred.resolve(result);
      });
    });
    return deferred.promise;
  };

  Datastore.prototype.findInCollection = function (collectionName, query) {
    var datastore = this;
    var deferred = _prom.defer();
    datastore.config.db.collection(collectionName, function (err, collection) {
      if (err) {
        return deferred.reject(err);
      }
      collection.find(query, function (err, cursor) {
        if (err) {
          return deferred.reject(err);
        }
        deferred.resolve(cursor);
      });
    });
    return deferred.promise;
  };

  Datastore.prototype.getAllDocuments = function (docHandler) {
    var datastore = this;
    return datastore.getCollectionNames().then(function (colnames) {
      var promises = [];
      colnames.forEach(function (collectionName) {
        var deferred = _prom.defer();
          datastore.findInCollection(collectionName, {}).then(function (cursor) {
            var cnt = 0;
            var stream = cursor.stream();
            var deleted = false; //always
            stream.on('close', function() {
              deferred.resolve({namespace: datastore.config.db.databaseName +"." + collectionName, sent: cnt});
            });

            stream.on('data', function(doc) {
              var lowlaId = new LowlaId();
              lowlaId.fromComponents(datastore.config.db.databaseName, collectionName, doc._id);
              var result = createResult(lowlaId, doc);
              result.version = doc._version;
              docHandler.write(result);
              ++cnt;
            });
          });
        promises.push(deferred.promise);
      });
      return _prom.all(promises) ;
    });
  };

  Datastore.prototype.cursorToArray = function (cursor) {
    var deferred = _prom.defer();
    cursor.toArray(function (err, docArray) {
      if (err) {
        return deferred.reject(err);
      }
      deferred.resolve(docArray);
    });
    return deferred.promise;
  };

  Datastore.prototype.encodeSpecialTypes = function (obj) {
    var datastore = this;
    for (var key in obj) {
      if (obj.hasOwnProperty(key)) {
        var val = obj[key];
        if ('object' === typeof val) {
          if(val instanceof Date){
            obj[key] = {_bsonType:'Date', millis: val.getTime()};
          }
          else if (val.hasOwnProperty('_bsontype')) {
            switch (val._bsontype) {
              case 'Binary':
                var buf = new Buffer(val);
                obj[key] = { _bsonType: 'Binary', type: 0}
                obj[key].encoded = val.toString('base64');
                break;

              case 'Date':  //not sure we can get here, working with objects we get Date handled above.
                obj[key] = {_bsonType:'Date', millis: val.getTime()};
                break;

              default:
                //nothing to do...
                //this may include ObjectID and other objects
                //which serialize acceptably to JSON.
                //console.log("Unhandled BSON type: " + key + ": ", val)
            }
          }
          else {
            datastore.encodeSpecialTypes(val);
          }
        }
      }
    }
    return obj;
  };

  Datastore.prototype.decodeSpecialTypes = function (obj) {
    var datastore = this;
    for (var key in obj) {
      if (obj.hasOwnProperty(key)) {
        var val = obj[key];
        if ('object' === typeof val) {
          if (val.hasOwnProperty('_bsonType')) {
            switch (val._bsonType) {
              case 'Binary':
                obj[key] = new Binary(new Buffer(val.encoded, 'base64'));
                break;

              case 'Date':
                obj[key] = new Date(parseInt(val.millis));
                break;

              default:
                throw Error('Unexpected BSON type: ' + val._bsonType);
            }
          }
          else {
            datastore.decodeSpecialTypes(val);
          }
        }
      }
    }

    return obj;
  };

  exports.Datastore = Datastore;
})(module.exports);