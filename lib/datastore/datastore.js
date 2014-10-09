(function (exports) {
  var _prom = require('./../promiseImpl');
  var _ = require('lodash');
  var MongoClient = require('mongodb').MongoClient;

  var defaultOptions = {
    db: false,
    mongoUrl: false,
    collections: false
  };

  var Datastore = function (options) {
    var ready = _prom.defer();
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
          db.collectionNames(function (err, collectionNames) {
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

  Datastore.prototype.databaseName = function () {
    return this.config.db.databaseName;
  }

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
  }

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
  }

  Datastore.prototype.updateDocumentByOperations = function (collectionName, id, version, updateOps) {
    var datastore = this;
    var deferred = _prom.defer();
    datastore.config.db.collection(collectionName, function (err, collection) {
      if (err) {
        return deferred.reject(err);
      }
      var isNew = false;
      if(!id){
        deferred.reject(new Error('updateDocumentByOperations: id must be specified'));
      }
      var query = {_id: id}
      if(version){
        query._version = version;
      }else{
        isNew = true;
      }
      collection.findAndModify(query, [
        ['_id', 1]
      ], updateOps, {upsert: isNew, new: true}, function (err, doc) {   //TODO _version check -> conflict handling
        if (err) {
          return deferred.reject(err);
        }
        deferred.resolve(doc);
      });
    });
    return deferred.promise;
  }

  Datastore.prototype.removeDocument = function (collectionName, id) {
    var datastore = this;
    var deferred = _prom.defer();
    datastore.config.db.collection(collectionName, function (err, collection) {
      if (err) {
        return deferred.reject(err);
      }
      collection.remove({_id: id}, function(err, numRemoved) {
        if (err) {
          return deferred.reject(err);
        }
        deferred.resolve(numRemoved);
      });
    });
    return deferred.promise;
  }

  Datastore.prototype.getDocument = function (collectionName, id) {
    var datastore = this;
    var deferred = _prom.defer();
    datastore.config.db.collection(collectionName, function (err, collection) {
      if (err) {
        return deferred.reject(err);
      }
      collection.findOne({_id:id}, function (err, doc) {
        if (err) {
          return deferred.reject(err);
        }
        deferred.resolve(doc);
      });
    });
    return deferred.promise;
  }

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
  }

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
              docHandler.write(datastore.config.db.databaseName, collectionName, doc, deleted);
              ++cnt;
            });
          });
        promises.push(deferred.promise);
      });
      return _prom.all(promises) ;
    });
  }

  Datastore.prototype.cursorToArray = function (cursor) {
    var deferred = _prom.defer()
    cursor.toArray(function (err, docArray) {
      if (err) {
        return deferred.reject(err);
      }
      deferred.resolve(docArray);
    });
    return deferred.promise;
  }

  exports.Datastore = Datastore;
})(module.exports);