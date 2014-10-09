
// *** NOT IN USE: checked in for history only.  See note in mongoPromises.js (same commit) *** //

(function(exports) {
  var Q = require('q');
  var _ = require('lodash');
  var mongo = require('./../mongoPromises.js');

  var defaultOptions = {
    db: false,
    mongoUrl: false,
    collections: false
  };

  var Datastore = function(options) {
    var ready = Q.defer();
    this.ready = ready.promise;

    var config = this.config = _.extend({}, defaultOptions, options);
    if (!config.db) {
      if (!config.mongoUrl) {
        throw new Error('Must specify either db or mongoUrl in LowlaAdapter options');
      }

      mongo.connect(config.mongoUrl).then(function(_db){
           config.db = _db;
           ready.resolve();
      });
    }
  };

  Datastore.prototype.databaseName = function(){
    return this.config.db.databaseName;
  }

  Datastore.prototype.getCollectionNames = function(){
    return this.config.db.collectionNames().then(function(colls) {
      var ret = [];
      for (col in colls) {
        if (-1 == colls[col].name.indexOf('.system.')) {
          ret.push(colls[col].name);
          console.log("col: " + colls[col].name);
        }
      }
      return ret;
    });
  }

  Datastore.prototype.findInCollection = function(collectionName, query, options) {
    var datastore = this;
    return datastore.config.db.collection(collectionName)
      .then(function(collection) {
        return collection.find(query);
      }).then(function(rawCursor){
        return mongo.cursor(rawCursor);
      })
      .then(function(cursorPromise){
        return cursorPromise;
      });
  }

  Datastore.prototype.cursorToArray = function(cursor){
    var deferred = Q.defer()
    cursor.toArray(function (err, docArray) {
      if (err) {
        deferred.reject(err);
      }
      deferred.resolve(docArray);
    });
    return deferred.promise;
  }

 // Datastore.prototype.getCollection = Q.nbind(this.db.collection, this.db);


  exports.Datastore = Datastore;
})(module.exports);