
var MongoClient = require('mongodb').MongoClient;
var _prom = require('../lib/promiseImpl.js');

var _mc = new MongoClient();
var _db;

var mongo = function(){};
var _mongo = new mongo();

exports.createDocs = function(rootName, num){
  var docs = [];
  for(i=1; i<=num; i++){
    docs.push({name: rootName + i, a: i, b: 2*i, _version:1})
  }
  return docs;
}
exports.enableQLongStackSupport = function(){
  if(_prom.hasOwnProperty('longStackSupport')){  //enable Q stack traces if we're using Q...
    if(!_prom.longStackSupport) {
      _prom.longStackSupport = true;
      console.log("Q longStack support enabled... \n");
    }
  }
}

mongo.prototype.openDatabase = function(url){
  var deferred = _prom.defer();
  _mc.connect(url, function (err, db) {
    if (err) {
      deferred.reject(err);
    }
    _db = db;
    deferred.resolve(db);
  });
  return deferred.promise;
}

mongo.prototype.getCollection = function(db, collName){
  var deferred = _prom.defer();
  db.collection(collName, function (err, coll) {
    if (err) {
      deferred.reject(err);
    }
    deferred.resolve(coll);
  });
  return deferred.promise;
}

mongo.prototype.removeCollection = function(db, collName){
  var deferred = _prom.defer();
  db.collection(collName, function (err, coll) {
    if (err) {
      deferred.reject(err);
    }
    coll.remove(function (){
      deferred.resolve(true);
    });
  });
  return deferred.promise;
}

mongo.prototype.findDocs = function(db, collectionName, query) {
  var deferred = _prom.defer();
  db.collection(collectionName, function (err, coll) {
    if (err) {
      return deferred.reject(error);
    }
    coll.find(query, function (err, cursor) {
      if (err) {
        return deferred.reject(error);
      }
      cursor.toArray(function(err, docs){
        if (err) {
          return deferred.reject(error);
        }
        deferred.resolve(docs);
      });
    });
  });

  return deferred.promise;
}

mongo.prototype.getIds = function(db, collectionName){
  return _mongo.findDocs(db, collectionName, {}).then(function(docs){
    var ids = [];
    for (i in docs){
      ids.push(docs[i]._id);
    }
    return ids;
  })
}

mongo.prototype.insertDocs = function(db, collectionName, docs) {
  var deferred = _prom.defer();

  db.collection(collectionName, function (err, coll) {
    if (err) {
      return deferred.reject(error);
    }
    coll.insert(docs, function (err, result) {
      if (err) {
        return deferred.reject(error);
      }
      deferred.resolve(result);
    });
  });

  return deferred.promise;
}

mongo.prototype.getCollectionNames = function(db){
  var deferred = _prom.defer();
  db.collectionNames(function (err, colls) {
    if (err) {
      return deferred.reject(err);
    }
    var ret = [];
    for (col in colls) {
      var colname = colls[col].name;
      if (-1 == colname.indexOf('.system.')) {
        var collectionName = colname.substr(1 + colname.indexOf('.'));
        ret.push(collectionName);
      }
    }
    deferred.resolve(ret);
  });
  return deferred.promise;
}

mongo.prototype.removeAllCollections = function(db){
  return _mongo.getCollectionNames(db).then(function(collnames){
    var promises = [];
    collnames.forEach(function(collname){
      var p = _mongo.removeCollection(db, collname);
      promises.push(p);
    });
    return _prom.all(promises);
  })
}

exports.mongo = _mongo;
