
var MongoClient = require('mongodb').MongoClient;
var Mongo = require('mongodb');
var _prom = require('../lib/promiseImpl.js');
var LowlaId = require('../lib/datastore/lowlaId.js').LowlaId;

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
};

exports.enableLongStackSupport = function(){

  if(_prom.hasOwnProperty('enableLongStackTraces')){
      _prom.enableLongStackTraces();
      console.log("longStack support enabled... \n");
  }
};

exports.readFile = function(path){
  var deferred = _prom.defer();
  require('fs').readFile(require('path').resolve(__dirname, path), 'UTF-8', function(err, data){
    if(err){
      return deferred.reject(err);
    }
    deferred.resolve( data );
  });
  return deferred.promise
};

exports.createLowlaId = function(dbName, collectionName, id){
  var lowlaId = new LowlaId();
  lowlaId.fromComponents(dbName, collectionName, id);
  return lowlaId;
};

//loggers for tests

var nonOp = function(){};
exports.NullLogger = {log:nonOp, debug:nonOp, info:nonOp, warn:nonOp, error:nonOp};

exports.TestLogger = function(){
  this.logsByLevel = {};
  this.logs = [];
  var logFunc = function(level, binding){
    return function(){
      if(! binding.logsByLevel[level]){
        binding.logsByLevel[level] = [];
      }
      var entry = {level: level, ts: new Date().getTime(), args: Array.prototype.slice.call(arguments)};
      entry.idxInLevel = binding.logsByLevel[level].push(entry);
      entry.idxInAll = binding.logs.push(entry)
    };
  };
  this.reset=function(){this.logsByLevel={}, this.logs=[];};
  this.print=function(){
    for(l in this.logs){
      console.log(this.logs[l].ts, this.logs[l].level, this.logs[l].args);
    }
  };
  this.inspect=function(){
    for(l in this.logs){
      console.log(util.inspect(this.logs[l], { showHidden: true, depth: null }));
    }
  };
  this.log=logFunc('log' , this);
  this.debug=logFunc('debug', this);
  this.info=logFunc('info', this);
  this.warn=logFunc('warn', this);
  this.error=logFunc('error', this);
};


//mongo

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
};

mongo.prototype.getCollection = function(db, collName){
  var deferred = _prom.defer();
  db.collection(collName, function (err, coll) {
    if (err) {
      deferred.reject(err);
    }
    deferred.resolve(coll);
  });
  return deferred.promise;
};

mongo.prototype.removeCollection = function(db, collName){
  var deferred = _prom.defer();
  db.dropCollection(collName, function (err, coll) {
    if (err) {
      deferred.reject(err);
    }
    return deferred.resolve(true);
  });
  return deferred.promise;
};

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
};

mongo.prototype.getIds = function(db, collectionName){
  return _mongo.findDocs(db, collectionName, {}).then(function(docs){
    var ids = [];
    for (i in docs){
      ids.push(docs[i]._id);
    }
    return ids;
  })
};

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
};

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
};

mongo.prototype.removeAllCollections = function(db){
  return _mongo.getCollectionNames(db).then(function(collnames){
    var promises = [];
    collnames.forEach(function(collname){
      var p = _mongo.removeCollection(db, collname);
      promises.push(p);
    });
    return _prom.all(promises);
  })
};

exports.mongo = _mongo;
