
var MongoClient = require('mongodb').MongoClient;
var Mongo = require('mongodb');
var _prom = require('../lib/promiseImpl.js');

var _mc = new MongoClient();
var _db;

var mongo = function(){};
var _mongo = new mongo();

exports.createDocs = function(rootName, num){
  var docs = [];
  for(var i=1; i<=num; i++){
    docs.push({_id: 'testId_' + i, name: rootName + i, a: i, b: 2*i, _version:1})
  }
  return docs;
};

exports.enableLongStackSupport = function(){

  if(_prom.hasOwnProperty('enableLongStackTraces')){
      _prom.enableLongStackTraces();
  }
};

exports.readFile = function(path){
  return new _prom.Promise(function(resolve, reject){
    require('fs').readFile(require('path').resolve(__dirname, path), 'UTF-8', function(err, data){
      if(err){
        return reject(err);
      }
      resolve( data );
    });
  });
};

exports.createLowlaId = function(dbName, collectionName, id){
  var lowlaId = dbName + '.' + collectionName + '$' + id;
  return lowlaId;
};

//loggers for tests

var nonOp = function(){};
exports.NullLogger = {verbose:nonOp, debug:nonOp, info:nonOp, warn:nonOp, error:nonOp};

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
      entry.idxInAll = binding.logs.push(entry);
      //console.log(entry);
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
  this.verbose=logFunc('verbose' , this);
  this.debug=logFunc('debug', this);
  this.info=logFunc('info', this);
  this.warn=logFunc('warn', this);
  this.error=logFunc('error', this);
};


//mongo

mongo.prototype.openDatabase = function(url){
  return new _prom.Promise(function(resolve, reject){
  _mc.connect(url, function (err, db) {
    if (err) {
      return reject(err);
    }
    _db = db;
    resolve(db);
  });
  });
};

mongo.prototype.getCollection = function(db, collName){
  return new _prom.Promise(function(resolve, reject) {
    db.collection(collName, function (err, coll) {
      if (err) {
        return reject(err);
      }
      resolve(coll);
    });
  });
};

mongo.prototype.removeCollection = function(db, collName){
  return new _prom.Promise(function(resolve, reject) {
    db.dropCollection(collName, function (err, coll) {
      if (err) {
        return reject(err);
      }
      resolve(true);
    });
  });
};

mongo.prototype.findDocs = function(db, collectionName, query) {
  return new _prom.Promise(function(resolve, reject) {
    db.collection(collectionName, function (err, coll) {
      if (err) {
        return reject(error);
      }
      coll.find(query, function (err, cursor) {
        if (err) {
          return reject(error);
        }
        cursor.toArray(function (err, docs) {
          if (err) {
            return reject(error);
          }
          resolve(docs);
        });
      });
    });
  });
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
  return new _prom.Promise(function(resolve, reject) {
    db.collection(collectionName, function (err, coll) {
      if (err) {
        return reject(err);
      }
      coll.insert(docs, function (err, result) {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  });
};

mongo.prototype.getCollectionNames = function(db){
  return new _prom.Promise(function(resolve, reject) {
    db.collectionNames(function (err, colls) {
      if (err) {
        return reject(err);
      }
      var ret = [];
      for (col in colls) {
        var colname = colls[col].name;
        if (-1 == colname.indexOf('.system.')) {
          var collectionName = colname.substr(1 + colname.indexOf('.'));
          ret.push(collectionName);
        }
      }
      resolve(ret);
    });
  });
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
