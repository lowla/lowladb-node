
var _prom = require('../lib/promiseImpl.js');

exports.createMockDatastore = function() {
  return {
    decodeSpecialTypes: function(obj) { return obj; },
    encodeSpecialTypes: function(obj) { return obj; },
    namespaceFromId: function(id) { return id.substring(0, id.indexOf('$')); },
    idFromComponents: function(ns, id) { return ns + '$' + id; },

    getAllDocuments: function() { return _prom.Promise.reject(Error('getAllDocuments() not implemented')); },
    getDocument: function() { return _prom.Promise.reject(Error('getDocument() not implemented')); },
    removeDocument: function() { return _prom.Promise.reject(Error('removeDocument() not implemented')); },
    updateDocumentByOperations: function() { return _prom.Promise.reject(Error('updateDocumentByOperations() not implemented')); }
  };
};

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
