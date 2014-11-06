(function (exports) {

  var LowlaId = function (clientId) {
    this.dbName = null;
    this.collectionName = null;
    this.id = null;
    if(clientId){
      this.fromClientId(clientId);
    }
  };

  LowlaId.prototype.fromClientId = function(clientId){
    var dot = clientId.indexOf('.');
    var dollar = clientId.indexOf('$');
    if(-1===dot || -1===dollar || (-1!==clientId.indexOf('$', 1+dollar))){
      throw new Error('LowlaId.fromClientId: clientId must be in the format database.collection$id');
    }
    var dbName = clientId.substring(0, dot);
    var id = null;
    var work = clientId;
    if (-1 != dollar) {
      id = clientId.substring(dollar + 1);
      work = clientId.substring(0, dollar);
    }
    var collectionName = work.substring(dot + 1);
    this.dbName = dbName;
    this.collectionName = collectionName;
    this.id = id;
    this._origClientId = clientId;
  };

  LowlaId.prototype.fromComponents = function(dbName, collectionName, id){
    if(!dbName || !collectionName || !id){
      throw new Error('LowlaId.fromComponents() requires parameters dbName, collectionName, id');
    }
    this.dbName = dbName;
    this.collectionName = collectionName;
    this.id = id;
  };

  LowlaId.prototype.getClientId = function(){
    return this.dbName + '.' + this.collectionName + '$' + this.id;
  };

  LowlaId.prototype.getClientNs = function(){
    return this.dbName + '.' + this.collectionName;
  };


  exports.LowlaId = LowlaId;
})(module.exports);