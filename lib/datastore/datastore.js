(function (exports) {
  var _prom = require('./../promiseImpl');
  var _ = require('lodash');
  var MongoClient = require('mongodb').MongoClient;
  var Binary = require('mongodb').Binary;
  var LowlaId = require('./lowlaId').LowlaId;

  var defaultOptions = {
    db: false,
    mongoUrl: false
  };

  var Datastore = function (options) {
    var datastore = this;
    this.ready = new _prom.Promise(function(resolve, reject) {
      var config = datastore.config = _.extend({}, defaultOptions, options);
      datastore.logger = config.logger;
      if (!config.db) {
        if (!config.mongoUrl) {
          return reject(new Error('Must specify either db or mongoUrl in LowlaAdapter options'));
        }
        MongoClient.connect(config.mongoUrl, function (err, db) {
          if (err) {
            return reject(err);
          }
          config.db = db;
          datastore.logger.debug("MongoDb datastore is ready. Database: " + db.databaseName);
          resolve(true);
        });
      }
    });
  };

  Datastore.prototype.databaseName = function () {
    return this.config.db.databaseName;
  };

  Datastore.prototype.getCollectionNames = function () {
    var datastore = this;
    return new _prom.Promise(function(resolve, reject){
      var ret = [];
      datastore.config.db.collectionNames(function (err, collectionNames) {
        if (err) {
          return reject(err);
        }
        for (col in collectionNames) {
          var colname = collectionNames[col].name;
          if (-1 == colname.indexOf('.system.')) {
            var collectionName = colname.substr(1 + colname.indexOf('.'));
            ret.push(collectionName);
          }
        }
        resolve(ret);
      });
    });
  };

  Datastore.prototype.getCollection = function (collectionName) {
    var datastore = this;
    return new _prom.Promise(function(resolve, reject){
      datastore.config.db.collection(collectionName, function (err, collection) {
        if (err) {
          return reject(err);
        }
        resolve(collection);
      });
    });
  };

  Datastore.prototype.updateDocumentByOperations = function (lowlaId, versionPreUpdate, updateOps) {
    var datastore = this;
    return new _prom.Promise(function(resolve, reject){
      datastore.getCollection(lowlaId.collectionName).then(
        function (collection) {
          try {
            var isNew = false;
            if (!lowlaId.id) {
              return reject(new Error('Datastore.updateDocumentByOperations: id must be specified'));
            }
            var query = {_id: lowlaId.id};
            if (versionPreUpdate) {
              query._version = versionPreUpdate;
            } else {
              isNew = true;
            }
            collection.findAndModify(query, [['_id', 1]], updateOps, {upsert: isNew, new: true},
              function (err, doc) {
                if (err) {
                  return reject(err)
                } else if (!doc) {
                  return reject({isConflict: true})
                }
                resolve(doc);
              }
            );
          }catch(err){
            reject(err);
          }
        },
        function(err){
          reject(err);
        });
    });
  };

  Datastore.prototype.removeDocument = function (lowlaId, versionPreDelete) {  //todo versions for conflicts
    var datastore = this;
    return new _prom.Promise(function (resolve, reject) {
      datastore.getCollection(lowlaId.collectionName).then(
        function (collection) {
          try{
            collection.remove({_id: lowlaId.id}, function (err, numRemoved) {
              if (err) {
                return reject(err);
              }
              if (1 > numRemoved) {
                return reject(new Error("Document was not removed but MongoDb did not return an error.  Already deleted?")); //todo
                isConflict = true; //todo
              }
              resolve(true);
            });
          }catch(err){
            reject(err);
          }
        },
        function(err){
          reject(err);
        });
    });
  };

  Datastore.prototype.getDocument = function (lowlaId) {
    var datastore = this;
    return new _prom.Promise(function (resolve, reject) {
      datastore.getCollection(lowlaId.collectionName).then(
        function (collection) {
          try{
            collection.findOne({_id:lowlaId.id}, function (err, doc) {
              if (err) {
                return reject(err);
              } else if(!doc) {               // deleted
                return reject({isDeleted:true});
              }
              resolve(doc);
            });
          }catch(err){
            reject(err);
          }
        },
        function(err){
          reject(err);
        });
    });
  };

  Datastore.prototype.findInCollection = function (collectionName, query) {
    var datastore = this;
    return new _prom.Promise(function (resolve, reject) {
      datastore.getCollection(collectionName).then(
        function (collection) {
          try {
            collection.find(query, function (err, cursor) {
              if (err) {
                return reject(err);
              }
              resolve(cursor);
            });
          } catch (err) {
            reject(err);
          }
        },
        function (err) {
          reject(err);
        }
      );
    });
  };

  Datastore.prototype.getAllDocuments = function (docHandler) {
    var datastore = this;
    return datastore.getCollectionNames().then(function (colnames) {
      var promises = [];
      colnames.forEach(function (collectionName) {
        promises.push(
          datastore.findInCollection(collectionName, {}).then(function (cursor) {
            return datastore.streamCursor(cursor, collectionName, docHandler)
          })
        );
      });
      return _prom.all(promises) ;
    });
  };

  Datastore.prototype.streamCursor = function (cursor, collectionName, docHandler){
    var datastore = this;
    return new _prom.Promise(function(resolve, reject){
      try{
        var cnt = 0;
        var stream = cursor.stream();
        var deleted = false; //always
        stream.on('close', function() {
          resolve({namespace: datastore.config.db.databaseName +"." + collectionName, sent: cnt});
        });
        stream.on('data', function(doc) {
          var lowlaId = new LowlaId();
          lowlaId.fromComponents(datastore.config.db.databaseName, collectionName, doc._id);
          docHandler.write(lowlaId, doc._version, deleted, doc);
          ++cnt;
        });
      } catch (err) {
        reject(err);
      }
    });
  };

  Datastore.prototype.cursorToArray = function (cursor) {
    return new _prom.Promise(function(resolve, reject){
      cursor.toArray(function (err, docArray) {
        if (err) {
          return reject(err);
        }
        resolve(docArray);
      });
    });
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
                obj[key] = { _bsonType: 'Binary', type: 0};
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