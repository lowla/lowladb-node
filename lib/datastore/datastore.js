(function (exports) {

  var defaultOptions = {
    db: false,
    mongoUrl: false,
    logger: console
  };

  // Public APIs
  exports.Datastore = Datastore;
  Datastore.prototype.decodeSpecialTypes = decodeSpecialTypes;
  Datastore.prototype.encodeSpecialTypes = encodeSpecialTypes;
  Datastore.prototype.findAll = findAll;
  Datastore.prototype.getAllDocuments = getAllDocuments;
  Datastore.prototype.getDocument = getDocument;
  Datastore.prototype.removeDocument = removeDocument;
  Datastore.prototype.updateDocumentByOperations = updateDocumentByOperations;
  Datastore.namespaceFromId = namespaceFromId;
  Datastore.idFromComponents = idFromComponents;

  // Internal APIs
  Datastore.prototype._cursorToArray = _cursorToArray;
  Datastore.prototype._findInCollection = _findInCollection;
  Datastore.prototype._getCollection = getCollection;
  Datastore.prototype._getCollectionNames = _getCollectionNames;
  Datastore.prototype._streamCursor = _streamCursor;

  //////////////////////////////

  var _prom = require('../promiseImpl');
  var _ = require('lodash');
  var MongoClient = require('mongodb').MongoClient;
  var Binary = require('mongodb').Binary;

  function namespaceFromId(lowlaId) {
    var idx = lowlaId.indexOf('$');
    if (-1 === idx) {
      throw Error('Invalid LowlaID, missing namespace for ID ' + lowlaId);
    }
    return lowlaId.substring(0, idx);
  }

  function idFromComponents(namespace, datastoreKey) {
    return namespace + '$' + datastoreKey;
  }

  function componentsFromId(lowlaId) {
    var dot = lowlaId.indexOf('.');
    var dollar = lowlaId.indexOf('$');
    if(-1===dot || -1===dollar){
      throw new Error('Internal error: Lowla ID must be in the format database.collection$id');
    }
    var dbName = lowlaId.substring(0, dot);
    var id = null;
    var work = lowlaId;
    if (-1 != dollar) {
      id = lowlaId.substring(dollar + 1);
      work = lowlaId.substring(0, dollar);
    }
    var collectionName = work.substring(dot + 1);
    return {
      dbName: dbName,
      collectionName: collectionName,
      id: id
    };
  }

  function Datastore(options) {
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
  }

  function _getCollectionNames() {
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
  }

  function getCollection(collectionName) {
    var datastore = this;
    return new _prom.Promise(function(resolve, reject){
      datastore.config.db.collection(collectionName, function (err, collection) {
        if (err) {
          return reject(err);
        }
        resolve(collection);
      });
    });
  }

  function updateDocumentByOperations(lowlaId, versionPreUpdate, updateOps) {
    var datastore = this;
    return new _prom.Promise(function(resolve, reject){
      lowlaId = componentsFromId(lowlaId);
      datastore._getCollection(lowlaId.collectionName).then(
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
  }

  function removeDocument(lowlaId, versionPreDelete) {  //todo versions for conflicts
    var datastore = this;
    lowlaId = componentsFromId(lowlaId);
    return new _prom.Promise(function (resolve, reject) {
      datastore._getCollection(lowlaId.collectionName).then(
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
  }

  function getDocument(lowlaId) {
    var datastore = this;
    lowlaId = componentsFromId(lowlaId);
    return new _prom.Promise(function (resolve, reject) {
      datastore._getCollection(lowlaId.collectionName).then(
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
  }

  function _findInCollection(collectionName, query) {
    var datastore = this;
    return new _prom.Promise(function (resolve, reject) {
      datastore._getCollection(collectionName).then(
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
  }

  function findAll(collection, query) {
    var datastore = this;
    return datastore._findInCollection(collection, query)
      .then(function(cursor) {
        return datastore._cursorToArray(cursor);
      });
  }

  function getAllDocuments(docHandler) {
    var datastore = this;
    return datastore._getCollectionNames().then(function (colnames) {
      var promises = [];
      colnames.forEach(function (collectionName) {
        promises.push(
          datastore._findInCollection(collectionName, {}).then(function (cursor) {
            return datastore._streamCursor(cursor, collectionName, docHandler)
          })
        );
      });
      return _prom.all(promises) ;
    });
  }

  function _streamCursor(cursor, collectionName, docHandler){
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
          var lowlaId = Datastore.idFromComponents(datastore.config.db.databaseName + '.' + collectionName, doc._id);
          docHandler.write(lowlaId, doc._version, deleted, doc);
          ++cnt;
        });
      } catch (err) {
        reject(err);
      }
    });
  }

  function _cursorToArray(cursor) {
    return new _prom.Promise(function(resolve, reject){
      cursor.toArray(function (err, docArray) {
        if (err) {
          return reject(err);
        }
        resolve(docArray);
      });
    });
  }

  function encodeSpecialTypes(obj) {
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
  }

  function decodeSpecialTypes(obj) {
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
  }
})(module.exports);