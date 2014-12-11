(function (exports) {

  var defaultOptions = {
    dbDir: './lowladb',
    logger: console
  };

  // Public APIs
  exports.Datastore = LowlaNEDB;
  LowlaNEDB.prototype.decodeSpecialTypes = decodeSpecialTypes;
  LowlaNEDB.prototype.encodeSpecialTypes = encodeSpecialTypes;
  LowlaNEDB.prototype.findAll = findAll;
  LowlaNEDB.prototype.getAllDocuments = getAllDocuments;
  LowlaNEDB.prototype.getDocument = getDocument;
  LowlaNEDB.prototype.idFromComponents = idFromComponents;
  LowlaNEDB.prototype.namespaceFromId = namespaceFromId;
  LowlaNEDB.prototype.removeDocument = removeDocument;
  LowlaNEDB.prototype.updateDocumentByOperations = updateDocumentByOperations;

  // Private APIs
  LowlaNEDB.prototype._openNamespace = _openNamespace;

  var NEDB = require('nedb');
  var _ = require('lodash');
  var _p = require('./promiseImpl');

  function LowlaNEDB(options) {
    this.config = _.extend({}, defaultOptions, options);
    this._collections = {};
  }

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

  function encodeSpecialTypes(obj) {
    for (var key in obj) {
      if (obj.hasOwnProperty(key)) {
        var val = obj[key];
        if ('object' === typeof val) {
          if(val instanceof Date){
            obj[key] = {_bsonType:'Date', millis: val.getTime()};
          }
          else {
            this.encodeSpecialTypes(val);
          }
        }
      }
    }
    return obj;
  }

  function decodeSpecialTypes(obj) {
    for (var key in obj) {
      if (obj.hasOwnProperty(key)) {
        var val = obj[key];
        if ('object' === typeof val) {
          if (val.hasOwnProperty('_bsonType')) {
            switch (val._bsonType) {
              case 'Date':
                obj[key] = new Date(parseInt(val.millis));
                break;
            }
          }
          else {
            this.decodeSpecialTypes(val);
          }
        }
      }
    }
    return obj;
  }

  function findAll(namespace, query, sort) {
    var idObj = _splitId(namespace + '$dummy');
    return this._openNamespace(idObj)
      .then(function(db) {
        return new _p.Promise(function(resolve, reject) {
          var cursor = db.find(query);
          if (sort) {
            cursor = cursor.sort(sort);
          }
          cursor.exec(function(err, docs) {
            if (err) { return reject(err); }
            resolve(docs);
          });
        });
      });
  }

  function getAllDocuments(docHandler) {
    var nedb = this;
    var promises = [];
    Object.keys(nedb._collections).forEach(function(key) {
      var onePromise = nedb._collections[key].then(function(db) {
        return scanNEDB(db, key);
      });

      promises.push(onePromise);
    });

    return _p.all(promises);

    function scanNEDB(db, namespace) {
      return new _p.Promise(function (resolve, reject) {
        db.find({}, function (err, docs) {
          if (err) {
            return reject(err);
          }

          docs.forEach(function (doc) {
            docHandler.write(nedb.idFromComponents(namespace, doc._id), doc._version, false, doc);
          });

          resolve({namespace: namespace, sent: docs.length});
        });
      });
    }
  }

  function getDocument(lowlaId) {
    var idObj = _splitId(lowlaId);
    return this._openNamespace(idObj)
      .then(function(db) {
        return new _p.Promise(function(resolve, reject) {
          db.find({_id: idObj.id}, function(err, docs) {
            if (err) { return reject(err); }
            if (0 !== docs.length) {
              resolve(docs[0]);
            }
            else {
              reject({ isDeleted: true });
            }
          });
        });
      });
  }

  function removeDocument(lowlaId, versionPreDelete) {
    var idObj = _splitId(lowlaId);
    return this._openNamespace(idObj)
      .then(function(db) {
        return new _p.Promise(function(resolve, reject) {
          db.remove({_id: idObj.id}, {}, function(err, numRem) {
            if (err) { reject(err); }
            resolve(numRem);
          });
        });
      });
  }

  function updateDocumentByOperations(lowlaId, versionPreUpdate, updateOps) {
    var idObj = _splitId(lowlaId);
    return this._openNamespace(idObj)
      .then(function(db) {
        return new _p.Promise(function(resolve, reject) {
          var upsert = false;
          var query = { _id: idObj.id };
          if (versionPreUpdate) {
            query._version = versionPreUpdate;
          }
          else {
            upsert = true;
          }

          try {
            db.update(query, updateOps, {upsert: upsert}, updateFn);
          }
          catch (err) {
            return reject(err);
          }

          function updateFn(err, numRep, newDoc) {
            if (err) { return reject(err); }
            if (0 === numRep) {
              return reject({isConflict: true});
            }
            if (newDoc) {
              return resolve(newDoc);
            }

            db.find({_id: idObj.id}, function(err, doc) {
              if (err) { return reject(err); }
              if (0 !== doc.length) {
                resolve(doc[0]);
              }
              else {
                resolve(undefined);
              }
            });
          }
        });
      });
  }

  function _openNamespace(idObj) {
    var db = this;
    if (!db._collections[idObj.namespace]) {
      db._collections[idObj.namespace] = _p.Promise.resolve().then(function() {
        var options = { autoload: true };
        if (db.config.dbDir) {
          var hashName = _hash(idObj.dbName) + '_' + _hash(idObj.collectionName) + '.lowla';
          options.filename = db.config.dbDir + '/' + hashName;
        }
        return new NEDB(options);
      });
    }
    return db._collections[idObj.namespace];
  }

  function _hash(s) {
    var hash = 0;
    var ch;

    if (s.length == 0) return hash;
    for (var i = 0, l = s.length; i < l; i++) {
      ch = s.charCodeAt(i);
      hash = ((hash << 5) - hash) + ch;
      hash |= 0; // Convert to 32bit integer
    }
    hash >>>= 0;
    return hash.toString(16).toUpperCase();
  }

  function _splitId(lowlaId) {
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
      id: id,
      namespace: dbName + '.' + collectionName,
      lowlaId: lowlaId
    };
  }
})(module.exports);
