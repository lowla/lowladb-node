(function(exports) {
  var _ = require('lodash');
  var _prom = require('./promiseImpl');
  var MongoClient = require('mongodb').MongoClient;

  var defaultOptions = {
    syncUrl: 'mongodb://127.0.0.1:27007/lowlasync',
    atomCollection: 'lowlaAtom',
    sequenceCollection: 'lowlaSequence'
  };

  var LowlaSyncer = function (options) {
    var ready = _prom.defer();
    this.ready = ready.promise;

    var syncer = this;
    var config = this.config = _.extend({}, defaultOptions, options);
    MongoClient.connect(config.syncUrl, function (err, db) {
      if (err) {
        throw err;
      }

      syncer.db = db;
      syncer.atoms = syncer.db.collection(config.atomCollection);
      syncer.atoms.ensureIndex('remoteKey', function () {
      });
      syncer.sequences = syncer.db.collection(config.sequenceCollection);

      ready.resolve();
    });
  };

  LowlaSyncer.prototype._updateAtom = function (seq, doc) {
    var deferred = _prom.defer();
    this.atoms.findAndModify(
      { remoteKey: doc.id},
      [
        ['remoteKey', 1]
      ],
      { $set: {
        clientNs: doc.clientNs,
        version: doc.version,
        id: doc.id,
        sequence: seq,
        deleted: false
      } },
      { upsert: true },
      function(err, response) {
        if (err) {
          deferred.reject(err);
        }
        else {
          deferred.resolve(response);
        }
      });
    return deferred.promise;
  };

  LowlaSyncer.prototype._deleteAtom = function (seq, docId) {
    var deferred = _prom.defer();
    this.atoms.findAndModify(
      { remoteKey: docId },
      { $set: {
        sequence: seq,
        deleted: true
      } },
      deferred.makeNodeResolver());
    return deferred.promise;
  };

  LowlaSyncer.prototype._bumpSequence = function () {
    var self = this;
    return _prom.promise(function (resolve, reject) {
      self.sequences.findAndModify(
        { _id: 'current'},
        [
          ['_id', 1]
        ],
        { $inc: { value: 1 } },
        { new: true, upsert: true },
        function (err, res) {
          if (err) {
            reject(err);
          }
          else {
            resolve(res);
          }
        });
    });
  };

  LowlaSyncer.prototype.notifyUpdates = function () {
    //TODO -- Either EventEmitter or callback that version have changed; push notification or socket.io, etc.
  };

  LowlaSyncer.prototype.updateWithPayload = function (payload) {
    var syncer = this;

    return this._bumpSequence().then(function (seq) {
      var promises = [];
      var modified = payload.modified || [];
      var deleted = payload.deleted || [];

      modified.forEach(function (doc) {
        promises.push(syncer._updateAtom(seq.value, doc));
      });

      deleted.forEach(function (docId) {
        promises.push(syncer._deleteAtom(seq.value, docId));
      });

      return _prom.all(promises).then(syncer.notifyUpdates).then(function () {
        return { sequence: seq.value }
      });
    });
  };

  LowlaSyncer.prototype.getNotifierFunction = function(){
    var syncer = this;
    return function(payload){syncer.updateWithPayload(payload)}
  }


  LowlaSyncer.prototype.changesSinceSequence = function (sequence) {
    var syncer = this;
    return _prom.promise(function (resolve, reject) {
      syncer.sequences.findOne({_id: 'current'}, function (err, seq) {
        if (err) {
          throw err;
        }

        if (!seq) {
          if (!sequence) {
            resolve({ sequence: 0 });
          }
          else {
            resolve({ atoms: [], sequence: 0 });
          }
          return;
        }

        if (!sequence) {
          resolve({ sequence: seq.value });
          return;
        }

        sequence = parseInt(sequence);
        syncer.atoms.find({sequence: { $gte: sequence }}).toArray(function (err, atoms) {
          if (err) {
            reject(err);
          }
          else {
            resolve({ atoms: atoms, sequence: seq.value });
          }
        })
      })
    });
  };

  LowlaSyncer.prototype.configureRoutes = function(app) {
    var syncer = this;
    app.get('/_lowla/changes', function(req, res, next) {
      syncer.changesSinceSequence(req.query.seq)
        .then(function(results) {
          res.setHeader('Cache-Control', 'no-cache, private, no-store, must-revalidate, max-stale=0, post-check=0, pre-check=0');
          res.setHeader("Content-Type", "application/json");
          res.send( JSON.stringify(results) );
        })
    })
  };

  exports.LowlaSyncer = LowlaSyncer;
})(module.exports);

