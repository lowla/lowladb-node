(function(exports) {

  // Public APIs
  exports.LowlaSyncer = LowlaSyncer;
  LowlaSyncer.prototype.changesSinceSequence = changesSinceSequence;
  LowlaSyncer.prototype.configureRoutes = configureRoutes;
  LowlaSyncer.prototype.getNotifierFunction = getNotifierFunction;
  LowlaSyncer.prototype.updateWithPayload = updateWithPayload;

  // Internal APIs
  LowlaSyncer.prototype._bumpSequence = _bumpSequence;
  LowlaSyncer.prototype._deleteAtom = _deleteAtom;
  LowlaSyncer.prototype._notifyUpdates = _notifyUpdates;
  LowlaSyncer.prototype._updateAtom = _updateAtom;

  /////////////////////////////////////

  var _ = require('lodash');
  var _prom = require('./promiseImpl');
  var lowlaUtil = require('./util');
  var LowlaNeDB = require('./nedb').Datastore;

  var defaultOptions = {
    datastore: false,
    atomCollection: 'lowlaAtom',
    sequenceCollection: 'lowlaSequence',
    notifier: false
  };

  function LowlaSyncer(options) {
    var config = this.config = _.extend({}, defaultOptions, options);
    if(!config.logger){
      this.logger = config.logger = lowlaUtil.loggerSetup(console);
    } else {
      this.logger = config.logger = lowlaUtil.loggerSetup(config.logger);
    }
    if (!config.datastore) {
      config.datastore = new LowlaNeDB({ dbDir: false });
    }

    config.sequenceId = config.datastore.idFromComponents('lowlasync.' + config.sequenceCollection, 'current');
    config.atomPrefix = 'lowlasync.' + config.atomCollection;
  }

  function _updateAtom(seq, doc) {
    var syncer = this;
    var atomId = syncer.config.datastore.idFromComponents(syncer.config.atomPrefix, doc.id);
    return syncer.config.datastore.updateDocumentByOperations(atomId, undefined,
        {
          $set: {
            clientNs: doc.clientNs,
            version: doc.version,
            id: doc.id,
            sequence: seq,
            deleted: false
          }
        });
  }

  function _deleteAtom(seq, docId) {
    var syncer = this;
    var atomId = syncer.config.datastore.idFromComponents(syncer.config.atomPrefix, docId);
    return syncer.config.datastore.updateDocumentByOperations(atomId, undefined,
      {
        $set: {
          sequence: seq,
          deleted: true
        }
      });
  }

  function _bumpSequence() {
    var syncer = this;
    return syncer.config.datastore.updateDocumentByOperations(syncer.config.sequenceId, undefined, { $inc: { value: 1 }});
  }

  function _notifyUpdates() {
    //TODO -- Either EventEmitter or callback that version have changed; push notification or socket.io, etc.
    this.logger.verbose("Notify updates called...");
    if (this.config.notifier) {
      this.config.notifier('changes');
    }
  }

  function updateWithPayload(payload) {
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

      return _prom.all(promises)
        .then(function() {
          return syncer._bumpSequence();
        })
        .then(function(newSeq) {
          syncer._notifyUpdates();
          return newSeq;
        })
        .then(function (newSeq) {
          return { sequence: newSeq.value }
      });
    });
  }

  function getNotifierFunction(){
    var syncer = this;
    return function(payload){syncer.updateWithPayload(payload)}
  }

  function changesSinceSequence(sequence) {
    var syncer = this;
    return syncer.config.datastore.getDocument(syncer.config.sequenceId)
      .then(function(seq) {
        if (!seq) {
          if (!sequence) {
            return {sequence: 0};
          }
          else {
            return {atoms: [], sequence: 0};
          }
        }

        if (!sequence) {
          return {sequence: seq.value};
        }

        sequence = parseInt(sequence);
        return syncer.config.datastore.findAll(syncer.config.atomPrefix, {sequence: {$gte: sequence}}, { sequence: 1 })
          .then(function(atoms) {
            return { atoms: atoms, sequence: seq.value };
          });
      })
      .catch(function(err) {
        if (err && err.isDeleted) {
          return !sequence ? { sequence: 0 } : { atoms: [], sequence: 0 };
        }
        else {
          throw err;
        }
      });
  }

  function configureRoutes(app) {
    var syncer = this;
    app.get('/_lowla/changes', function(req, res, next) {
      syncer.changesSinceSequence(req.query.seq)
        .then(function(results) {
          res.setHeader('Cache-Control', 'no-cache, private, no-store, must-revalidate, max-stale=0, post-check=0, pre-check=0');
          res.setHeader("Content-Type", "application/json");
          res.send( JSON.stringify(results) );
        })
    })
  }
})(module.exports);

