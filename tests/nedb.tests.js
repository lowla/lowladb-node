(function() {
  var chai = require('chai');
  chai.use(require('chai-as-promised'));
  var should = chai.should();
  var _p = require('../lib/promiseImpl');

  describe('NEDB Datastore', function() {
    var Datastore = require('../lib/nedb').Datastore;
    var db;
    beforeEach(function() {
      db = new Datastore({dbDir: null}); // in memory for tests
    });

    describe('namespaceFromId()', function() {
      it('parses namespace from LowlaID', function() {
        db.namespaceFromId('dbName.collOne$1234').should.equal('dbName.collOne');
        db.namespaceFromId('dbName.collOne.subColl$1234').should.equal('dbName.collOne.subColl');
        db.namespaceFromId('dbName.collOne.subColl$1234$extraDollar').should.equal('dbName.collOne.subColl');
      });
    });

    describe('idFromComponents()', function() {
      it('creates a LowlaID from components', function() {
        db.idFromComponents('dbName.collOne', '1234').should.equal('dbName.collOne$1234');
        db.idFromComponents('dbName.collOne.subColl', '1234').should.equal('dbName.collOne.subColl$1234');
        db.idFromComponents('dbName.collOne.subColl', '1234$extraDollar').should.equal('dbName.collOne.subColl$1234$extraDollar');
      });
    });

    describe('encode/decodeSpecialTypes()', function() {
      it('can handle Date objects', function() {
        var dateVal = new Date();
        var obj = { d: dateVal };

        db.encodeSpecialTypes(obj);
        obj.d._bsonType.should.equal('Date');
        obj.d.millis.should.equal(dateVal.getTime());

        db.decodeSpecialTypes(obj);
        should.not.exist(obj.d._bsonType);
        should.not.exist(obj.d.millis);
        obj.d.should.be.instanceOf(Date);
        obj.d.getTime().should.equal(dateVal.getTime());
      });

      it('can handle nested objects', function() {
        var dateVal = new Date();
        var obj = { subDoc: { date: dateVal }};

        db.encodeSpecialTypes(obj);
        obj.subDoc.date._bsonType.should.equal('Date');
        obj.subDoc.date.millis.should.equal(dateVal.getTime());

        db.decodeSpecialTypes(obj);
        should.not.exist(obj.subDoc.date._bsonType);
        should.not.exist(obj.subDoc.date.millis);
        obj.subDoc.date.should.be.instanceOf(Date);
        obj.subDoc.date.getTime().should.equal(dateVal.getTime());
      })
    });

    describe('getAllDocuments()', function() {
      function testAllDocuments() {
        var docs = [];
        function handler(lowlaId, version, deleted, doc) {
          docs.push({lowlaId: lowlaId, doc: doc});
        }

        return _p.Promise.all([
          db.updateDocumentByOperations('dbName.collOne$1234', undefined, { $set: { a: 1 }}),
          db.updateDocumentByOperations('dbName.collOne$2345', undefined, { $set: { a: 2 }}),
          db.updateDocumentByOperations('dbName.collTwo$9876', undefined, { $set: { a: 3 }})
        ])
          .then(function() {
            return db.getAllDocuments({ write: handler });
          })
          .then(function(res) {
            docs.should.have.length(3);
          })
      }

      it(' - In-memory DB', testAllDocuments);

      it(' - On-disk DB', function() {
        db = new Datastore({dbDir: '_testData' });
        return testAllDocuments()
          .then(function() {
            var fs = require('fs');
            var files = fs.readdirSync('_testData');
            files.forEach(function(file) {
              var path = '_testData/' + file;
              if (!fs.statSync(path).isDirectory()) {
                fs.unlinkSync(path);
              }
            });
            fs.rmdirSync('_testData');
          });
      });
    });

    describe('getDocument()', function() {
      beforeEach(function() {
        return _p.Promise.all([
          db.updateDocumentByOperations('dbName.collOne$1234', undefined, { $set: { a: 1 }}),
          db.updateDocumentByOperations('dbName.collOne$2345', undefined, { $set: { a: 2 }}),
          db.updateDocumentByOperations('dbName.collTwo$9876', undefined, { $set: { a: 3 }})
        ]);
      });

      it('can find a document', function() {
        return db.getDocument('dbName.collTwo$9876')
          .then(function(doc) {
            should.exist(doc);
            doc._id.should.equal('9876');
            doc.a.should.equal(3);
          });
      });

      it('can find one document among many', function() {
        return db.getDocument('dbName.collOne$2345')
          .then(function(doc) {
            should.exist(doc);
            doc._id.should.equal('2345');
            doc.a.should.equal(2);
          });
      });

      it('can not find what is not there', function() {
        return db.getDocument('dbName.collNotThere$311')
          .then(function() {
            throw Error('Should not resolve on missing doc');
          }, function(err) {
            should.exist(err.isDeleted);
            err.isDeleted.should.equal(true);
          });
      });
    });

    describe('removeDocument()', function() {
      it('can remove an existing document', function() {
        return db.updateDocumentByOperations('dbName.collName$1234', undefined, { $set: { a: 1 }})
          .then(function(newDoc) {
            should.exist(newDoc);
            return db.removeDocument('dbName.collName$1234', undefined);
          })
          .then(function(count) {
            count.should.equal(1);
            return db.findAll('dbName.collName', {}, {});
          })
          .then(function(docs) {
            docs.should.have.length(0);
          });
      });

      it('preserves other documents when removing one', function() {
        return db.updateDocumentByOperations('dbName.collName$1234', undefined, { $set: { a: 1 }})
          .then(function() {
            return db.updateDocumentByOperations('dbName.collName$2345', undefined, { $set: { a: 2 }})
          })
          .then(function() {
            return db.updateDocumentByOperations('dbName.collName$3456', undefined, { $set: { a: 3 }})
          })
          .then(function() {
            return db.removeDocument('dbName.collName$2345', undefined);
          })
          .then(function() {
            return db.findAll('dbName.collName', {}, { a: 1 });
          })
          .then(function(docs) {
            docs.should.have.length(2);
            docs[0]._id.should.equal('1234');
            docs[0].a.should.equal(1);
            docs[1]._id.should.equal('3456');
            docs[1].a.should.equal(3);
          });
      });

      it('conflicts if versions do not match', function() {
        return db.updateDocumentByOperations('dbName.collName$1234', undefined, { $set: { a: 1, _version: 5 }})
          .then(function() {
            return db.removeDocument('dbName.collName$1234', 3);
          })
          .then(function() {
            throw Error('Should not have removed document!');
          }, function(err) {
            err.isConflict.should.equal(true);
          });
      });
    });

    describe('updateDocumentByOperations()', function() {
      it('can insert a new document', function() {
        return db.updateDocumentByOperations('dbName.collName$1234', undefined, { $set: { a: 1, b: 2 }})
          .then(function(newDoc) {
            newDoc._id.should.equal('1234');
            newDoc.a.should.equal(1);
            newDoc.b.should.equal(2);
          });
      });

      it('can modify an existing document', function() {
        return db.updateDocumentByOperations('dbName.collName$1234', undefined, { $set: { a: 1, b: 2 }})
          .then(function() {
            return db.updateDocumentByOperations('dbName.collName$1234', undefined, { $set: { b: 3 }});
          })
          .then(function(newDoc) {
            newDoc._id.should.equal('1234');
            newDoc.a.should.equal(1);
            newDoc.b.should.equal(3);
          });
      });

      it('supports $inc on a new document', function() {
        return db.updateDocumentByOperations('dbName.collName$1234', undefined, { $inc: { _version: 1 }})
          .then(function(newDoc) {
            newDoc._id.should.equal('1234');
            newDoc._version.should.equal(1);
          });
      });

      it('supports $inc on an existing document', function() {
        return db.updateDocumentByOperations('dbName.collName$1234', undefined, { $inc: { _version: 1 }})
          .then(function() {
            return db.updateDocumentByOperations('dbName.collName$1234', undefined, {$inc: {_version: 1}});
          })
          .then(function(newDoc) {
            newDoc._id.should.equal('1234');
            newDoc._version.should.equal(2);
          });
      });

      it('supports $unset', function() {
        return db.updateDocumentByOperations('dbName.collName$1234', undefined, { $set: { a: 1, b: 2 }})
          .then(function() {
            return db.updateDocumentByOperations('dbName.collName$1234', undefined, { $unset: { b: true }});
          })
          .then(function(newDoc) {
            newDoc._id.should.equal('1234');
            newDoc.a.should.equal(1);
            should.not.exist(newDoc.b);
          });
      });

      it('fails on conflicts', function() {
        return db.updateDocumentByOperations('dbName.collName$1234', undefined, { $set: { a: 1, b: 2 }, $inc: { _version: 1 }})
          .then(function(doc) {
            doc._version.should.equal(1);
            return db.updateDocumentByOperations('dbName.collName$1234', 1, { $set: { a: 2, b: 4 }, $inc: { _version: 1 }})
          })
          .then(function(doc) {
            doc._version.should.equal(2);
            doc.a.should.equal(2);
            doc.b.should.equal(4);
            return db.updateDocumentByOperations('dbName.collName$1234', 1, { $set: { a: 22, b: 44 }, $inc: { _version: 1 }});
          })
          .then(function() {
            throw Error('Should not have resolved on conflict doc');
          }, function(err) {
            err.should.deep.equal({isConflict: true});
          });
      });

      it('can force updates on otherwise conflicting ops', function() {
        return db.updateDocumentByOperations('dbName.collName$1234', undefined, { $set: { a: 1, b: 2 }, $inc: { _version: 1 }})
          .then(function(doc) {
            doc._version.should.equal(1);
            return db.updateDocumentByOperations('dbName.collName$1234', 1, { $set: { a: 2, b: 4 }, $inc: { _version: 1 }})
          })
          .then(function(doc) {
            doc._version.should.equal(2);
            doc.a.should.equal(2);
            doc.b.should.equal(4);
            return db.updateDocumentByOperations('dbName.collName$1234', 1, { $set: { a: 22, b: 44 }, $inc: { _version: 1 }}, true);
          })
          .then(function(doc) {
            doc.a.should.equal(22);
            doc.b.should.equal(44);
            doc._version.should.equal(3);
          });
      });

      /*TODO - the following exhibits a bug in NeDB until https://github.com/louischatriot/nedb/pull/230 or equivalent
      it('fails updates by rejecting promise', function() {
        return db.updateDocumentByOperations('dbName.collName$1234', undefined, { $set: { $bad: 1 }})
          .should.eventually.be.rejectedWith(Error);
      })*/
    });
  })
})();
