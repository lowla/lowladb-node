(function() {
  var should = require('chai').should();
  var _p = require('../lib/promiseImpl');

  describe('NEDB Datastore', function() {
    var Datastore = require('../lib/datastore/nedb').Datastore;
    var db;
    beforeEach(function() {
      db = new Datastore({dbDir: null}); // in memory for tests
    });

    describe('namespaceFromId()', function() {
      it('parses namespace from LowlaID', function() {
        Datastore.namespaceFromId('dbName.collOne$1234').should.equal('dbName.collOne');
        Datastore.namespaceFromId('dbName.collOne.subColl$1234').should.equal('dbName.collOne.subColl');
        Datastore.namespaceFromId('dbName.collOne.subColl$1234$extraDollar').should.equal('dbName.collOne.subColl');
      });
    });

    describe('idFromComponents()', function() {
      it('creates a LowlaID from components', function() {
        Datastore.idFromComponents('dbName.collOne', '1234').should.equal('dbName.collOne$1234');
        Datastore.idFromComponents('dbName.collOne.subColl', '1234').should.equal('dbName.collOne.subColl$1234');
        Datastore.idFromComponents('dbName.collOne.subColl', '1234$extraDollar').should.equal('dbName.collOne.subColl$1234$extraDollar');
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
            return db.getAllDocuments(handler);
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
          .then(function(doc) {
            should.not.exist(doc);
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
    });
  })
})();
