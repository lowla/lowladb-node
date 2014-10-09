
/* *** NOT IN USE: checked in for history only:
   Contemplated wrapping MongoDb at the API level, rather than at the Datastore level,
   so that the Datastore would work purely with promises.
   However:
    - added length to the promise chains, and therefore additional nextTicks()
    - currently depends heavily on Q helper methods which would require extra work to switch Promise libraries
    - Datastore is likely to remain database specific (e.g. MongoDb) anyway
    - without it the Datastore module will be more readable to those used to working with callbacks in MongoDb
      so the extra indirection doesn't seem worthwhile

   This and related files (datastorePromised.js, mongoPromise.tests.js) will be removed but are committed here in case we want to revisit the thought
   or use this file as a starting point for something else.
*/

(function(mongoPromises) {
  var Q = require('q');
  var MongoClient = require('mongodb').MongoClient;

  var dbPromise = function(db) {
    return {
      _raw: db,
      collectionNames: Q.nbind(db.collectionNames, db),  //promise collection names
      collection: function(name){ return Q.nbind(db.collection, db)(name).then(collectionPromise) },  //promise wrapped collection
      databaseName: db.databaseName
    }
  };

  var collectionPromise = function(collection) {
    return {
      _raw: collection,
      insert: Q.nbind(collection.insert, collection),
      find: Q.nbind(collection.find, collection),
      findAndModify: Q.nbind(collection.findAndModify, collection),
      remove: Q.nbind(collection.remove, collection)
    }
  };

  var cursorPromise = function(cursor){
    return{
      _raw: cursor,
      toArray: Q.nbind(cursor.toArray, cursor)
    }
  }

  var connect = Q.denodeify(MongoClient.connect);
  var connectPromise = function(url){
    return connect(url).then(dbPromise);
  }

  mongoPromises.connect = connectPromise;
  mongoPromises.db = dbPromise;
  mongoPromises.collection = collectionPromise;
  mongoPromises.cursor = cursorPromise;


})(module.exports);
