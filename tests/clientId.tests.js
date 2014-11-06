var should = require('chai').should();
var LowlaId = require('../lib/datastore/lowlaId.js').LowlaId;

describe('LowlaId', function(){

  it('parses a client id via constructor', function(){
    var lowlaId = new LowlaId('myDb.myCollection$someId');
    lowlaId.dbName.should.equal('myDb');
    lowlaId.collectionName.should.equal('myCollection');
    lowlaId.id.should.equal('someId');
    lowlaId.getClientNs().should.equal('myDb.myCollection')
    lowlaId.getClientId().should.equal('myDb.myCollection$someId')
  }); 

  it('parses a client id', function(){
    var lowlaId = new LowlaId();
    lowlaId.fromClientId('myDb.myCollection$someId');
    lowlaId.dbName.should.equal('myDb');
    lowlaId.collectionName.should.equal('myCollection');
    lowlaId.id.should.equal('someId');
    lowlaId.getClientNs().should.equal('myDb.myCollection')
    lowlaId.getClientId().should.equal('myDb.myCollection$someId')
  });

  it('parses a client id with dotted collection names', function(){
    var lowlaId = new LowlaId();
    lowlaId.fromClientId('myDb.myCollection1.myCollection2.myCollection3$someId');
    lowlaId.dbName.should.equal('myDb');
    lowlaId.collectionName.should.equal('myCollection1.myCollection2.myCollection3');
    lowlaId.id.should.equal('someId');
    lowlaId.getClientNs().should.equal('myDb.myCollection1.myCollection2.myCollection3')
    lowlaId.getClientId().should.equal('myDb.myCollection1.myCollection2.myCollection3$someId')
  });

  it('rejects a client id missing database', function(){
    var lowlaId = new LowlaId();
    should.Throw(function(){lowlaId.fromClientId('myCollection$someId')}, 'LowlaId.fromClientId: clientId must be in the format database.collection$id');
  });

  it('rejects a client id missing id', function(){
    var lowlaId = new LowlaId();
    should.Throw(function(){lowlaId.fromClientId('myDb.myCollection')}, 'LowlaId.fromClientId: clientId must be in the format database.collection$id');
  });

  it('rejects a malformed client id', function(){
    var lowlaId = new LowlaId();
    should.Throw(function(){lowlaId.fromClientId('myDb.myDollar$myCollection$someId')}, 'LowlaId.fromClientId: clientId must be in the format database.collection$id');
  });

  it('builds an id from components', function(){
    var lowlaId = new LowlaId();
    lowlaId.fromComponents('myDb', 'myCollection', 'someId');
    lowlaId.dbName.should.equal('myDb');
    lowlaId.collectionName.should.equal('myCollection');
    lowlaId.id.should.equal('someId');
    lowlaId.getClientNs().should.equal('myDb.myCollection')
    lowlaId.getClientId().should.equal('myDb.myCollection$someId')
  });

  it('builds an id from components with dotted collection names', function(){
    var lowlaId = new LowlaId();
    lowlaId.fromComponents('myDb', 'myCollection1.myCollection2.myCollection3', 'someId');
    lowlaId.dbName.should.equal('myDb');
    lowlaId.collectionName.should.equal('myCollection1.myCollection2.myCollection3');
    lowlaId.id.should.equal('someId');
    lowlaId.getClientNs().should.equal('myDb.myCollection1.myCollection2.myCollection3')
    lowlaId.getClientId().should.equal('myDb.myCollection1.myCollection2.myCollection3$someId')
  });

  it('rejects a malformed id from components', function(){
    var lowlaId = new LowlaId();
    should.Throw(function(){lowlaId.fromComponents('myDb', 'someId')}, 'LowlaId.fromComponents() requires parameters dbName, collectionName, id');
  });

});