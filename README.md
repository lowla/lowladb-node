
## LowlaDB for Node ##

> A LowlaDB Sync and Adapter server for Node

```js
var lowla = require('lowladb-node');
var express = require('express');
var app = express();

lowla.configureRoutes(app);

app.listen(3000);
```

### Installation ###

```bash
$ npm install lowladb-node --save
```

## Usage ##

At its simplest, use `configureRoutes(app)` as above to configure an ExpressJS server with LowlaDB Sync and Adapter
endpoints.  By default, LowlaDB will store its data via [NeDB](https://github.com/louischatriot/nedb) in a folder
called `lowlanedb`.

### Real-Time Sync via Socket.IO ###

LowlaDB supports the use of [Socket.IO](http://socket.io) to notify clients of document changes.  To enable this
support, construct an instance of SocketIO and provide it to the `configureRoutes()` method:

```js
var app = require('express')();
var server = require('http').Server(app);
var io = require('socket.io')(server);
var lowla = require('lowladb-node');

lowla.configureRoutes(app, { io: io });
```

The Socket.IO runtime will be made available to clients via `/socket.io/socket.client.js`.  See the
[LowlaDB Demo](https://github.com/lowla/lowladb-demo-node) project for an example of client configuration.

## Configuration ##

The `configureRoutes` method takes an optional second argument with configuration options.

```js
var lowlaConfig = {
    // The datastore to use; the default is NeDB
    datastore: new lowla.NEDBDatastore({ dbDir: 'lowlanedb' }),

    // SocketIO instance to use for live client updates
    io: undefined
};

lowla.configureRoutes(app, lowlaConfig);
```
