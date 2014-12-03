
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

### Configuration ###

The `configureRoutes` method takes an optional second argument with configuration options.

```js
var lowlaConfig = {
    // The datastore to use; the default is NeDB
    datastore: new lowla.NEDBDatastore({ dbDir: 'lowlanedb' })
};

lowla.configureRoutes(app, lowlaConfig);
```
