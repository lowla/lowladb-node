
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
endpoints.  By default, LowlaDB will be configured to store its data in a local MongoDB instance.

### Configuration ###

The `configureRoutes` method takes an optional second argument with configuration options.

```js
var lowlaConfig = {
    // The MongoDB URL to store Sync data
    syncUrl: 'mongodb://127.0.0.1/lowlasync',

    // The MongoDB URL to store document data
    mongoUrl: 'mongodb://127.0.0.1/lowladb',
};

lowla.configureRoutes(app, lowlaConfig);
```

