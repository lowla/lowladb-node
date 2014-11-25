/**
 * Created by michael on 11/21/14.
 */

var _ = require('lodash');

var lowla = {};

lowla.Syncer = require('./lib/sync.js').LowlaSyncer;
lowla.Adapter = require('./lib/adapter.js').LowlaAdapter;

lowla.configureRoutes = function(app, options) {
  var defaultConfig = {
    syncUrl: 'mongodb://127.0.0.1/lowlasync',
    mongoUrl: 'mongodb://127.0.0.1/lowladb'
  };

  var config = _.extend({}, defaultConfig, options);

  var syncer = new lowla.Syncer(config);
  config.syncNotifier = syncer.getNotifierFunction();

  var adapter = new lowla.Adapter(config);

  syncer.configureRoutes(app);
  adapter.configureRoutes(app);

  return config;
};

module.exports = lowla;
