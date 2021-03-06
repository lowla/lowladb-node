/**
 * Created by michael on 11/21/14.
 */

var _ = require('lodash');

var lowla = {};

lowla.Syncer = require('./lib/sync').LowlaSyncer;
lowla.Adapter = require('./lib/adapter').LowlaAdapter;
lowla.NeDBDatastore = require('./lib/nedb').Datastore;
lowla.configureRoutes = configureRoutes;

module.exports = lowla;

function configureRoutes(app, options) {
  var defaultConfig = {
    datastore: false
  };

  var config = _.extend({}, defaultConfig, options);
  if (!config.datastore) {
    config.datastore = new lowla.NeDBDatastore({ dbDir: 'lowlanedb' });
  }
  if (!config.notifier && config.io) {
    config.notifier = require('./lib/sionotify').createNotifier(config.io);
  }

  var syncer = new lowla.Syncer(config);
  config.syncNotifier = syncer.getNotifierFunction();

  var adapter = new lowla.Adapter(config);

  syncer.configureRoutes(app);
  adapter.configureRoutes(app);

  return config;
}
