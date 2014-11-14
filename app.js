var express = require('express');
var path = require('path');
var favicon = require('static-favicon');
var morgan = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');

//Create a logger to pass to Lowla.  This example uses bunyan, but any standard logger will do so long as it implements the required calls. //TODO doc this requirement
var bunyan = require('bunyan');
var PrettyStream = require('bunyan-prettystream');  //TODO bunyan-prettystream is not recommended for production use - but great for webstorm dev.
var prettyStdOut = new PrettyStream();
prettyStdOut.pipe(process.stdout);
var logConfig = { name: 'LowlaDb',streams: [
  { level: 'debug', stream: prettyStdOut },
  { level: 'debug', path: 'LowLa.log' }
]};
var logger = bunyan.createLogger(logConfig);
logger.stream = (function(_thisLogger){
  return {
    write:function(message, encoding){
      _thisLogger.trace(message.slice(0, -1));
    }
  }
})(logger);
logger.log = logger.debug;


var routes = require('./routes/index');
var users = require('./routes/users');

var lowlaConfig = {
  syncUrl: 'mongodb://127.0.0.1/lowlasync',
  mongoUrl: 'mongodb://127.0.0.1/lowladb'
 , logger: logger
};

var LowlaSync = require('./lib/sync.js').LowlaSyncer;
var LowlaAdapter = require('./lib/adapter.js').LowlaAdapter;
var lowlaSync = new LowlaSync(lowlaConfig);

lowlaConfig.syncNotifier = lowlaSync.getNotifierFunction();
var lowlaDB = new LowlaAdapter(lowlaConfig);

var app = express();

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

app.use(favicon());
app.use(morgan({stream: logger.stream }));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded());
app.use(cookieParser());
app.use(require('less-middleware')(path.join(__dirname, 'public')));
app.use(express.static(path.join(__dirname, 'public')));

//TODO cross-domain access should be pluggable/configurable.
app.use( function(req, res, next) {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if ('OPTIONS' == req.method) {
    res.send(204, '');
  }
  else {
    next();
  }
});

app.use('/', routes);
app.use('/users', users);
lowlaDB.configureRoutes(app);
lowlaSync.configureRoutes(app);

/// catch 404 and forward to error handler
app.use(function(req, res, next) {
  var err = new Error('Not Found');
  err.status = 404;
  next(err);
});

/// error handlers

// development error handler
// will print stacktrace
if (app.get('env') === 'development') {
  app.use(function(err, req, res, next) {
    res.status(err.status || 500);
    res.render('error', {
      message: err.message,
      error: err
    });
  });
}

// production error handler
// no stacktraces leaked to user
app.use(function(err, req, res, next) {
  res.status(err.status || 500);
  res.render('error', {
    message: err.message,
    error: {}
  });
});

module.exports = app;


