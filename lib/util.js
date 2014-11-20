exports.loggerSetup = function(logger){
  if( !logger.error || !logger.warn || !logger.info || !(logger.debug || logger.log) ) {
    throw new Error('Logger must support methods: error(), warn(), info() and debug() OR log(); verbose() is optional.')
  }
  if(!logger.debug && logger.log){
    logger.debug = logger.log;
  }
  if(!logger.verbose){
    logger.info('Logger does not support verbose mode, disabled.')
    logger.verbose = function(){};
  }
  return logger;
}