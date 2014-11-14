
(function(exports) {

  var lib = require('bluebird');

  var isLongStackEnabled = false;

  exports.all = lib.all;
  exports.Promise = lib.Promise;
  exports.reject = lib.reject;

  //bluebird
  exports.settle = lib.settle;
  exports.enableLongStackTraces = function(){
    if(!isLongStackEnabled){
      lib.Promise.longStackTraces();
      isLongStackEnabled = true;
      console.log("Long stack traces are now enabled.");
    }else{
      console.log("Long stack traces are already enabled.");
    }
  };

  //q
  //exports.settle = lib.allSettled;
  //exports.enableLongStackTraces = function(){
  //  if(!isLongStackEnabled){
  //    lib.enableLongStackSupport = true;
  //    isLongStackEnabled = true;
  //    console.log("Long stack traces are now enabled.");
  //  }else{
  //    console.log("Long stack traces are already enabled.");
  //  }
  //};


  //NOTE also that the switch to bluebird changed the way settle / allSettled values are dealt with at the end of adapter.pushWithPayload:
  //bluebird
      //if(result[r].isFulfilled()){
      //  result[r] = result[r].value();
      //}else{
      //  result[r] = result[r].reason();
      //}
  //q
      //if(result[r].state === 'fulfilled'){
      //  result[r] = result[r].value;
      //}else{
      //  result[r] = result[r].reason;
      //  adapter.logger.error("Document not written: ");
      //}

})(module.exports);