
(function(exports) {

  var lib = require('bluebird');

  var isLongStackEnabled = false;

  exports.all = lib.all;
  exports.Promise = lib.Promise;
  exports.reject = lib.reject;

  exports.settle = lib.settle;
  exports.enableLongStackTraces = function(){
    if(!isLongStackEnabled){
      lib.Promise.longStackTraces();
      isLongStackEnabled = true;
      console.log("Long stack traces are now enabled.");
    }else{
      //console.log("Long stack traces are already enabled.");
    }
  };


})(module.exports);