
(function(exports) {

  var lib = require('q');

  var customDefer = function() {
    var __d = function(){
      var that=this;
      that.promise=lib.Promise(function (resolve, reject) {
        that.resolve=resolve;
        that.reject=reject;
      })
    };
    return new __d();
  };

  var isLongStackEnabled = false;

  exports.defer = lib.defer;
  exports.all = lib.all;
  exports.promise = lib.promise;
  exports.Promise = lib.Promise;
  exports.reject = lib.reject;
  exports.settle = lib.allSettled;
  exports.enableLongStackTraces = function(){
    if(!isLongStackEnabled){
      lib.enableLongStackSupport = true;
      isLongStackEnabled = true;
      console.log("Long stack traces are now enabled.")
    }else{
      console.log("Long stack traces are already enabled.")
    }

  };

})(module.exports);