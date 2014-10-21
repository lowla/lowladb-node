
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
  }


  exports.defer = customDefer; //lib.defer;
  exports.all = lib.all;
  exports.promise = lib.promise;
  exports.Promise = lib.Promise;
})(module.exports);