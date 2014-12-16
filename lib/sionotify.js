
module.exports.createNotifier = createSocketIoNotifier;

function createSocketIoNotifier(io) {
  return socketIoNotifier;

  function socketIoNotifier(eventName, payload) {
    io.sockets.emit(eventName, payload);
  }
}
