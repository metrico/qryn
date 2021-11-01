const Watcher = require('../db/watcher');

module.exports = function handler(connection, res){
    try {


    const w = new Watcher(res.query);
    w.on('data', s => {
        connection.socket.send(s);
    });
    w.on('error', err => {
        console.log(err);
        connection.socket.send(err);
        connection.close();
    });
    connection.socket.on('close', () => {
        w.removeAllListeners('data');
        w.destroy();
    });
    } catch (e) {
        console.log(e);
    }
}