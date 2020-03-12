const app = require('express')();
const http = require('http').createServer(app);
const WebSocket = require('ws');

let mines = { fro: null, evo: null, gho: null, lco: null, hvc: null };
let paths = {
  events: [],
  'unit-locations': [],
  actions: [],
  dismisses: []
};
let connections = {};
Object.keys(mines).forEach(mine => {
    connections[mine] = JSON.parse(JSON.stringify(paths));
});

const wss = new WebSocket.Server({ server: http });
wss.on('connection', (...data) => {
    const path = data[1].url.split('/').splice(1, 2);
    connections[path[0]][path[1]] = data[0];
});

Object.keys(connections).forEach(mine => {
    Object.keys(connections[mine]).forEach(path => {

    });
});

app.get('/', function(req, res){
    res.sendFile(__dirname + '/index.html');
});

http.listen(3000, function(){
    console.log('listening on *:3000');
});