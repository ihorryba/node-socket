const app = require('express')();
const http = require('http').createServer(app);
const momenttz = require('moment-timezone');
const { BigQuery } = require('@google-cloud/bigquery');
const { PubSub } = require('@google-cloud/pubsub');
const WebSocket = require('ws');
const { roadQuality, truckLaunchSpeed } = require('./queries');
const _ = require('lodash');

const bigqueryClient = new BigQuery();
const pubsub = new PubSub();

const cashedEvents = { fro: [], evo: [], gho: [], lco: [], hvc: [] };
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

Object.keys(cashedEvents).forEach(mine => {
    getInitialData(getTime(), mine).then(() => {
        connections[mine].events.forEach(socket => {
            socket.send(JSON.stringify({ type: 'events', data: cashedEvents[mine] }));
        });
    });
});

const topic = pubsub.topic('aha-events');
subscribeToTopic('back-events-js');

const wss = new WebSocket.Server({ server: http });
wss.on('connection', (...data) => {
    const path = data[1].url.split('/').splice(1, 2);
    connections[path[0]][path[1]].push(data[0]);
    data[0].send(JSON.stringify({ type: 'events', data: cashedEvents[path[0]] }));
    data[0].on('close', () => {
        connections[path[0]][path[1]] = connections[path[0]][path[1]].filter(item => item !== data[0]);
    });
});

http.listen(3000, function(){
    console.log('listening on *:3000');
});

function getInitialData(time, mine) {
    return Promise.all([
        bigqueryClient.query({ query: roadQuality(
            time.startTimestamp, time.endTimestamp, mine.toUpperCase()), location: 'US' }),
        bigqueryClient.query({ query: truckLaunchSpeed(
            time.startTimestamp, time.endTimestamp, mine.toUpperCase()), location: 'US' })
    ]).then((data) => {
        const result = [...data[1][0], ...data[0][0]];
        convertToNeededStructure(result);
        return cashedEvents[mine] = result;
    });
}

function getTime() {
    const timestamp = momenttz();
    const endTimestamp = timestamp.utc().format();
    const startTimestamp = (timestamp.subtract(24, "hours").utc().format());
    return { startTimestamp, endTimestamp };
}

function fromSnakeToCamelCase(obj) {
    Object.keys(obj).forEach(key => {
        const arr = key.split('_');
        if (arr.length > 1) {
            const prop = arr.map((el, index) => {
                return index === 0 ? el : el.charAt(0).toUpperCase() + el.slice(1)
            });
            obj[prop.join('')] = obj[key];
            delete obj[key];
        }
    });
}

function ptopsToLowerCase(obj) {
    Object.keys(obj).forEach(key => {
        obj[key.toLowerCase()] = obj[key];
        delete obj[key];
    });
}

function prepareSegmentShovelData(item) {
    item.PAYLOAD = JSON.parse(item.PAYLOAD);
    if ('shovel_data' in item.PAYLOAD) {
        if (item.PAYLOAD.shovel_data.length) {
            item.PAYLOAD.shovel_data =
                _.maxBy(item.PAYLOAD.shovel_data, shovel => new Date(shovel.entry_time));
            fromSnakeToCamelCase(item.PAYLOAD.shovel_data);
        } else {
            item.PAYLOAD.shovel_data = {};
        }
    } else {
        if (item.PAYLOAD.segment_data.length) {
            item.PAYLOAD.segment_data =
                _.maxBy(item.PAYLOAD.segment_data, shovel => new Date(shovel.entry_time));
            fromSnakeToCamelCase(item.PAYLOAD.segment_data);
        } else {
            item.PAYLOAD.segment_data = {};
        }
    }
}

function convertToNeededStructure(result) {
    result.forEach(item => {
        prepareSegmentShovelData(item);
        ptopsToLowerCase(item);
        fromSnakeToCamelCase(item.payload);
    });
}

function subscribeToTopic(name) {
    const subscription = topic.subscription(name);
    subscription.on('error', () => {
        subscription.removeListener('message', onMessage);
        subscribeToTopic(name);
    });
    subscription.on('close', () => {
        subscription.removeListener('message', onMessage);
        subscribeToTopic(name);
    });
    subscription.on('message', onMessage);
}

function onMessage(message) {
    const data = JSON.parse(message.data.toString());
    if (data.metric_name === 'road_quality' || data.metric_name === 'truck_launch_speed') {
        const mine = data.site.toLowerCase();
        getInitialData(getTime(), mine).then(() => {
            connections[mine].events.forEach(socket => {
                socket.send(JSON.stringify({ type: 'events', data: cashedEvents[mine] }));
            });
        });
    }
    message.ack();
}

// Object.keys(connections).forEach(mine => {
//     Object.keys(connections[mine]).forEach(path => {
//
//     });
// });
