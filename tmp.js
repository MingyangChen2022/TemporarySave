'use strict';

const express = require('express');
let kafkaClient = require('./kafkaClient')
let bodyParser = require('body-parser')
const spookyhash = require('spookyhash')
const jsonMerger = require("json-merger");
const fs = require('fs');
const cron = require('node-cron');
let logger = require ('./logger');
const { createGzip } = require('zlib');
const { pipeline } = require('stream');
const util = require('util');
// const exec = util.promisify(require('child_process').exec);
const exec = require('child_process').exec;
const {
    createReadStream,
    createWriteStream
} = require('fs');



var serverconfig = JSON.parse (fs.readFileSync (__dirname+"/serverconfig.json").toString ());
// Constants
const PORT = 8080;
const HOST = '0.0.0.0';
const MAX_LINES_PER_FILE = serverconfig.maxLinesPerFile;
const clientMode = "tecentcos"
const validHeaders = [
  'x-expired-session-dropped',
  'x-request-ts',
  'x-event-count',
  'x-debug-device',
  'x-data-block-id',
  'x-unity-version',
  'user-agent'
];

var streamHandler = {};
var writtenFileHandler = {};
var writtenRecordCountHandler = {};
var targetDir = serverconfig.targetDirectory
var sourceDir = serverconfig.sourceDirectory


// App
const app = express();

function defaultContentTypeMiddleware (req, res, next) {
    req.headers['content-type'] = 'text/plain';
    next();
}

// create application/json parser
var jsonParser = bodyParser.json()
var rawParser = bodyParser.text()
app.use(defaultContentTypeMiddleware);
app.use(jsonParser)
app.use(rawParser)

app.get('/health', jsonParser, (req, res) => {
    res.send('ok');
});

const run = async () => {
    await kafkaClient.consumer.subscribe({ topic: 'topic-unity-analytics', fromBeginning: true })
    await kafkaClient.consumer.run({
        eachBatchAutoResolve: false,
        eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
            for (let message of batch.messages) {
                if (!isRunning() || isStale()) break
                processMessage(message)
                resolveOffset(message.offset)
            }
        },
    })
}

function processMessage(message, gzip, destination) {
    // check the message is correct, if not abandon       
    if (validJsonString(message.value.toString()) === false) {
        logger.errorlogger.error('message value is not valid json:   ' + message.value.toString())
        return
    }
    console.log("******start*********")
    console.log(message)
    console.log("*****end**********")
    const messageBody = JSON.parse(message.value.toString())
    const ip = messageBody['clientIp']
    const body = messageBody['body']

    if (ip === undefined || body === undefined) {
        logger.errorlogger.error('ip or body is missing')
        return
    }

}


function validJsonString(str) {
    try {
        JSON.parse(str);
    } catch (e) {
        return false;
    }
    return true;
}


run().catch(e => console.error(`[example/consumer] ${e.message}`, e))







app.listen(PORT, HOST);
require('events').EventEmitter.defaultMaxListeners = 100;
logger.infologger.info(`Running on http://${HOST}:${PORT}`);