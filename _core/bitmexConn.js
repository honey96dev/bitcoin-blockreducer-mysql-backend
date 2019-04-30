const BitMEXClient = require('bitmex-realtime-api');
const config = require('./config');

const bitmexClient = new BitMEXClient({
    testnet: config.bitmex.testnet,
    apiKeyID: config.bitmex.apiKeyID,
    apiKeySecret: config.bitmex.apiKeySecret,
    maxTableLen: config.bitmex.maxTableLen
});

module.exports = bitmexClient;