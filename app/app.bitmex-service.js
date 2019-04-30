const request = require('request');
const dbConn = require('./../_core/dbConn');
const bitmexClient = require('./../_core/bitmexConn');
const sprintfJs = require('sprintf-js');

const sprintf = sprintfJs.sprintf,
    vsprintf = sprintfJs.vsprintf;

let lowestSell = 0;
let highestBuy = 0;

let service = {};

service.downloadBitmexData = function (binSize, startTime) {
    if (startTime.length == 0) {
        startTime = '2015-09-25T12:00:00.000Z';
    }
    startTime = startTime.replace("000Z", "100Z");
    let url = sprintf('https://www.bitmex.com/api/v1/trade/bucketed?binSize=%s&partial=false&symbol=%s&count=%d&reverse=false&startTime=%s',
        binSize, 'XBTUSD', 750, startTime);
    console.log('downloadBitmexData', url);
    try {
        request(url, function (error, response, body) {
            if (error) {
                console.log(error);
            }
            ;
            let items = JSON.parse(body);
            if (response.statusCode == 200 && items.length > 0) {
                let sql;
                let lastTimestamp;
                let rows = [];
                for (let item of items) {
                    // rows.push({
                    //     timestamp: item.timestamp,
                    //     symbol: item.symbol,
                    //     open: item.open,
                    //     high: item.high,
                    //     low: item.low,
                    //     close: item.close,
                    //     volume: item.volume,
                    // });
                    rows.push([
                        item.timestamp,
                        item.symbol,
                        item.open,
                        item.high,
                        item.low,
                        item.close,
                        item.volume,
                    ]);
                    lastTimestamp = item.timestamp;
                }
                // sql = sprintf("INSERT INTO `bitmex_data_%s`(`timestamp`, `symbol`, `open`, `high`, `low`, `close`, `volume`) VALUES('%s', '%s', %s, %s, %s, %s, %s);", binSize, item.timestamp, item.symbol, item.open, item.high, item.low, item.close, item.volume);
                sql = sprintf("INSERT INTO `bitmex_data_%s`(`timestamp`, `symbol`, `open`, `high`, `low`, `close`, `volume`) VALUES ?;", binSize);
                dbConn.query(sql, [rows], (error, results, fields) => {
                    if (error) {
                        console.log(error)
                    }
                    setTimeout(service.downloadBitmexData, 5000, binSize, lastTimestamp);
                });
                console.log('5s', lastTimestamp);
                return;
            }
            setTimeout(service.downloadBitmexData, 5 * 60000, binSize, startTime);
            console.log('5m', startTime);
        });
    } catch (e) {
        setTimeout(service.downloadBitmexData, 5 * 60000, binSize, startTime);
        console.log('5m-exception', startTime);
    }
};

service.getLastTimestamp4Bucket = function (binSize, callback) {
    const sql = sprintf("SELECT `timestamp` FROM `bitmex_data_%s` ORDER BY `timestamp` DESC LIMIT 0, 1;", binSize);
    dbConn.query(sql, null, (error, results, fields) => {
        if (error) {
            console.log(error)
        }
        // console.log(results);
        if (results.length > 0) {
            callback(results[0].timestamp);
        } else {
            callback('');
        }
        // callback(results);
    });
};

service.readOrderBook = function () {
    bitmexClient.addStream('XBTUSD', 'orderBookL2_25', (data, symbol, tableName) => {
        if (data.length > 0) {
            let prices = {
                'Sell': [],
                'Buy': [],
            };
            for (let item of data) {
                prices[item.side].push(item);
            }
            prices['Sell'].sort(function(a, b) {
                return a.price - b.price;
            });
            prices['Buy'].sort(function(a, b) {
                return b.price - a.price;
            });
            lowestSell = prices['Sell'][0]['price'];
            highestBuy = prices['Buy'][0]['price'];
            // console.log('Sell', lowestSell, 'Buy', highestBuy);
        }
    });
};

service.readTrade = function () {
    bitmexClient.addStream('XBTUSD', 'trade', (data, symbol, tableName) => {
        if (data.length > 0) {
            let trades = {
                'Sell': [],
                'Buy': [],
            };
            for (let item of data) {
                trades[item.side].push(item);
            }
            // console.log(trades);
            let hiddenOrders = [];
            for (let item of trades['Sell']) {
                if (item.price > highestBuy && item.price <= lowestSell) {
                    hiddenOrders.push(item);
                    // console.log('Hidden Sell', highestBuy, item.price, lowestSell, item.trdMatchID, JSON.stringify(item));
                }
            }
            for (let item of trades['Buy']) {
                if (item.price >= highestBuy && item.price < lowestSell) {
                    hiddenOrders.push(item);
                    // console.log('Hidden Buy ', highestBuy, item.price, lowestSell, item.trdMatchID, JSON.stringify(item));
                }
            }

            let sql;
            for (let item of hiddenOrders) {
                sql = sprintf("SELECT * FROM hidden_orders WHERE trdMatchID = '%s';", item.trdMatchID);
                // console.log(sql);
                dbConn.query(sql, null, (error, results, fields) => {
                    if (error) {
                        console.log(error)
                    }
                    if (results.length == 0) {
                        sql = "INSERT INTO `hidden_orders` SET ?;";
                        dbConn.query(sql, [item], (error, results, fields) => {
                            if (error) {
                                console.log(error)
                            }
                        });
                    }
                });
            }
        }
    });
};

module.exports = service;
