const request = require('request');
const dbConn = require('./../_core/dbConn');
const bitmexClient = require('./../_core/bitmexConn');
const sprintfJs = require('sprintf-js');

const sprintf = sprintfJs.sprintf,
    vsprintf = sprintfJs.vsprintf;

let lowestSell = 0;
let highestBuy = 0;

let service = {};
// let bitmex1mTimeoutId = null;
// let bitmex5mTimeoutId = null;
// let request1m = false;
// let request5m = false;
let ordersBuffer = [];
let hiddenOrdersBuffer = [];
let orderIDs = [];
let hiddenOrderIDs = [];
service.downloadBitmexData = function (binSize, startTime) {
    try {
        if (startTime.length == 0) {
            if (binSize == '1m') {
                // startTime = '2017-01-01T00:00:00.000Z';
                startTime = '2019-04-25T00:00:00.000Z';
            } else if (binSize == '5m') {
                startTime = '2015-09-25T12:00:00.000Z';
            } else if (binSize == '1h') {
                startTime = '2015-09-25T12:00:00.000Z';
            }
        }
        startTime = startTime.replace("000Z", "100Z");
        let url = sprintf('https://www.bitmex.com/api/v1/trade/bucketed?binSize=%s&partial=false&symbol=%s&count=%d&reverse=false&startTime=%s',
            binSize, 'XBTUSD', 750, startTime);
        console.log('downloadBitmexData', url);
        request(url, function (error, response, body) {
            // console.log('downloadBitmexData-end');
            if (error) {
                console.log(error);
            }
            let items = JSON.parse(body);
            if (response.statusCode == 200 && items.length > 0) {
                let sql;
                let lastTimestamp;
                let rows = [];
                for (let item of items) {
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
                // sql = sprintf("CREATE TABLE IF NOT EXISTS `bitmex_data_%s` (  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,  `timestamp` varchar(30) DEFAULT NULL,  `symbol` varchar(10) DEFAULT NULL,  `open` double DEFAULT '0',  `high` double DEFAULT '0',  `low` double DEFAULT '0',  `close` double DEFAULT '0',  `volume` double DEFAULT '0',  PRIMARY KEY (`id`));", binSize);
                // console.log('insert');
                // dbConn.query(sql, [rows], (error, results, fields) => {
                //     if (error) {
                //         console.log(error)
                //     }
                    sql = sprintf("INSERT INTO `bitmex_data_%s`(`timestamp`, `symbol`, `open`, `high`, `low`, `close`, `volume`) VALUES ?;", binSize);

                    // console.log('mysql-start');
                    dbConn.query(sql, [rows], (error, results, fields) => {
                        if (error) {
                            console.log(error)
                        }
                        console.log('mysql-end');
                        setTimeout(service.downloadBitmexData, 5000, binSize, lastTimestamp);
                        // console.log('setTimeout-1m', '0');
                        console.log('setTimeout', '0', '5s', lastTimestamp);
                    });
                // });
                return;
            }
            setTimeout(service.downloadBitmexData, 60000, binSize, startTime);
            console.log('1m', startTime);
        });
    } catch (e) {
        setTimeout(service.downloadBitmexData, 60000, binSize, startTime);
        console.log('1m-exception', startTime);
    }
};
//
// service.downloadBitmex1mData = function (startTime) {
//     try {
//         if (startTime.length == 0) {
//             // startTime = '2017-01-01T00:00:00.000Z';
//             startTime = '2019-04-25T00:00:00.000Z';
//         }
//         startTime = startTime.replace("000Z", "100Z");
//         let url = sprintf('https://www.bitmex.com/api/v1/trade/bucketed?binSize=1m&partial=false&symbol=%s&count=%d&reverse=false&startTime=%s',
//             'XBTUSD', 750, startTime);
//         console.log('downloadBitmexData', url);
//         request(url, function (error, response, body) {
//             if (error) {
//                 console.log(error);
//             }
//             let items = JSON.parse(body);
//             if (response.statusCode == 200 && items.length > 0) {
//                 let sql;
//                 let lastTimestamp;
//                 let rows = [];
//                 for (let item of items) {
//                     rows.push([
//                         item.timestamp,
//                         item.symbol,
//                         item.open,
//                         item.high,
//                         item.low,
//                         item.close,
//                         item.volume,
//                     ]);
//                     lastTimestamp = item.timestamp;
//                 }
//                 // sql = sprintf("INSERT INTO `bitmex_data_%s`(`timestamp`, `symbol`, `open`, `high`, `low`, `close`, `volume`) VALUES('%s', '%s', %s, %s, %s, %s, %s);", binSize, item.timestamp, item.symbol, item.open, item.high, item.low, item.close, item.volume);
//                 sql = sprintf("CREATE TABLE IF NOT EXISTS `bitmex_data_1m` (  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,  `timestamp` varchar(30) DEFAULT NULL,  `symbol` varchar(10) DEFAULT NULL,  `open` double DEFAULT '0',  `high` double DEFAULT '0',  `low` double DEFAULT '0',  `close` double DEFAULT '0',  `volume` double DEFAULT '0',  PRIMARY KEY (`id`));");
//                 // console.log('create_table', sql);
//                 dbConn.query(sql, [rows], (error, results, fields) => {
//                     if (error) {
//                         console.log(error)
//                     }
//                     sql = sprintf("INSERT INTO `bitmex_data_1m`(`timestamp`, `symbol`, `open`, `high`, `low`, `close`, `volume`) VALUES ?;");
//                     dbConn.query(sql, [rows], (error, results, fields) => {
//                         if (error) {
//                             console.log(error)
//                         }
//                         if (bitmex1mTimeoutId) {
//                             clearTimeout(bitmex1mTimeoutId);
//                         }
//                         bitmex1mTimeoutId = setTimeout(service.downloadBitmex1mData, 5000, lastTimestamp);
//                         // console.log('setTimeout-1m', bitmex1mTimeoutId);
//                     });
//                 });
//                 console.log('5s', lastTimestamp);
//                 return;
//             }
//             if (bitmex1mTimeoutId) {
//                 clearTimeout(bitmex1mTimeoutId);
//             }
//             bitmex1mTimeoutId = setTimeout(service.downloadBitmex1mData, 5 * 60000, startTime);
//             console.log('5m', startTime);
//         });
//     } catch (e) {
//         if (bitmex1mTimeoutId) {
//             clearTimeout(bitmex1mTimeoutId);
//         }
//         bitmex1mTimeoutId = setTimeout(service.downloadBitmex1mData, 5 * 60000, startTime);
//         console.log('5m-exception', startTime);
//     }
// };
//
// service.downloadBitmex5mData = function (startTime) {
//     console.log('5m-request-start');
//     try {
//         if (startTime.length == 0) {
//             startTime = '2015-09-25T12:00:00.000Z';
//         }
//         startTime = startTime.replace("000Z", "100Z");
//         let url = sprintf('https://www.bitmex.com/api/v1/trade/bucketed?binSize=5m&partial=false&symbol=%s&count=%d&reverse=false&startTime=%s',
//             'XBTUSD', 750, startTime);
//         console.log('downloadBitmexData-start', url);
//         request(url, function (error, response, body) {
//             if (error) {
//                 console.log(error);
//             }
//             console.log('downloadBitmexData-end');
//             let items = JSON.parse(body);
//             if (response.statusCode == 200 && items.length > 0) {
//                 let sql;
//                 let lastTimestamp;
//                 let rows = [];
//                 for (let item of items) {
//                     rows.push([
//                         item.timestamp,
//                         item.symbol,
//                         item.open,
//                         item.high,
//                         item.low,
//                         item.close,
//                         item.volume,
//                     ]);
//                     lastTimestamp = item.timestamp;
//                 }
//                 // sql = sprintf("INSERT INTO `bitmex_data_%s`(`timestamp`, `symbol`, `open`, `high`, `low`, `close`, `volume`) VALUES('%s', '%s', %s, %s, %s, %s, %s);", binSize, item.timestamp, item.symbol, item.open, item.high, item.low, item.close, item.volume);
//                 sql = sprintf("CREATE TABLE IF NOT EXISTS `bitmex_data_5m` (  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,  `timestamp` varchar(30) DEFAULT NULL,  `symbol` varchar(10) DEFAULT NULL,  `open` double DEFAULT '0',  `high` double DEFAULT '0',  `low` double DEFAULT '0',  `close` double DEFAULT '0',  `volume` double DEFAULT '0',  PRIMARY KEY (`id`));");
//                 // console.log('create_table', sql);
//                 dbConn.query(sql, [rows], (error, results, fields) => {
//                     if (error) {
//                         console.log(error)
//                     }
//                     sql = sprintf("INSERT INTO `bitmex_data_5m`(`timestamp`, `symbol`, `open`, `high`, `low`, `close`, `volume`) VALUES ?;");
//                     dbConn.query(sql, [rows], (error, results, fields) => {
//                         if (error) {
//                             console.log(error)
//                         }
//                         /*if (bitmex5mTimeoutId) {
//                             clearTimeout(bitmex5mTimeoutId);
//                         }
//                         bitmex5mTimeoutId = */setTimeout(service.downloadBitmex5mData, 5000, lastTimestamp);
//                         // console.log('setTimeout-5m', bitmex5mTimeoutId);
//                     });
//                 });
//                 console.log('5s', lastTimestamp);
//                 return;
//             }
//             /*if (bitmex5mTimeoutId) {
//                 clearTimeout(bitmex5mTimeoutId);
//             }
//             bitmex5mTimeoutId = */setTimeout(service.downloadBitmex5mData, 5 * 60000, startTime);
//             console.log('5m', startTime);
//         });
//     } catch (e) {
//         /*if (bitmex5mTimeoutId) {
//             clearTimeout(bitmex5mTimeoutId);
//         }
//         bitmex5mTimeoutId = */setTimeout(service.downloadBitmex5mData, 5 * 60000, startTime);
//         console.log('5m-exception', startTime);
//     }
// };

service.getLastTimestamp4Bucket = function (binSize, callback) {
    const sql = sprintf("SELECT `timestamp` FROM `bitmex_data_%s` ORDER BY `timestamp` DESC LIMIT 0, 1;", binSize);
    dbConn.query(sql, null, (error, results, fields) => {
        // if (error) {
        //     console.log(error)
        // }
        let timestamp = '';
        try {
            if (results != null && results.length > 0) {
                timestamp = results[0].timestamp;
            }
        } finally {
            callback(timestamp);
        }
    });
};

service.readOrderBook = function () {
    bitmexClient.addStream('XBTUSD', 'orderBookL2_25', (data, symbol, tableName) => {
        if (data.length > 0) {
            // console.log(data);
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
            try {
                lowestSell = prices['Sell'][0]['price'];
                highestBuy = prices['Buy'][0]['price'];
            } catch (e) {

            }
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
                if (orderIDs.indexOf(item.trdMatchID) === -1) {
                    orderIDs.push(item.trdMatchID);
                    ordersBuffer.push(item);
                }
            }
            // console.log(JSON.stringify(trades));
            // let hiddenOrders = [];
            for (let item of trades['Sell']) {
                if (item.price > highestBuy && item.price <= lowestSell) {
                    // hiddenOrders.push(item);
                    if (hiddenOrdersBuffer.indexOf(item.trdMatchID) === -1) {
                        hiddenOrderIDs.push(item.trdMatchID);
                        hiddenOrdersBuffer.push(item);
                    }
                    // console.log('Hidden Sell', highestBuy, item.price, lowestSell, item.trdMatchID, JSON.stringify(item));
                }
            }
            for (let item of trades['Buy']) {
                if (item.price >= highestBuy && item.price < lowestSell) {
                    // hiddenOrders.push(item);
                    if (hiddenOrdersBuffer.indexOf(item.trdMatchID) === -1) {
                        hiddenOrderIDs.push(item.trdMatchID);
                        hiddenOrdersBuffer.push(item);
                    }
                    // console.log('Hidden Buy ', highestBuy, item.price, lowestSell, item.trdMatchID, JSON.stringify(item));
                }
            }

        }
    });
};

service.commitData = function() {

    let sql;
    let item;
    let buffer = [];
    console.log('buffer-length-start', ordersBuffer.length, hiddenOrdersBuffer.length);
    while (ordersBuffer.length) {
        item = ordersBuffer.shift();
        orderIDs = orderIDs.filter(function(value, index, arr) {
            return value == item.trdMatchID;
        });
        buffer.push(item);
        // sql = sprintf("INSERT INTO `orders`(`timestamp`, `symbol`, `side`, `size`, `price`, `tickDirection`, `trdMatchID`, `grossValue`, `homeNotional`, `foreignNotional`) SELECT * FROM (SELECT '%s' `timestamp`, '%s' `symbol`, '%s' `side`, '%s' `size`, '%s' `price`, '%s' `tickDirection`, '%s' `trdMatchID`, '%s' `grossValue`, '%s' `homeNotional`, '%s' `foreignNotional`) AS `tmp` WHERE NOT EXISTS (SELECT `id` FROM `orders` WHERE `trdMatchID` = '%s') LIMIT 0, 1;",
        //     item.timestamp, item.symbol, item.side, item.size, item.price, item.tickDirection, item.trdMatchID,
        //     item.grossValue, item.homeNotional, item.foreignNotional, item.trdMatchID);
        // sql = sprintf("SELECT COUNT(`id`) `count` FROM `orders` WHERE `trdMatchID` = '%s';", item.trdMatchID);
        // sql = sprintf("INSERT INTO `orders`(`timestamp`, `symbol`, `side`, `size`, `price`, `tickDirection`, `trdMatchID`, `grossValue`, `homeNotional`, `foreignNotional`) SET ?");
        // // console.log(sql);
        // await dbConn.query(sql, [], (error, results, fields) => {
        //     if (error) {
        //         console.log(error)
        //     }
        // });
    }
    if (buffer.length > 0) {
        sql = sprintf("INSERT INTO `orders`(`timestamp`, `symbol`, `side`, `size`, `price`, `tickDirection`, `trdMatchID`, `grossValue`, `homeNotional`, `foreignNotional`) VALUES ?");
        // console.log(sql);
        dbConn.query(sql, [buffer], (error, results, fields) => {
            if (error) {
                console.log(error)
            }
        });
    }
    buffer = []
    while (hiddenOrdersBuffer.length) {
        item = hiddenOrdersBuffer.shift();
        hiddenOrderIDs = hiddenOrderIDs.filter(function(value, index, arr) {
            return value == item.trdMatchID;
        });
        buffer.push(item);
        // sql = sprintf("INSERT INTO `hidden_orders`(`timestamp`, `symbol`, `side`, `size`, `price`, `tickDirection`, `trdMatchID`, " +
        //     "`grossValue`, `homeNotional`, `foreignNotional`) " +
        //     "SELECT * FROM (SELECT '%s' `timestamp`, '%s' `symbol`, '%s' `side`, '%s' `size`, '%s' `price`, '%s' `tickDirection`, '%s' `trdMatchID`, '%s' `grossValue`, '%s' `homeNotional`, '%s' `foreignNotional`) AS `tmp` WHERE NOT EXISTS (SELECT `id` FROM `hidden_orders` WHERE `trdMatchID` = '%s') LIMIT 0, 1;",
        //     item.timestamp, item.symbol, item.side, item.size, item.price, item.tickDirection, item.trdMatchID,
        //     item.grossValue, item.homeNotional, item.foreignNotional, item.trdMatchID);
        // // console.log(sql);
        // await dbConn.query(sql, null, (error, results, fields) => {
        //     if (error) {
        //         console.log(error)
        //     }
        // });
    }
    if (buffer.length > 0) {
        sql = sprintf("INSERT INTO `hidden_orders`(`timestamp`, `symbol`, `side`, `size`, `price`, `tickDirection`, `trdMatchID`, `grossValue`, `homeNotional`, `foreignNotional`) VALUES ?");
        // console.log(sql);
        dbConn.query(sql, [buffer], (error, results, fields) => {
            if (error) {
                console.log(error)
            }
        });
    }
    console.log('buffer-length-end', ordersBuffer.length, hiddenOrdersBuffer.length);
    setTimeout(service.commitData, 60000);
};

module.exports = service;
