const request = require('request');
const dbConn = require('./../_core/dbConn');
const bitmexClient = require('./../_core/bitmexConn');
var mysql = require('mysql2');
var config = require('../_core/config');
var Fili = require('fili');
const sprintfJs = require('sprintf-js');
const sprintf = sprintfJs.sprintf,
    vsprintf = sprintfJs.vsprintf;
const fftJs = require('fft-js');

let lowestSell = 0;
let highestBuy = 0;

let service = {};
// let bitmex1mTimeoutId = null;
// let bitmex5mTimeoutId = null;
// let request1m = false;
// let request5m = false;
let downloadBitmexTimeoutId = new Map();
let downloadBitmexInstrumentTimeoutId = null;
let commitTimeoutId = null;
let commitVolumeId = null;
let ordersBuffer = [];
let hiddenOrdersBuffer = [];
let orderIDs = [];
let hiddenOrderIDs = [];
let volumeTimestamp = '';
let calcedVolume1m = 0;
let calcedVolume5m = 0;
let calcedVolume1h = 0;
let volumeCnt5m = 0;
let volumeCnt1h = 0;

let openInterest5m = 0;
let openInterest1h = 0;
let openValue5m = 0;
let openValue1h = 0;
let openInterestCnt5m = 0;
let openInterestCnt1h = 0;
// let openInterestTimoutId;

let vwap5m = 0;
let vwap5mCnt= 0;
let vwap1h = 0;
let vwap1hCnt= 0;

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
                // console.log(response.statusCode);
            }
            // if (body && body.length > 0 && body.charAt(0) != '<') {
            if (response && response.statusCode === 200) {
                let items = JSON.parse(body);
                if (items.length > 0) {
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
                    sql = sprintf("INSERT INTO `bitmex_data_%s`(`timestamp`, `symbol`, `open`, `high`, `low`, `close`, `volume`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `symbol` = VALUES(`symbol`), `open` = VALUES(`open`), `high` = VALUES(`high`), `low` = VALUES(`low`), `close` = VALUES(`close`), `volume` = VALUES(`volume`);", binSize);

                    // console.log('mysql-start');
                    // let dbConn;
                    // if (binSize == '1m') {
                    //     dbConn = dbConn1m;
                    // } else if (binSize == '5m') {
                    //     dbConn = dbConn5m;
                    // } else if (binSize == '1h') {
                    //     dbConn = dbConn1h;
                    // }
                    // let dbConn = new mysql.createConnection(config.mysql);
                    // dbConn.connect(error => {
                    //     if (!error) {
                    //     }
                    dbConn.query(sql, [rows], (error, results, fields) => {
                        if (error) {
                            console.log(error);
                        } else {
                            service.calculateFFT(binSize, lastTimestamp);
                        }
                        // dbConn.end(function (err) {
                        //     // The connection is terminated now
                        // });
                        console.log('mysql-end');
                        if (downloadBitmexTimeoutId.get(binSize) != null) {
                            clearTimeout(downloadBitmexTimeoutId.get(binSize));
                        }
                        downloadBitmexTimeoutId.set(binSize, setTimeout(service.downloadBitmexData, 5000, binSize, lastTimestamp));
                        // console.log('setTimeout-1m', '0');
                        console.log('setTimeout', '0', '5s', lastTimestamp);
                    });
                    // });

                    // });
                    return;
                }
            }

            if (downloadBitmexTimeoutId.get(binSize) != null) {
                clearTimeout(downloadBitmexTimeoutId.get(binSize));
            }
            downloadBitmexTimeoutId.set(binSize, setTimeout(service.downloadBitmexData, 60000, binSize, startTime));
            console.log('1m', startTime);
        });
    } catch (e) {

        if (downloadBitmexTimeoutId.get(binSize) != null) {
            clearTimeout(downloadBitmexTimeoutId.get(binSize));
        }
        downloadBitmexTimeoutId.set(binSize, setTimeout(service.downloadBitmexData, 60000, binSize, startTime));
        console.log('1m-exception', startTime);
    }
};

service.downloadBitmexInstrumentData = function () {
    try {
        let timestamp;
        let flag = false;
        let url = sprintf('https://www.bitmex.com/api/v1/instrument?symbol=XBTUSD&count=100&reverse=false');
        console.log('downloadBitmexInstrumentData', url);
        request(url, function (error, response, body) {
            // console.log('downloadBitmexData-end');
            if (error) {
                console.log(error);
                // console.log(response.statusCode);
            }
            // if (body && body.length > 0 && body.charAt(0) != '<') {
            if (response && response.statusCode === 200) {
                let items = JSON.parse(body);
                if (items.length > 0) {
                    let sql;
                    let lastTimestamp;
                    let rows = [];
                    let openInterests = [];
                    let item = items[0]; {
                        lastTimestamp = item.timestamp;
                        timestamp = new Date(lastTimestamp);
                        timestamp.setMinutes(timestamp.getMinutes(), 0, 0);
                        rows = [
                            timestamp.toISOString(),
                            parseFloat(item.vwap) * 3 / (parseFloat(item.highPrice) + parseFloat(item.lowPrice) + parseFloat(item.lastPrice)),
                        ];
                        vwap5m += rows[1];
                        vwap1h += rows[1];
                        openInterests = [
                            timestamp.toISOString(),
                            item.openInterest,
                            item.openValue,
                        ];
                        openInterest5m += item.openInterest;
                        openValue5m += item.openValue;
                        openInterest1h += item.openInterest;
                        openValue1h += item.openValue;
                    }

                    if (++vwap5mCnt == 5) {
                        timestamp = new Date(lastTimestamp);
                        timestamp.setMinutes(Math.floor(timestamp.getMinutes() / 5) * 5, 0, 0);
                        sql = sprintf("INSERT INTO `vwap_5m`(`timestamp`, `vwap_seed`) VALUES ('%s', '%f') ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `vwap_seed` = VALUES(`vwap_seed`);", timestamp.toISOString(), vwap5m / 5);
                        vwap5mCnt = 0;
                        vwap5m = 0;
                        // console.log(sql);
                        dbConn.query(sql, null, (error, results, fields) => {
                            if (error) {
                                console.log(error);
                            } else {

                            }
                            // dbConn.end(function (err) {
                            //     // The connection is terminated now
                            // });
                            console.log('vwap_seed-mysql-end');
                        });
                    }

                    if (++vwap1hCnt == 60) {
                        timestamp = new Date(lastTimestamp);
                        timestamp.setMinutes(Math.floor(timestamp.getMinutes() / 5) * 5, 0, 0);
                        sql = sprintf("INSERT INTO `vwap_1h`(`timestamp`, `vwap_seed`) VALUES ('%s', '%f') ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `vwap_seed` = VALUES(`vwap_seed`);", timestamp.toISOString(), vwap1h / 60);
                        vwap1hCnt = 0;
                        vwap1h = 0;
                        // console.log(sql);
                        dbConn.query(sql, null, (error, results, fields) => {
                            if (error) {
                                console.log(error);
                            } else {

                            }
                            // dbConn.end(function (err) {
                            //     // The connection is terminated now
                            // });
                            console.log('vwap_seed-mysql-end');
                        });
                    }

                    sql = sprintf("INSERT INTO `vwap_1m`(`timestamp`, `vwap_seed`) VALUES ('%s', '%f') ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `vwap_seed` = VALUES(`vwap_seed`);", rows[0], rows[1]);

                    // console.log(sql);
                    dbConn.query(sql, null, (error, results, fields) => {
                        if (error) {
                            console.log(error);
                        } else {

                        }
                        // dbConn.end(function (err) {
                        //     // The connection is terminated now
                        // });
                        console.log('vwap_seed-mysql-end');
                    });

                    if (++openInterestCnt5m == 5) {
                        timestamp = new Date(lastTimestamp);
                        timestamp.setMinutes(Math.floor(timestamp.getMinutes() / 5) * 5, 0, 0);
                        sql = sprintf("INSERT INTO `interested_n_value_5m`(`timestamp`, `openInterest`, `openValue`) VALUES ('%s', '%f', '%f') ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `openInterest` = VALUES(`openInterest`), `openValue` = VALUES(`openValue`);", timestamp.toISOString(), openInterest5m / 5, openValue5m / 5);
                        openInterestCnt5m = 0;
                        openInterest5m = 0;
                        openValue5m = 0;
                        // console.log(sql);
                        dbConn.query(sql, null, (error, results, fields) => {
                            if (error) {
                                console.log(error);
                            } else {

                            }
                            // dbConn.end(function (err) {
                            //     // The connection is terminated now
                            // });
                            console.log('interested_n_value5m-mysql-end');
                        });
                    }

                    if (++openInterestCnt1h == 60) {
                        timestamp = new Date(lastTimestamp);
                        timestamp.setHours(timestamp.getHours(), 0, 0, 0);
                        sql = sprintf("INSERT INTO `interested_n_value_1h`(`timestamp`, `openInterest`, `openValue`) VALUES ('%s', '%f', '%f') ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `openInterest` = VALUES(`openInterest`), `openValue` = VALUES(`openValue`);", timestamp.toISOString(), openInterest1h / 60, openValue1h / 60);
                        openInterestCnt1h = 0;
                        openInterest1h = 0;
                        openValue1h = 0;
                        // console.log(sql);
                        dbConn.query(sql, null, (error, results, fields) => {
                            if (error) {
                                console.log(error);
                            } else {

                            }
                            // dbConn.end(function (err) {
                            //     // The connection is terminated now
                            // });
                            console.log('interested_n_value1h-mysql-end');
                        });
                    }

                    sql = sprintf("INSERT INTO `interested_n_value_1m`(`timestamp`, `openInterest`, `openValue`) VALUES ('%s', '%f', '%f') ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `openInterest` = VALUES(`openInterest`), `openValue` = VALUES(`openValue`);", openInterests[0], openInterests[1], openInterests[2]);
                    // console.log(sql);
                    dbConn.query(sql, [openInterests], (error, results, fields) => {
                        if (error) {
                            console.log(error);
                        } else {

                        }
                        // dbConn.end(function (err) {
                        //     // The connection is terminated now
                        // });
                        console.log('interested_n_value1m-mysql-end');
                        if (downloadBitmexInstrumentTimeoutId != null) {
                            clearTimeout(downloadBitmexInstrumentTimeoutId);
                        }
                        downloadBitmexInstrumentTimeoutId = setTimeout(service.downloadBitmexInstrumentData, 60000);
                        console.log('downloadBitmexInstrumentTimeoutId-1m', 60000);
                    });
                    return;
                }
            }
            console.log('interested_n_value1m-mysql-end');
            if (downloadBitmexInstrumentTimeoutId != null) {
                clearTimeout(downloadBitmexInstrumentTimeoutId);
            }
            downloadBitmexInstrumentTimeoutId = setTimeout(service.downloadBitmexInstrumentData, 60000);
            console.log('downloadBitmexInstrumentTimeoutId-1m', 60000);
        });
    } catch (e) {
        console.log(e);
        if (downloadBitmexInstrumentTimeoutId != null) {
            clearTimeout(downloadBitmexInstrumentTimeoutId);
        }
        downloadBitmexInstrumentTimeoutId = setTimeout(service.downloadBitmexInstrumentData, 60000);
        console.log('downloadBitmexInstrumentData-1m-exception', 60000);
    }
};

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
                    volumeTimestamp = item.timestamp;
                }
            }
            // console.log(JSON.stringify(trades));
            // let hiddenOrders = [];
            for (let item of trades['Sell']) {
                if (item.price > highestBuy && item.price <= lowestSell) {
                    // hiddenOrders.push(item);
                    if (hiddenOrderIDs.indexOf(item.trdMatchID) === -1) {
                        hiddenOrderIDs.push(item.trdMatchID);
                        hiddenOrdersBuffer.push(item);
                        calcedVolume1m -= item.price;
                        calcedVolume5m -= item.price;
                        calcedVolume1h -= item.price;
                    }
                    // console.log('Hidden Sell', highestBuy, item.price, lowestSell, item.trdMatchID, JSON.stringify(item));
                }
            }
            for (let item of trades['Buy']) {
                if (item.price >= highestBuy && item.price < lowestSell) {
                    // hiddenOrders.push(item);
                    if (hiddenOrderIDs.indexOf(item.trdMatchID) === -1) {
                        hiddenOrderIDs.push(item.trdMatchID);
                        hiddenOrdersBuffer.push(item);
                        calcedVolume1m += item.price;
                        calcedVolume5m += item.price;
                        calcedVolume1h += item.price;
                    }
                    // console.log('Hidden Buy ', highestBuy, item.price, lowestSell, item.trdMatchID, JSON.stringify(item));
                }
            }

        }
    });
};
//
// service.readVolume = function() {
//     bitmexClient.addStream('XBTUSD', 'trade', (data, symbol, tableName) => {
//         if (data.length > 0) {
//             let trades = {
//                 'Sell': [],
//                 'Buy': [],
//             };
//             for (let item of data) {
//                 trades[item.side].push(item);
//                 if (orderIDs.indexOf(item.trdMatchID) === -1) {
//                     orderIDs.push(item.trdMatchID);
//                     ordersBuffer.push(item);
//                 }
//             }
//             // console.log(JSON.stringify(trades));
//             // let hiddenOrders = [];
//             for (let item of trades['Sell']) {
//                 if (item.price > highestBuy && item.price <= lowestSell) {
//                     // hiddenOrders.push(item);
//                     if (hiddenOrderIDs.indexOf(item.trdMatchID) === -1) {
//                         hiddenOrderIDs.push(item.trdMatchID);
//                         hiddenOrdersBuffer.push(item);
//                     }
//                     // console.log('Hidden Sell', highestBuy, item.price, lowestSell, item.trdMatchID, JSON.stringify(item));
//                 }
//             }
//             for (let item of trades['Buy']) {
//                 if (item.price >= highestBuy && item.price < lowestSell) {
//                     // hiddenOrders.push(item);
//                     if (hiddenOrderIDs.indexOf(item.trdMatchID) === -1) {
//                         hiddenOrderIDs.push(item.trdMatchID);
//                         hiddenOrdersBuffer.push(item);
//                     }
//                     // console.log('Hidden Buy ', highestBuy, item.price, lowestSell, item.trdMatchID, JSON.stringify(item));
//                 }
//             }
//
//         }
//     });
// };

service.commitOrdersData = function() {
    // let commitFlag = false;
    let sql;
    let item;
    let buffer = [];
    console.log('buffer-length-start', ordersBuffer.length, hiddenOrdersBuffer.length);
    while (ordersBuffer.length) {
        item = ordersBuffer.shift();
        orderIDs = orderIDs.filter(function(value, index, arr) {
            return value != item.trdMatchID;
        });
        buffer.push([
            item.timestamp,
            item.symbol,
            item.side,
            item.size,
            item.price,
            item.tickDirection,
            item.trdMatchID,
            item.grossValue,
            item.homeNotional,
            item.foreignNotional
        ]);
        if (buffer.length > 512) {
            // let dbConn = new mysql.createConnection(config.mysql);
            sql = sprintf("INSERT INTO `orders`(`timestamp`, `symbol`, `side`, `size`, `price`, `tickDirection`, `trdMatchID`, `grossValue`, `homeNotional`, `foreignNotional`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `symbol` = VALUES(`symbol`), `side` = VALUES(`side`), `size` = VALUES(`size`), `price` = VALUES(`price`), `tickDirection` = VALUES(`tickDirection`), `trdMatchID` = VALUES(`trdMatchID`), `grossValue` = VALUES(`grossValue`), `homeNotional` = VALUES(`homeNotional`), `foreignNotional` = VALUES(`foreignNotional`);");
            // sql = sprintf("INSERT INTO `hidden_orders` SET ?");
            // console.log(sql);
            dbConn.query(sql, [buffer], (error, results, fields) => {
                if (error) {
                    // console.log(error);
                    console.log('commitOrdersData', 'order');
                    // dbConn = null;
                } else {
                }
                // dbConn.end(function (err) {
                //     // The connection is terminated now
                // });
            });
            buffer = [];
            // commitFlag = true;
        }
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
    // ordersBuffer = [];
    // orderIDs = [];
    if (buffer.length > 0) {

        // let dbConn = new mysql.createConnection(config.mysql);
        sql = sprintf("INSERT INTO `orders`(`timestamp`, `symbol`, `side`, `size`, `price`, `tickDirection`, `trdMatchID`, `grossValue`, `homeNotional`, `foreignNotional`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `symbol` = VALUES(`symbol`), `side` = VALUES(`side`), `size` = VALUES(`size`), `price` = VALUES(`price`), `tickDirection` = VALUES(`tickDirection`), `trdMatchID` = VALUES(`trdMatchID`), `grossValue` = VALUES(`grossValue`), `homeNotional` = VALUES(`homeNotional`), `foreignNotional` = VALUES(`foreignNotional`);");
        // sql = sprintf("INSERT INTO `hidden_orders` SET ?");
        // console.log(sql);
        dbConn.query(sql, [buffer], (error, results, fields) => {
            if (error) {
                // console.log(error);
                console.log('commitOrdersData', 'order');
                // dbConn = null;
            } else {
            }
            // dbConn.end(function (err) {
            //     // The connection is terminated now
            // });
        });
        // commitFlag = true;
    }

    buffer = [];
    while (hiddenOrdersBuffer.length) {
        item = hiddenOrdersBuffer.shift();
        hiddenOrderIDs = hiddenOrderIDs.filter(function(value, index, arr) {
            return value != item.trdMatchID;
        });
        buffer.push([
            item.timestamp,
            item.symbol,
            item.side,
            item.size,
            item.price,
            item.tickDirection,
            item.trdMatchID,
            item.grossValue,
            item.homeNotional,
            item.foreignNotional
        ]);
        if (buffer.length > 512) {
            // let dbConn = new mysql.createConnection(config.mysql);
            sql = sprintf("INSERT INTO `hidden_orders`(`timestamp`, `symbol`, `side`, `size`, `price`, `tickDirection`, `trdMatchID`, `grossValue`, `homeNotional`, `foreignNotional`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `symbol` = VALUES(`symbol`), `side` = VALUES(`side`), `size` = VALUES(`size`), `price` = VALUES(`price`), `tickDirection` = VALUES(`tickDirection`), `trdMatchID` = VALUES(`trdMatchID`), `grossValue` = VALUES(`grossValue`), `homeNotional` = VALUES(`homeNotional`), `foreignNotional` = VALUES(`foreignNotional`);");
            // sql = sprintf("INSERT INTO `hidden_orders` SET ?");
            // console.log(sql);
            dbConn.query(sql, [buffer], (error, results, fields) => {
                if (error) {
                    // console.log(error);
                    console.log('commitOrdersData', 'hiddenOrder');
                    // dbConn = null;
                } else {
                }
                // dbConn.end(function (err) {
                //     // The connection is terminated now
                // });
            });
            buffer = [];

            // commitFlag = true;
        }
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
    // hiddenOrdersBuffer = [];
    // hiddenOrderIDs = [];

    if (buffer.length > 0) {
        // let dbConn = new mysql.createConnection(config.mysql);
        sql = sprintf("INSERT INTO `hidden_orders`(`timestamp`, `symbol`, `side`, `size`, `price`, `tickDirection`, `trdMatchID`, `grossValue`, `homeNotional`, `foreignNotional`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `symbol` = VALUES(`symbol`), `side` = VALUES(`side`), `size` = VALUES(`size`), `price` = VALUES(`price`), `tickDirection` = VALUES(`tickDirection`), `trdMatchID` = VALUES(`trdMatchID`), `grossValue` = VALUES(`grossValue`), `homeNotional` = VALUES(`homeNotional`), `foreignNotional` = VALUES(`foreignNotional`);");
        // sql = sprintf("INSERT INTO `hidden_orders` SET ?");
        // console.log(sql);
        let query = dbConn.query(sql, [buffer], (error, results, fields) => {
            if (error) {
                // console.log(error);
                console.log('commitOrdersData', 'hiddenOrder');
                // dbConn = null;
            } else {
            }
            // dbConn.end(function (err) {
            //     // The connection is terminated now
            // });

            // if (commitTimeoutId != null) {
            //     clearTimeout(commitTimeoutId);
            // }
            // commitTimeoutId = setTimeout(service.commitOrdersData, 60000);
        });
        // commitFlag = true;
        // console.log(query.sql);
    }
    // if (!commitFlag) {
        if (commitTimeoutId != null) {
            clearTimeout(commitTimeoutId);
        }
        commitTimeoutId = setTimeout(service.commitOrdersData, 30000);
    // }
    console.log('buffer-length-end', ordersBuffer.length, hiddenOrdersBuffer.length);
};

service.commitVolumeData = function() {
    console.log('commitVolumeData', volumeTimestamp);
    let sql;
    let timestamp;
    if (++volumeCnt5m > 5) {
        timestamp = new Date(volumeTimestamp);
        timestamp.setMinutes(Math.floor(timestamp.getMinutes() / 5) * 5, 0, 0);
        volumeCnt5m = 0;
        sql = sprintf("INSERT INTO volume_5m(`timestamp`, `volume`) VALUES ('%s', '%f') ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `volume` = VALUES(`volume`);", timestamp.toISOString(), calcedVolume5m);
        calcedVolume5m = 0;
        dbConn.query(sql, null, (error, results, fields) => {
            if (error) {
                console.log(error);
                // dbConn = null;
            } else {

            }
            // dbConn1.end(function (err) {
            //     // The connection is terminated now
            // });
        });
    }
    if (++volumeCnt1h > 60) {
        timestamp = new Date(volumeTimestamp);
        timestamp.setHours(timestamp.getHours(), 0, 0, 0);
        volumeCnt1h = 0;
        sql = sprintf("INSERT INTO volume_1h(`timestamp`, `volume`) VALUES ('%s', '%f') ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `volume` = VALUES(`volume`);", timestamp.toISOString(), calcedVolume1h);
        calcedVolume1h = 0;
        dbConn.query(sql, null, (error, results, fields) => {
            if (error) {
                console.log(error);
                // dbConn = null;
            } else {

            }
            // dbConn1.end(function (err) {
            //     // The connection is terminated now
            // });
        });
    }
    timestamp = new Date(volumeTimestamp);
    timestamp.setMinutes(timestamp.getMinutes(), 0, 0);
    sql = sprintf("INSERT INTO volume_1m(`timestamp`, `volume`) VALUES ('%s', '%f') ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `volume` = VALUES(`volume`);", timestamp.toISOString(), calcedVolume1m);
    calcedVolume1m = 0;
    dbConn.query(sql, null, (error, results, fields) => {
        if (error) {
            console.log(error);
            // dbConn = null;
        } else {

        }
        console.log('commitVolumeData', volumeTimestamp);
        if (commitVolumeId != null) {
            clearTimeout(commitVolumeId);
        }
        commitVolumeId = setTimeout(service.commitVolumeData, 60000);
        console.log('commitVolumeData', 60000);
        // dbConn1.end(function (err) {
        //     // The connection is terminated now
        // });
    });
};

service.calculateFFT = function(binSize, startTime) {
    // let dbConn = new mysql.createConnection(config.mysql);
    let sql = sprintf("SELECT * FROM (SELECT `timestamp`, `open`, `high`, `low`, `close` FROM `bitmex_data_%s_view` WHERE `timestamp` <= '%s' ORDER BY `timestamp` DESC LIMIT 500) `tmp` ORDER BY `timestamp`;", binSize, startTime);
    // let sql = sprintf("SELECT * FROM (SELECT `id`, `timestamp`, IFNULL(`open`, 0) `open`, IFNULL(`high`, 0) `high`, IFNULL(`low`, 0) `low`, IFNULL(`close`, 0) `close` FROM `bitmex_data_%s_view` WHERE `timestamp` BETWEEN '2015-09-25T12:05:00.000Z' AND '2019-09-31T23:59:00.100Z' ORDER BY `timestamp` DESC) `tmp` ORDER BY `timestamp`;", binSize);
    // sql = sprintf("INSERT INTO `hidden_orders` SET ?");
    // console.log(sql);
    dbConn.query(sql, null, (error, results, fields) => {
        if (error) {
            console.log(error);
            // dbConn = null;
        } else {
            let calced = [];
            // let ids = [];
            let timestamps = [];
            let open = [];
            let high = [];
            let low = [];
            let close = [];
            let maxChange = [];
            let lowPass = [];
            let highPass = [];
            let maxChange1;
            if (results != null && results.length > 0) {
                for (let i = 0; i < 100; i++) {
                    // calced.push(results[0]);
                    // ids.push(results[0].id);
                    timestamps.push(results[0].timestamp);
                    open.push(results[0].open);
                    high.push(results[0].high);
                    low.push(results[0].low);
                    close.push(results[0].close);
                    maxChange1 = (parseFloat(results[0].high) - parseFloat(results[0].low)) / parseFloat(results[0].close);
                    if (isNaN(maxChange1)) {
                        maxChange1 = 0
                    }
                    maxChange.push(maxChange1);
                }
                // calced = calced.concat(results);
                for (let item of results) {
                    // calced.push(item);
                    // ids.push(item.id);
                    timestamps.push(item.timestamp);
                    open.push(item.open);
                    high.push(item.high);
                    low.push(item.low);
                    close.push(item.close);
                    maxChange1 = ((parseFloat(item.high) - parseFloat(item.low)) / parseFloat(item.close));
                    if (isNaN(maxChange1)) {
                        maxChange1 = 0
                    }
                    maxChange.push(maxChange1);
                }
                const resultLast = results.length - 1;
                for (let i = 0; i < 100; i++) {
                    // calced.push(results[resultLast]);
                    // ids.push(results[resultLast].id);
                    timestamps.push(results[resultLast].timestamp);
                    open.push(results[resultLast].open);
                    high.push(results[resultLast].high);
                    low.push(results[resultLast].low);
                    close.push(results[resultLast].close);
                    maxChange1 = ((parseFloat(results[resultLast].high) - parseFloat(results[resultLast].low)) / parseFloat(results[resultLast].close));
                    if (isNaN(maxChange1)) {
                        maxChange1 = 0
                    }
                    maxChange.push(maxChange1);
                }

                var iirCalculator = new Fili.CalcCascades();

                var lowpassFilterCoeffs = iirCalculator.lowpass({
                    order: 3, // cascade 3 biquad filters (max: 12)
                    characteristic: 'butterworth',
                    Fs: 800, // sampling frequency
                    Fc: 80, // cutoff frequency / center frequency for bandpass, bandstop, peak
                    BW: 1, // bandwidth only for bandstop and bandpass filters - optional
                    gain: 0, // gain for peak, lowshelf and highshelf
                    preGain: false // adds one constant multiplication for highpass and lowpass
                    // k = (1 + cos(omega)) * 0.5 / k = 1 with preGain == false
                });

                var iirLowpassFilter = new Fili.IirFilter(lowpassFilterCoeffs);

                lowPass = iirLowpassFilter.multiStep(maxChange);

                var highpassFilterCoeffs = iirCalculator.highpass({
                    order: 3, // cascade 3 biquad filters (max: 12)
                    characteristic: 'butterworth',
                    Fs: 800, // sampling frequency
                    Fc: 80, // cutoff frequency / center frequency for bandpass, bandstop, peak
                    BW: 1, // bandwidth only for bandstop and bandpass filters - optional
                    gain: 0, // gain for peak, lowshelf and highshelf
                    preGain: false // adds one constant multiplication for highpass and lowpass
                    // k = (1 + cos(omega)) * 0.5 / k = 1 with preGain == false
                });

                var iirHighpassFilter = new Fili.IirFilter(highpassFilterCoeffs);
                highPass = iirHighpassFilter.multiStep(maxChange);
            }
            if (timestamps.length == 0) {
                return;
            }
            for (let i = 100; i < timestamps.length - 100; i++) {
                // calced.shift();
                // calced.pop();
                // ids.shift();
                // ids.pop();
                // lowPass.shift();
                // lowPass.pop();
                // highPass.shift();
                // highPass.pop();
                calced.push([
                    timestamps[i],
                    lowPass[i],
                    highPass[i]
                ])
            }
            // console.log(lowPass);
            let sql = sprintf("INSERT INTO `bitmex_data_%s`(`timestamp`, `lowPass`, `highPass`) VALUES ? ON DUPLICATE KEY UPDATE `lowPass` = VALUES(`lowPass`), `highPass` = VALUES(`highPass`);", binSize);
            let buffer = [];
            for (let item of calced) {
                buffer.push(item);
                if (buffer.length > 512) {
                    // let dbConn1 = new mysql.createConnection(config.mysql);
                    let query = dbConn.query(sql, [buffer], (error, results, fields) => {
                        if (error) {
                            console.log(error);
                            // dbConn = null;
                        } else {
                        }

                    });
                    buffer = [];
                    // dbConn1.end(function (err) {
                    //     // The connection is terminated now
                    // });
                }
            }
            if (buffer.length > 0) {
                // let dbConn1 = new mysql.createConnection(config.mysql);
                let query = dbConn.query(sql, [buffer], (error, results, fields) => {
                    if (error) {
                        console.log(error);
                        // dbConn = null;
                    } else {

                    }
                    // dbConn1.end(function (err) {
                    //     // The connection is terminated now
                    // });
                });
            }
            // console.log('sql', query.sql);
        }
    });
};


let id0Timeout = [];
service.saveId0Service = function (interval) {
    _calculateId0(interval, results => {
        // console.log(results);
        if (results.result && results.result == 'error') {
            console.log('saveId0Service-error', interval);
        } else {
            let sql = sprintf("SELECT COUNT(`timestamp`) `count` FROM `id0_%s` WHERE `timestamp` = '%s';", interval, results.timestamp);

            dbConn.query(sql, null, (error, resultsNoUse, fields) => {
                if (error) {
                    console.log(error);
                    return;
                }
                // console.log(resultsNoUse[0].count);
                // return;
                if (resultsNoUse[0].count > 0) {
                    return;
                }
                sql = sprintf("INSERT INTO `id0_%s` (`timestamp`, `open`, `high`, `low`, `close`, `num_3`, `num_3i`, `num_6`, `num_6i`, `num_9`, `num_9i`, `num_100`, `num_100i`) VALUES ('%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", interval, results.timestamp, results.open, results.high, results.low, results.close, results.num_3, results.num_3i, results.num_6, results.num_6i, results.num_9, results.num_9i, results.num_100, results.num_100i);
                // console.log(sql);
                dbConn.query(sql, null, (error, results, fields) => {
                    if (error) {
                        console.log('saveId0Service-save-error', error);
                    } else {
                        console.log('saveId0Service-save-success', interval);
                    }
                });
            });
        }
        if (id0Timeout[interval] != null) {
            clearTimeout(id0Timeout[interval]);
        }
        id0Timeout[interval] = setTimeout(service.saveId0Service, 60000, interval);
    });
};

function _calculateId0(interval, callback) {
    const acceptInterval = ['1m', '5m', '1h'];
    if (acceptInterval.indexOf(interval) === -1) {
        if (callback) {
            callback({
                result: 'error',
                data: 'binSize error',
            });
        }
    }
    let sql = sprintf("SELECT * FROM (SELECT `timestamp`, `date`, IFNULL(`open`, 0) `open`, IFNULL(`high`, 0) `high`, IFNULL(`low`, 0) `low`, IFNULL(`close`, 0) `close` FROM `bitmex_data_%s_view` ORDER BY `timestamp` DESC LIMIT 2000) `sub` ORDER BY `timestamp` ASC;", interval);
    // console.log(sql);
    dbConn.query(sql, null, (error, results, fields) => {
        if (error) {
            console.log(error);
            if (callback) {
                callback({
                    result: 'error',
                    data: 'internal server error',
                });
            }
        }
        if (results == null) {
            if (callback) {
                callback({
                    result: 'error',
                    data: 'no data',
                });
            }
        }
        let resultCnt = results.length;
        if (results.length < 2048) {
            const cnt = resultCnt;
            const last = results[cnt - 1];
            for (let i = cnt; i < 2048; i++) {
                results.push(last);
            }
        }
        // let dates = [];
        let opens = [];
        for (let item of results) {
            // dates.push(item.date);
            opens.push(item.open);
        }
        let fft = fftJs.fft(opens);
        const cnts = [3, 6, 9, 100];
        let buffer;
        let iffts = new Map();
        for (let cnt of cnts) {
            let i;
            const cnt2 = 2048 - cnt;
            let ifft;
            buffer = [];
            for (i = 0; i < cnt; i++) {
                buffer.push(fft[i]);
            }
            for (i = cnt; i < cnt2; i++) {
                buffer.push([0, 0]);
            }
            for (i = cnt2; i < 2048; i++) {
                buffer.push(fft[i]);
            }
            ifft = fftJs.ifft(buffer);
            // console.log(ifft[0][0]);
            iffts.set('ifft' + cnt, ifft);
        }
        // console.log(iffts);
        // console.log(iffts.get('ifft3'));
        let ifft3 = iffts.get('ifft3');
        let ifft6 = iffts.get('ifft6');
        let ifft9 = iffts.get('ifft9');
        let ifft100 = iffts.get('ifft100');
        const finalIdx = resultCnt - 1;
        const final = {
            id: 0,
            timestamp: results[finalIdx].timestamp,
            open: results[finalIdx].open,
            high: results[finalIdx].high,
            low: results[finalIdx].low,
            close: results[finalIdx].close,
            num_3: ifft3[finalIdx][0],
            num_3i: ifft3[finalIdx][1],
            num_6: ifft6[finalIdx][0],
            num_6i: ifft6[finalIdx][1],
            num_9: ifft9[finalIdx][0],
            num_9i: ifft9[finalIdx][1],
            num_100: ifft100[finalIdx][0],
            num_100i: ifft100[finalIdx][1],
        };
        if (callback) {
            callback(final);
        }
    });
}

function power_of_2(n) {
    if (typeof n !== 'number')
        return 'Not a number';

    return n && (n & (n - 1)) === 0;
}


module.exports = service;
