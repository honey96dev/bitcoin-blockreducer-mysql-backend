const request = require('request');
const dbConn = require('./../_core/dbConn');
const bitmexClient = require('./../_core/bitmexConn');
var mysql = require('mysql2');
var config = require('../_core/config');
var Fili = require('fili');
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
let downloadBitmexTimeoutId = new Map();
let commitTimeoutId = null;
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
                    let dbConn = new mysql.createConnection(config.mysql);
                    // dbConn.connect(error => {
                    //     if (!error) {
                    //     }
                    dbConn.query(sql, [rows], (error, results, fields) => {
                        if (error) {
                            console.log(error);
                            dbConn = null;
                        } else {
                            dbConn.end(function (err) {
                                // The connection is terminated now
                            });
                            service.calculateFFT(binSize, lastTimestamp);
                        }
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
                    if (hiddenOrderIDs.indexOf(item.trdMatchID) === -1) {
                        hiddenOrderIDs.push(item.trdMatchID);
                        hiddenOrdersBuffer.push(item);
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
                    }
                    // console.log('Hidden Buy ', highestBuy, item.price, lowestSell, item.trdMatchID, JSON.stringify(item));
                }
            }

        }
    });
};

service.commitData = function() {
    // let commitFlag = false;
    let sql;
    let item;
    let buffer = [];
    console.log('buffer-length-start', ordersBuffer.length, hiddenOrdersBuffer.length);
    while (ordersBuffer.length) {
        item = ordersBuffer.shift();
        // orderIDs = orderIDs.filter(function(value, index, arr) {
        //     return value == item.trdMatchID;
        // });
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
            let dbConn = new mysql.createConnection(config.mysql);
            sql = sprintf("INSERT INTO `orders`(`timestamp`, `symbol`, `side`, `size`, `price`, `tickDirection`, `trdMatchID`, `grossValue`, `homeNotional`, `foreignNotional`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `symbol` = VALUES(`symbol`), `side` = VALUES(`side`), `size` = VALUES(`size`), `price` = VALUES(`price`), `tickDirection` = VALUES(`tickDirection`), `trdMatchID` = VALUES(`trdMatchID`), `grossValue` = VALUES(`grossValue`), `homeNotional` = VALUES(`homeNotional`), `foreignNotional` = VALUES(`foreignNotional`);");
            // sql = sprintf("INSERT INTO `hidden_orders` SET ?");
            // console.log(sql);
            dbConn.query(sql, [buffer], (error, results, fields) => {
                if (error) {
                    // console.log(error);
                    console.log('commitData', 'order');
                    // dbConn = null;
                } else {
                }
                dbConn.end(function (err) {
                    // The connection is terminated now
                });
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
    ordersBuffer = [];
    orderIDs = [];
    if (buffer.length > 0) {

        let dbConn = new mysql.createConnection(config.mysql);
        sql = sprintf("INSERT INTO `orders`(`timestamp`, `symbol`, `side`, `size`, `price`, `tickDirection`, `trdMatchID`, `grossValue`, `homeNotional`, `foreignNotional`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `symbol` = VALUES(`symbol`), `side` = VALUES(`side`), `size` = VALUES(`size`), `price` = VALUES(`price`), `tickDirection` = VALUES(`tickDirection`), `trdMatchID` = VALUES(`trdMatchID`), `grossValue` = VALUES(`grossValue`), `homeNotional` = VALUES(`homeNotional`), `foreignNotional` = VALUES(`foreignNotional`);");
        // sql = sprintf("INSERT INTO `hidden_orders` SET ?");
        // console.log(sql);
        dbConn.query(sql, [buffer], (error, results, fields) => {
            if (error) {
                // console.log(error);
                console.log('commitData', 'order');
                // dbConn = null;
            } else {
            }
            dbConn.end(function (err) {
                // The connection is terminated now
            });
        });
        // commitFlag = true;
    }

    buffer = [];
    while (hiddenOrdersBuffer.length) {
        item = hiddenOrdersBuffer.shift();
        // hiddenOrderIDs = hiddenOrderIDs.filter(function(value, index, arr) {
        //     return value == item.trdMatchID;
        // });
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
            let dbConn = new mysql.createConnection(config.mysql);
            sql = sprintf("INSERT INTO `hidden_orders`(`timestamp`, `symbol`, `side`, `size`, `price`, `tickDirection`, `trdMatchID`, `grossValue`, `homeNotional`, `foreignNotional`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `symbol` = VALUES(`symbol`), `side` = VALUES(`side`), `size` = VALUES(`size`), `price` = VALUES(`price`), `tickDirection` = VALUES(`tickDirection`), `trdMatchID` = VALUES(`trdMatchID`), `grossValue` = VALUES(`grossValue`), `homeNotional` = VALUES(`homeNotional`), `foreignNotional` = VALUES(`foreignNotional`);");
            // sql = sprintf("INSERT INTO `hidden_orders` SET ?");
            // console.log(sql);
            dbConn.query(sql, [buffer], (error, results, fields) => {
                if (error) {
                    // console.log(error);
                    console.log('commitData', 'hiddenOrder');
                    // dbConn = null;
                } else {
                }
                dbConn.end(function (err) {
                    // The connection is terminated now
                });
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
    hiddenOrdersBuffer = [];
    hiddenOrderIDs = [];

    if (buffer.length > 0) {
        let dbConn = new mysql.createConnection(config.mysql);
        sql = sprintf("INSERT INTO `hidden_orders`(`timestamp`, `symbol`, `side`, `size`, `price`, `tickDirection`, `trdMatchID`, `grossValue`, `homeNotional`, `foreignNotional`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `symbol` = VALUES(`symbol`), `side` = VALUES(`side`), `size` = VALUES(`size`), `price` = VALUES(`price`), `tickDirection` = VALUES(`tickDirection`), `trdMatchID` = VALUES(`trdMatchID`), `grossValue` = VALUES(`grossValue`), `homeNotional` = VALUES(`homeNotional`), `foreignNotional` = VALUES(`foreignNotional`);");
        // sql = sprintf("INSERT INTO `hidden_orders` SET ?");
        // console.log(sql);
        let query = dbConn.query(sql, [buffer], (error, results, fields) => {
            if (error) {
                // console.log(error);
                console.log('commitData', 'hiddenOrder');
                // dbConn = null;
            } else {
            }
            dbConn.end(function (err) {
                // The connection is terminated now
            });

            // if (commitTimeoutId != null) {
            //     clearTimeout(commitTimeoutId);
            // }
            // commitTimeoutId = setTimeout(service.commitData, 60000);
        });
        // commitFlag = true;
        // console.log(query.sql);
    }
    // if (!commitFlag) {
        if (commitTimeoutId != null) {
            clearTimeout(commitTimeoutId);
        }
        commitTimeoutId = setTimeout(service.commitData, 60000);
    // }
    console.log('buffer-length-end', ordersBuffer.length, hiddenOrdersBuffer.length);
};

service.calculateFFT = function(binSize, startTime) {
    let dbConn = new mysql.createConnection(config.mysql);
    let sql = sprintf("SELECT * FROM (SELECT `timestamp`, `open`, `high`, `low`, `close` FROM `bitmex_data_%s_view` WHERE `timestamp` <= '%s' ORDER BY `timestamp` DESC LIMIT 500) `tmp` ORDER BY `timestamp`;", binSize, startTime);
    // let sql = sprintf("SELECT * FROM (SELECT `id`, `timestamp`, IFNULL(`open`, 0) `open`, IFNULL(`high`, 0) `high`, IFNULL(`low`, 0) `low`, IFNULL(`close`, 0) `close` FROM `bitmex_data_%s_view` WHERE `timestamp` BETWEEN '2015-09-25T12:05:00.000Z' AND '2019-09-31T23:59:00.100Z' ORDER BY `timestamp` DESC) `tmp` ORDER BY `timestamp`;", binSize);
    // sql = sprintf("INSERT INTO `hidden_orders` SET ?");
    // console.log(sql);
    dbConn.query(sql, null, (error, results, fields) => {
        if (error) {
            console.log(error);
            dbConn = null;
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
                    let dbConn1 = new mysql.createConnection(config.mysql);
                    let query = dbConn1.query(sql, [buffer], (error, results, fields) => {
                        if (error) {
                            console.log(error);
                            // dbConn = null;
                        } else {
                        }

                    });
                    buffer = [];
                    dbConn1.end(function (err) {
                        // The connection is terminated now
                    });
                }
            }
            let dbConn1 = new mysql.createConnection(config.mysql);
            let query = dbConn1.query(sql, [buffer], (error, results, fields) => {
                if (error) {
                    console.log(error);
                    // dbConn = null;
                } else {

                }
                dbConn1.end(function (err) {
                    // The connection is terminated now
                });
            });
            // console.log('sql', query.sql);
        }
    });
};

module.exports = service;
