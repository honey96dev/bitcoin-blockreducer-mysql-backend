const request = require('request');
const dbConn = require('./../_core/dbConn');
const sprintfJs = require('sprintf-js');

const sprintf = sprintfJs.sprintf,
    vsprintf = sprintfJs.vsprintf;

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
                // console.log('5s');
                return;
            }
            setTimeout(service.downloadBitmexData, 5 * 60000, binSize, startTime);
            // console.log('5m');
        });
    } catch (e) {
        setTimeout(service.downloadBitmexData, 15 * 60000, binSize, startTime);
    }
};

service.getLastTimestamp = function (binSize, callback) {
    const sql = sprintf("SELECT `timestamp` FROM `bitmex_data_%s` ORDER BY `timestamp` DESC LIMIT 0, 1;", binSize);
    dbConn.query(sql, null, (error, results, fields) => {
        if (error) {
            console.log(error)
        }
        // console.log(results);
        callback(results);
    });
};

module.exports = service;
