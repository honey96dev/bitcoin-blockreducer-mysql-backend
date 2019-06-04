const dbConn = require('./../_core/dbConn');
const config = require('../_core/config');
const Fili = require('fili');
const sprintfJs = require('sprintf-js');
const sprintf = sprintfJs.sprintf,
    vsprintf = sprintfJs.vsprintf;
const fftJs = require('fft-js');

let hiddenOrderCalcIntervalId = undefined;

const calculate = (timestamp) => {
    if (hiddenOrderCalcIntervalId) {
        clearTimeout(hiddenOrderCalcIntervalId);
    }
    let sql = sprintf("SELECT '%s' `timestamp`, H.symbol, H.side, SUM(H.size) `size`, SUM(H.price) `price`, H.tickDirection, H.trdMatchID, SUM(H.grossValue) `grossValue`, SUM(H.homeNotional) `homeNotional`, SUM(H.foreignNotional) `foreignNotional` FROM `hidden_orders` H WHERE `timestamp` LIKE '%s%s' GROUP BY `side`;", timestamp, timestamp.substr(0, 16), '%');
    console.log(sql);

    // hiddenOrderCalcIntervalId = setTimeout(calculate, 1000, timestamp);
    dbConn.query(sql, undefined, (error, results, fields) => {
        if (error) {
            console.log(error);
            hiddenOrderCalcIntervalId = setTimeout(calculate, 30000, timestamp);
            return;
        }
        if (results.length > 0) {
            // sql = sprintf("INSERT INTO `hidden_orders2`(`timestamp`, `symbol`, `side`, `size`, `price`, `tickDirection`, `trdMatchID`, `grossValue`, `homeNotional`, `foreignNotional`) VALUES ('%s', '%s', '%s', '%f', '%f', '%s', '%s', '%f', '%f', '%f') ON DUPLICATE KEY UPDATE `symbol` = VALUES(`symbol`), `side` = VALUES(`side`), `size` = VALUES(`size`), `price` = VALUES(`price`), `tickDirection` = VALUES(`tickDirection`), `trdMatchID` = VALUES(`trdMatchID`), `grossValue` = VALUES(`grossValue`), `homeNotional` = VALUES(`homeNotional`), `foreignNotional` = VALUES(`foreignNotional`);", timestamp, row.symbol, row.side, row.size, row.price, row.tickDirection, row.trdMatchID, row.grossValue, row.homeNotional, row.foreignNotional);
            let rows = [];
            for (let item of results) {
                rows.push([
                    item.timestamp + ":" + item.side,
                    item.timestamp,
                    item.symbol,
                    item.side,
                    item.size,
                    item.price,
                    item.tickDirection,
                    item.trdMatchID,
                    item.grossValue,
                    item.homeNotional,
                    item.foreignNotional,
                ]);
            }
            sql = sprintf("INSERT INTO `hidden_orders2`(`id`, `timestamp`, `symbol`, `side`, `size`, `price`, `tickDirection`, `trdMatchID`, `grossValue`, `homeNotional`, `foreignNotional`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `symbol` = VALUES(`symbol`), `side` = VALUES(`side`), `size` = VALUES(`size`), `price` = VALUES(`price`), `tickDirection` = VALUES(`tickDirection`), `trdMatchID` = VALUES(`trdMatchID`), `grossValue` = VALUES(`grossValue`), `homeNotional` = VALUES(`homeNotional`), `foreignNotional` = VALUES(`foreignNotional`);");
            console.log(JSON.stringify(rows));
            dbConn.query(sql, [rows], (error, results, query) => {
                if (error) {
                    console.log(error);
                    hiddenOrderCalcIntervalId = setTimeout(calculate, 30000, timestamp);
                } else {
                    timestamp = new Date(new Date(timestamp).getTime() + 60000).toISOString();
                    hiddenOrderCalcIntervalId = setTimeout(calculate, 30000, timestamp);
                }
            });
        } else {
            timestamp = new Date(new Date(timestamp).getTime() + 60000).toISOString();
            hiddenOrderCalcIntervalId = setTimeout(calculate, 30000, timestamp);
        }
    });
};

let sql = "SELECT `timestamp` FROM `hidden_orders2` ORDER BY `timestamp` DESC LIMIT 1;";
dbConn.query(sql, undefined, (error, results, fields) => {
    if (error || results.length == 0) {
        calculate('2019-05-04T12:55:00.000Z');
    } else {
        let timestamp = new Date(new Date(results[0].timestamp).getTime() + 60000);
        timestamp.setSeconds(0, 0);
        calculate(timestamp.toISOString());
    }
});
