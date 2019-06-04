const dbConn = require('./../_core/dbConn');
const config = require('../_core/config');
const Fili = require('fili');
const sprintfJs = require('sprintf-js');
const sprintf = sprintfJs.sprintf,
    vsprintf = sprintfJs.vsprintf;
const fftJs = require('fft-js');

let intervalId = {};

const calculate = (interval, timestamp) => {
    const delay = 500;
    if (typeof intervalId[interval] !== 'undefined') {
        clearTimeout(intervalId[interval]);
    }
    // let sql = sprintf("SELECT `timestamp` FROM `id0_%s_` ORDER BY `timestamp` DESC LIMIT 1;", interval);
    // dbConn.query(sql, undefined, (error, results, fields) => {
    //     if (error) {
    //         console.log(error);
    //         intervalId[interval] = setTimeout(calculate, delay, interval, timestamp);
    //         return;
    //     }
        let id0LastTimestamp = '';
        if (timestamp.length > 0) {
            // id0LastTimestamp = results[0].timestamp;
            let timeStep = 0;
            if (interval === '1m') {
                timeStep = 60000;
            } else if (interval === '5m') {
                timeStep = 300000;
            } else if (interval === '1h') {
                timeStep = 3600000;
            }
            id0LastTimestamp = new Date(new Date(timestamp).getTime() + timeStep).toISOString();
        } else {
            if (interval === '1m') {
                id0LastTimestamp = '2019-04-25T00:01:00.000Z';
                // id0LastTimestamp = '2019-04-25T00:01:00.000Z';
            } else if (interval === '5m') {
                id0LastTimestamp = '2015-09-25T12:05:00.000Z';
                // id0LastTimestamp = '2015-09-25T12:05:00.000Z';
            } else if (interval === '1h') {
                id0LastTimestamp = '2015-11-19T20:00:00.000Z';
                // id0LastTimestamp = '2015-09-25T13:00:00.000Z';
            }
        }

        sql = sprintf("SELECT * FROM (SELECT * FROM `bitmex_data_%s` WHERE `timestamp` <= '%s' ORDER BY `timestamp` DESC LIMIT 2000) `tmp` ORDER BY `timestamp` ASC;", interval, id0LastTimestamp);
        // console.log(sql);
        dbConn.query(sql, undefined, (error, results, fields) => {
            if (error) {
                console.log(error);
                intervalId[interval] = setTimeout(calculate, delay, interval, id0LastTimestamp);
                return;
            }
            // console.log(JSON.stringify(results));
            let resultCnt = results.length;
            if (results.length < 2048) {
                const cnt = resultCnt;
                let lastTime = new Date(results[cnt - 1].timestamp);
                let timeStep = 0;
                if (interval === '1m') {
                    timeStep = 60000;
                } else if (interval === '5m') {
                    timeStep = 300000;
                } else if (interval === '1h') {
                    timeStep = 3600000;
                }
                const last = results[cnt - 1];
                for (let i = cnt; i < 2048; i++) {
                    // results.push(last);
                    lastTime = new Date(lastTime.getTime() + timeStep);
                    results.push({
                        // timestamp: last.timestamp,
                        timestamp: lastTime.toISOString(),
                        symbol: last.symbol,
                        open: last.open,
                        high: last.high,
                        low: last.low,
                        close: last.close,
                        volume: last.volume,
                        lowPass: last.lowPass,
                        highPass: last.highPass,
                    });
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
            const row =[
                results[finalIdx].timestamp,
                results[finalIdx].open,
                results[finalIdx].high,
                results[finalIdx].low,
                results[finalIdx].close,
                ifft3[finalIdx][0],
                ifft3[finalIdx][1],
                ifft6[finalIdx][0],
                ifft6[finalIdx][1],
                ifft9[finalIdx][0],
                ifft9[finalIdx][1],
                ifft100[finalIdx][0],
                ifft100[finalIdx][1],
            ];
            // console.log(ifft100);
            // let sql = sprintf("INSERT INTO `id0_%s` (`timestamp`, `open`, `high`, `low`, `close`, `num_3`, `num_3i`, `num_6`, `num_6i`, `num_9`, `num_9i`, `num_100`, `num_100i`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `open` = VALUES(`open`), `high` = VALUES(`high`), `low` = VALUES(`low`), `close` = VALUES(`close`), `num_3` = VALUES(`num_3`), `num_3i` = VALUES(`num_3i`), `num_6` = VALUES(`num_6`), `num_6i` = VALUES(`num_6i`), `num_9` = VALUES(`num_9`), `num_9i` = VALUES(`num_9i`), `num_100` = VALUES(`num_100`), `num_100i` = VALUES(`num_100i`);", interval);
            let sql = sprintf("INSERT INTO `id0_%s_` (`timestamp`, `open`, `high`, `low`, `close`, `num_3`, `num_3i`, `num_6`, `num_6i`, `num_9`, `num_9i`, `num_100`, `num_100i`) VALUES ('%s', '%f', '%f', '%f', '%f', '%f', '%f', '%f', '%f', '%f', '%f', '%f', '%f') ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `open` = VALUES(`open`), `high` = VALUES(`high`), `low` = VALUES(`low`), `close` = VALUES(`close`), `num_3` = VALUES(`num_3`), `num_3i` = VALUES(`num_3i`), `num_6` = VALUES(`num_6`), `num_6i` = VALUES(`num_6i`), `num_9` = VALUES(`num_9`), `num_9i` = VALUES(`num_9i`), `num_100` = VALUES(`num_100`), `num_100i` = VALUES(`num_100i`);", interval, results[finalIdx].timestamp,
                results[finalIdx].open,
                results[finalIdx].high,
                results[finalIdx].low,
                results[finalIdx].close,
                ifft3[finalIdx][0],
                ifft3[finalIdx][1],
                ifft6[finalIdx][0],
                ifft6[finalIdx][1],
                ifft9[finalIdx][0],
                ifft9[finalIdx][1],
                ifft100[finalIdx][0],
                ifft100[finalIdx][1]);
            // console.log(sql);
            dbConn.query(sql, [row], (error, results2, fields) => {
                if (error) {
                    // console.log('saveId0Service-save-error', error);
                } else {
                    // console.log('saveId0Service-save-success', interval);
                }
                sql = "SELECT `timestamp` FROM `bitmex_data_%s` ORDER BY `timestamp` DESC LIMIT 1;";
                dbConn.query(sql, [row], (error, results5, fields) => {
                    if (error) {
                        // console.log('saveId0Service-save-error', error);
                    } else {
                        // console.log('saveId0Service-save-success', interval);
                        if (results5 && results5.length > 0) {
                            if (results5[0].timestamp == results[finalIdx].timestamp) {

                                console.warn(new Date(), interval, 'done');
                                return;
                            }
                        }
                    }
                    // console.log('interval-set', interval, new Date());
                    intervalId[interval] = setTimeout(calculate, delay, interval, id0LastTimestamp);
                });
                // intervalId[interval] = setTimeout(calculate, delay, interval, timestamp);
            });
        });
    // });
};

console.warn(new Date());
let sql = "SELECT `timestamp` FROM `id0_1m_` ORDER BY `timestamp` DESC LIMIT 1;";
dbConn.query(sql, undefined, (error, results, fields) => {
    if (error || results.length == 0) {
        calculate('1m', '');
    } else {
        calculate('1m', results[0].timestamp);
    }
});
sql = "SELECT `timestamp` FROM `id0_5m_` ORDER BY `timestamp` DESC LIMIT 1;";
dbConn.query(sql, undefined, (error, results, fields) => {
    if (error || results.length == 0) {
        calculate('5m', '');
    } else {
        calculate('5m', results[0].timestamp);
    }
});
sql = "SELECT `timestamp` FROM `id0_1h_` ORDER BY `timestamp` DESC LIMIT 1;";
dbConn.query(sql, undefined, (error, results, fields) => {
    if (error || results.length == 0) {
        calculate('1h', '');
    } else {
        calculate('1h', results[0].timestamp);
    }
});
// calculate('5m', '');
// calculate('1h', '');
