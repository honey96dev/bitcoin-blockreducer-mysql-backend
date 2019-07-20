const dbConn = require('./../_core/dbConn');
const mysql = require('mysql2');
const config = require('../_core/config');
const Fili = require('fili');
const sprintfJs = require('sprintf-js');
const sprintf = sprintfJs.sprintf,
    vsprintf = sprintfJs.vsprintf;
const fftJs = require('fft-js');

let service = {};

let timeoutId = {};
let timeoutDelay = 30000;

service.calculateFFT = (interval, timestamp) => {
    if (timestamp.length === 0) {
        if (interval == '5m') {
            timestamp = '2015-09-25T12:05:00.000Z';
        } else if (interval == '1h') {
            timestamp = '2015-09-25T13:00:00.000Z';
        }
    }
    if (timeoutId[interval]) {
        clearTimeout(timeoutId[interval]);
    }
    let timeStep;
    if (interval == '5m') {
        timeStep = 5 * 60 * 1000;
    } else if (interval == '1h') {
        timeStep = 60 * 60 * 1000;
    }
    timestamp = new Date(new Date(timestamp).getTime() - timeStep * 500).toISOString();
    let sql = sprintf("SELECT * FROM `bitmex_data_%s` WHERE `timestamp` > '%s' ORDER BY `timestamp` LIMIT 1000;", interval, timestamp);
    console.log('calculateFFT', sql);
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
                // for (let i = 0; i < 100; i++) {
                //     // calced.push(results[0]);
                //     // ids.push(results[0].id);
                //     timestamps.push(results[0].timestamp);
                //     open.push(results[0].open);
                //     high.push(results[0].high);
                //     low.push(results[0].low);
                //     close.push(results[0].close);
                //     maxChange1 = (parseFloat(results[0].high) - parseFloat(results[0].low)) / parseFloat(results[0].close);
                //     if (isNaN(maxChange1)) {
                //         maxChange1 = 0
                //     }
                //     maxChange.push(maxChange1);
                // }
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

                    timestamp = item.timestamp;
                }
                const resultLast = results.length - 1;
                let lastTime = new Date(results[resultLast].timestamp);
                let timeStep = 0;
                if (interval === '1m') {
                    timeStep = 60000;
                } else if (interval === '5m') {
                    timeStep = 300000;
                } else if (interval === '1h') {
                    timeStep = 3600000;
                }
                for (let i = 0; i < 100; i++) {
                    // calced.push(results[resultLast]);
                    // ids.push(results[resultLast].id);
                    lastTime = new Date(lastTime.getTime() + timeStep);
                    timestamps.push(lastTime.toISOString());
                    // timestamps.push(results[resultLast].timestamp);
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
            for (let i = 0; i < timestamps.length - 200; i++) {
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
                    open[i],
                    high[i],
                    low[i],
                    close[i],
                    lowPass[i],
                    highPass[i]
                ])
            }
            // console.log(lowPass);
            let sql = sprintf("INSERT INTO `fft_%s`(`timestamp`, `open`, `high`, `low`, `close`, `lowPass`, `highPass`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `open` = VALUES(`open`), `open` = VALUES(`open`), `high` = VALUES(`high`), `low` = VALUES(`low`), `close` = VALUES(`close`), `highPass` = VALUES(`highPass`);", interval);
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
        timeoutId[interval] = setTimeout(service.calculateFFT, timeoutDelay, interval, timestamp);
    });
};

module.exports = service;
