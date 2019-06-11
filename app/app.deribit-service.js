const request = require('request');
const dbConn = require('./../_core/dbConn');
const config = require('../_core/config');
const deribitConfig = config.deribit;
const Fili = require('fili');
const sprintfJs = require('sprintf-js');
const sprintf = sprintfJs.sprintf,
    vsprintf = sprintfJs.vsprintf;
const fftJs = require('fft-js');

let service = {
    delayInstruments: 30000,
    delayTicker: 0,

    timeoutInstrumentsId: undefined,
    timeoutTickersId: undefined,

    instrumentsBuffer: [],
    detailRowsBuffer: [],
    socket: undefined,
};

service.downloadDeribitInstruments = () => {
    if (service.timeoutInstrumentsId) {
        clearTimeout(service.timeoutInstrumentsId);
        service.timeoutInstrumentsId = undefined;
    }

    const baseUrl = deribitConfig.testnet ? deribitConfig.baseUrlTestnet : deribitConfig.baseUrlRealnet;
    // let url = sprintf("%s%s", baseUrl, deribitConfig.pathInstruments);
    let url = sprintf("%s%s?currency=BTC&kind=option&expired=false", baseUrl, deribitConfig.pathInstruments);
    // console.log(url);
    request(url, null, (error, response, body) => {
        if (error) {
            console.log(error);
            service.timeoutInstrumentsId = setTimeout(service.downloadDeribitInstruments, service.delayInstruments);
            return;
        }
        body = JSON.parse(body);
        const instruments = body.result;
        console.log('instruments', new Date(), instruments.length);
        // instrumentsCnt = instruments.length;
        // tikersCnt = 0;
        for (let instrument of instruments) {
            // console.log(JSON.stringify(instrument));
            // service.downloadTicker(instrument);
            service.instrumentsBuffer.push(instrument);
        }
        service.detailRowsBuffer = [];
        service.downloadTicker();
        // console.log(JSON.stringify(response), JSON.stringify(error));
        // timeoutInstrumentsId = setTimeout(service.downloadDeribitInstruments, delayInstruments);
    });
};

service.downloadTicker = () => {
    if (service.timeoutTickersId) {
        clearTimeout(service.timeoutTickersId);
        service.timeoutTickersId = undefined;
    }
    let instrument = service.instrumentsBuffer.shift();
    const baseUrl = deribitConfig.testnet ? deribitConfig.baseUrlTestnet : deribitConfig.baseUrlRealnet;
    let url = sprintf("%s%s?instrument_name=%s", baseUrl, deribitConfig.pathTicker, instrument.instrument_name);
    // console.log(url);
    request(url, undefined, (error, response, body) => {
        if (error) {
            console.log(error);
            if (service.instrumentsBuffer.length > 0) {
                service.timeoutTickersId = setTimeout(service.downloadTicker, service.delayTicker);
            } else {
                service.calculateChartData();
            }
            return;
        }
        // console.log(body);
        body = JSON.parse(body);
        body = body.result;
        const expiration_timestamp = new Date(instrument.expiration_timestamp);
        const creation_timestamp = new Date(instrument.creation_timestamp);
        const type_symbol = instrument.instrument_name.substr(instrument.instrument_name.length - 1, 1);
        const strike = Math.round(parseFloat(instrument.strike) * 1000);
        const option_symbol = sprintf("GS%02d%02d%02d%s%08d", expiration_timestamp.getFullYear() % 100, expiration_timestamp.getMonth() + 1, expiration_timestamp.getDate(), type_symbol, strike);
        const type = type_symbol == 'C' ? 'Call' : 'Put';
        service.detailRowsBuffer.push([
            instrument.instrument_name, body.underlying_price, option_symbol, type, expiration_timestamp.toISOString(), creation_timestamp.toISOString(), instrument.strike, body.last_price, body.best_bid_price, body.best_ask_price, body.stats.volume, body.open_interest, body.mark_iv, body.bid_iv, body.iv_ask, body.greeks.delta, body.greeks.gamma, body.greeks.theta, body.greeks.vega
        ]);
        console.log('detailed', service.detailRowsBuffer.length);
        if (service.instrumentsBuffer.length > 0) {
            service.timeoutTickersId = setTimeout(service.downloadTicker, service.delayTicker);
        } else {
            service.calculateChartData();
        }
        // let sql = sprintf("INSERT INTO `deribit_instruments`(`instrument_name`, `underlying_price`, `option_symbol`, `type`, `expiration_timestamp`, `creation_timestamp`, `strike`, `last_price`, `best_bid_price`, `best_ask_price`, `volume`, `open_interest`, `mark_iv`, `bid_iv`, `iv_ask`, `delta`, `gamma`, `theta`, `vega`) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');", instrument.instrument_name, body.underlying_price, option_symbol, type, expiration_timestamp.toISOString(), creation_timestamp.toISOString(), instrument.strike, body.last_price, body.best_bid_price, body.best_ask_price, body.stats.volume, body.open_interest, body.mark_iv, body.bid_iv, body.iv_ask, body.greeks.delta, body.greeks.gamma, body.greeks.theta, body.greeks.vega);
        // console.log(instrumentsCnt, sql);
        // dbConn.query(sql, undefined, (error, results, fields) => {
        //     // tikersCnt++;
        //     // if (tikersCnt == instrumentsCnt) {
        //     //     service.calculateChartData();
        //     // }
        //     if (error) {
        //         console.log(error);
        //     }
        //     if (service.instrumentsBuffer.length > 0) {
        //         service.timeoutTickersId = setTimeout(service.downloadTicker, service.delayTicker);
        //     } else {
        //         service.calculateChartData();
        //     }
        // });
    });
};

// service.downloadDeribitInstruments = () => {
//     if (service.timeoutInstrumentsId) {
//         clearTimeout(service.timeoutInstrumentsId);
//         service.timeoutInstrumentsId = undefined;
//     }
//
//     const baseUrl = deribitConfig.testnet ? deribitConfig.baseUrlTestnet : deribitConfig.baseUrlRealnet;
//     // let url = sprintf("%s%s", baseUrl, deribitConfig.pathInstruments);
//     let url = sprintf("%s%s?currency=BTC&kind=option&expired=false", baseUrl, deribitConfig.pathInstruments);
//     // console.log(url);
//     request(url, null, (error, response, body) => {
//         if (error) {
//             console.log(error);
//             service.timeoutInstrumentsId = setTimeout(service.downloadDeribitInstruments, service.delayInstruments);
//             return;
//         }
//         let sql = "DELETE FROM `deribit_instruments`";
//         dbConn.query(sql, undefined, (error, results, fields) => {
//             if (error) {
//                 service.timeoutInstrumentsId = setTimeout(service.downloadDeribitInstruments, service.delayInstruments);
//                 return;
//             }
//             body = JSON.parse(body);
//             const instruments = body.result;
//             console.log('instruments', new Date(), instruments.length);
//             // instrumentsCnt = instruments.length;
//             // tikersCnt = 0;
//             for (let instrument of instruments) {
//                 // console.log(JSON.stringify(instrument));
//                 // service.downloadTicker(instrument);
//                 service.instrumentsBuffer.push(instrument);
//             }
//             service.downloadTicker();
//             // console.log(JSON.stringify(response), JSON.stringify(error));
//             // timeoutInstrumentsId = setTimeout(service.downloadDeribitInstruments, delayInstruments);
//         });
//     });
// };
//
// service.downloadTicker = () => {
//     if (service.timeoutTickersId) {
//         clearTimeout(service.timeoutTickersId);
//         service.timeoutTickersId = undefined;
//     }
//     const instrumentsCnt = service.instrumentsBuffer.length;
//     let instrument = service.instrumentsBuffer.shift();
//     const baseUrl = deribitConfig.testnet ? deribitConfig.baseUrlTestnet : deribitConfig.baseUrlRealnet;
//     let url = sprintf("%s%s?instrument_name=%s", baseUrl, deribitConfig.pathTicker, instrument.instrument_name);
//     // console.log(url);
//     request(url, undefined, (error, response, body) => {
//         if (error) {
//             console.log(error);
//             if (service.instrumentsBuffer.length > 0) {
//                 service.timeoutTickersId = setTimeout(service.downloadTicker, service.delayTicker);
//             } else {
//                 service.calculateChartData();
//             }
//             return;
//         }
//         // console.log(body);
//         body = JSON.parse(body);
//         body = body.result;
//         const expiration_timestamp = new Date(instrument.expiration_timestamp);
//         const creation_timestamp = new Date(instrument.creation_timestamp);
//         const type_symbol = instrument.instrument_name.substr(instrument.instrument_name.length - 1, 1);
//         const strike = Math.round(parseFloat(instrument.strike) * 1000);
//         const option_symbol = sprintf("GS%02d%02d%02d%s%08d", expiration_timestamp.getFullYear() % 100, expiration_timestamp.getMonth() + 1, expiration_timestamp.getDate(), type_symbol, strike);
//         const type = type_symbol == 'C' ? 'Call' : 'Put';
//         let sql = sprintf("INSERT INTO `deribit_instruments`(`instrument_name`, `underlying_price`, `option_symbol`, `type`, `expiration_timestamp`, `creation_timestamp`, `strike`, `last_price`, `best_bid_price`, `best_ask_price`, `volume`, `open_interest`, `mark_iv`, `bid_iv`, `iv_ask`, `delta`, `gamma`, `theta`, `vega`) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');", instrument.instrument_name, body.underlying_price, option_symbol, type, expiration_timestamp.toISOString(), creation_timestamp.toISOString(), instrument.strike, body.last_price, body.best_bid_price, body.best_ask_price, body.stats.volume, body.open_interest, body.mark_iv, body.bid_iv, body.iv_ask, body.greeks.delta, body.greeks.gamma, body.greeks.theta, body.greeks.vega);
//         console.log(instrumentsCnt, sql);
//         dbConn.query(sql, undefined, (error, results, fields) => {
//             // tikersCnt++;
//             // if (tikersCnt == instrumentsCnt) {
//             //     service.calculateChartData();
//             // }
//             if (error) {
//                 console.log(error);
//             }
//             if (service.instrumentsBuffer.length > 0) {
//                 service.timeoutTickersId = setTimeout(service.downloadTicker, service.delayTicker);
//             } else {
//                 service.calculateChartData();
//             }
//         });
//     });
// };

service.calculateChartData = () => {
    console.log('calculateChartData', new Date());

    let sql = "DELETE FROM `deribit_instruments`;";
    dbConn.query(sql, undefined, (error, results, fields) => {
        if (error) {
            service.timeoutInstrumentsId = setTimeout(service.downloadDeribitInstruments, service.delayInstruments);
            return;
        }
        sql = sprintf("INSERT INTO `deribit_instruments`(`instrument_name`, `underlying_price`, `option_symbol`, `type`, `expiration_timestamp`, `creation_timestamp`, `strike`, `last_price`, `best_bid_price`, `best_ask_price`, `volume`, `open_interest`, `mark_iv`, `bid_iv`, `iv_ask`, `delta`, `gamma`, `theta`, `vega`) VALUES ?;");
        console.log(service.detailRowsBuffer.length, sql);
        dbConn.query(sql, [service.detailRowsBuffer], (error, results, fields) => {
            if (error) {
                service.timeoutInstrumentsId = setTimeout(service.downloadDeribitInstruments, service.delayInstruments);
                return;
            }
            sql = sprintf("DELETE FROM `deribit_instruments2`;");
            dbConn.query(sql, undefined, (error, results, fields) => {
                if (error) {
                    service.timeoutInstrumentsId = setTimeout(service.downloadDeribitInstruments, service.delayInstruments);
                    return;
                }
                sql = sprintf("INSERT INTO `deribit_instruments2`(`instrument_name`, `underlying_price`, `option_symbol`, `type`, `expiration_timestamp`, `creation_timestamp`, `strike`, `last_price`, `best_bid_price`, `best_ask_price`, `volume`, `open_interest`, `mark_iv`, `bid_iv`, `iv_ask`, `delta`, `gamma`, `theta`, `vega`) (SELECT `instrument_name`, `underlying_price`, `option_symbol`, `type`, `expiration_timestamp`, `creation_timestamp`, `strike`, `last_price`, `best_bid_price`, `best_ask_price`, `volume`, `open_interest`, `mark_iv`, `bid_iv`, `iv_ask`, `delta`, `gamma`, `theta`, `vega` FROM `deribit_instruments`);");

                dbConn.query(sql, undefined, (error, results, fields) => {
                    service.timeoutInstrumentsId = setTimeout(service.downloadDeribitInstruments, service.delayInstruments);
                });
            });
        });
    });
};

module.exports = service;