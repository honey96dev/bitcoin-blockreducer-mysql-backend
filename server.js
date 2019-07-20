// const express = require('express');
// const cors = require('cors');
// const path = require('path');
// const bodyParser = require('body-parser');

const appService = require('./app/app.service');
const bitmexService = require('./app/app.bitmex-service');
const deribitService = require('./app/app.deribit-service');
const fftService = require('./app/app.fft-service');
const config = require('./_core/config');
const port = process.env.PORT || config.server.port;
const dbConn = require('./_core/dbConn');
const sprintfJs = require('sprintf-js');
const sprintf = sprintfJs.sprintf,
    vsprintf = sprintfJs.vsprintf;
const cluster = require('cluster');


if (cluster.isMaster) {
    cluster.fork();

    cluster.on('exit', function(worker, code, signal) {
        cluster.fork();
    });
}

if (cluster.isWorker) {
    // bitmexService.readOrderBook();
    bitmexService.readTrade();
    // bitmexService.commitOrdersData();
    setTimeout(bitmexService.commitVolumeData, 60000);

    bitmexService.downloadBitmexInstrumentData();

    setTimeout(bitmexService.getLastTimestamp4Bucket, 0, '5m', function (startTime) {
        bitmexService.downloadBitmexData('5m', startTime);
    });

    setTimeout(bitmexService.getLastTimestamp4Bucket, 30000, '1h', function (startTime) {
        bitmexService.downloadBitmexData('1h', startTime);
    });

    setTimeout(bitmexService.getLastTimestamp4Bucket, 15000, '1m', function (startTime) {
        bitmexService.downloadBitmexData('1m', startTime);
    });

    setTimeout(bitmexService.saveId0Service, 0, '1m');
    setTimeout(bitmexService.saveId0Service, 15000, '5m');
    setTimeout(bitmexService.saveId0Service, 30000, '1h');

    let sql = "SELECT `timestamp` FROM `hidden_orders2` ORDER BY `timestamp` DESC LIMIT 1;";
    dbConn.query(sql, undefined, (error, results, fields) => {
        if (error || results.length == 0) {
            bitmexService.calculateHiddenOrders2('2019-05-04T12:55:00.000Z');
        } else {
            let timestamp = new Date(new Date(results[0].timestamp).getTime() + 60000);
            timestamp.setSeconds(0, 0);
            bitmexService.calculateHiddenOrders2(timestamp.toISOString());
        }
    });

    setTimeout(deribitService.downloadDeribitInstruments, 0);
}



//
// const app = express();
// app.use(cors());
// app.use(bodyParser.json());
// app.use(express.static( './public' ));

/**
 *  @project Call app.serive functions using init calling
 *  @implements Getting data from bitmext and store to database
 *  @description using price_5m_tbl and volume_5m_tbl
 */

// appService.InsertInit();
//appService.InsertInit();
// appService.StoreOrderBookData();
// appService.StoreAllTransactions();
//
// appService.SetUrl();
// setInterval(() => {
//     appService.Get5MLastTradePrice();
// }, 120000);
// ======================================================
// bitmexService.readOrderBook();
// bitmexService.readTrade();
// // setTimeout(bitmexService.commitOrdersData, 60000);
// bitmexService.commitOrdersData();
// setTimeout(bitmexService.commitVolumeData, 60000);
// // bitmexService.commitVolumeData();
//
// bitmexService.downloadBitmexInstrumentData();
//
// setTimeout(bitmexService.getLastTimestamp4Bucket, 0, '5m', function (startTime) {
//     bitmexService.downloadBitmexData('5m', startTime);
// });
//
// setTimeout(bitmexService.getLastTimestamp4Bucket, 30000, '1h', function (startTime) {
//     bitmexService.downloadBitmexData('1h', startTime);
// });
// //
// // bitmexService.getLastTimestamp4Bucket('1h', function (startTime) {
// //     bitmexService.downloadBitmexData('1h', startTime);
// // });
//
// setTimeout(bitmexService.getLastTimestamp4Bucket, 15000, '1m', function (startTime) {
//     bitmexService.downloadBitmexData('1m', startTime);
// });
// ==========================================================================
if (cluster.isWorker) {
    function calculateFFT(interval) {
        // if (fftTimeoutId) {
        //     clearTimeout(fftTimeoutId);
        // }
        // setTimeout(calculateFFT, 30000);
        // let sql = "";
        // bitmexService.calculateFFT('5m');
        let sql = sprintf("SELECT * FROM `fft_%s` ORDER BY `timestamp` DESC LIMIT 1;", interval);
        dbConn.query(sql, null, (error, result, fields) => {
            let timestamp = '';
            if (!!result && result.length > 0) {
                timestamp = result[0].timestamp;
            }
            console.log(interval, timestamp);
            fftService.calculateFFT(interval, timestamp);
        });
    }

    calculateFFT('5m');
    calculateFFT('1h');
}
// bitmexService.getLastTimestamp4Bucket('1m', function (startTime) {
//     bitmexService.downloadBitmexData('1m', startTime);
// });
// bitmexService.getLastTimestamp4Bucket('5m', function (startTime) {
//     bitmexService.downloadBitmexData('5m', startTime);
// });

// bitmexService.getLastTimestamp4Bucket('1m', function (startTime) {
//     bitmexService.downloadBitmex1mData(startTime);
// });

// app.get('/', (req, res, next) => {
//     res.sendFile(path.join(__dirname, './public/index.html'));
// });
//
// app.listen(port, () => {
//     console.log('BitmexToMysql server running on port : ' + port);
// });
