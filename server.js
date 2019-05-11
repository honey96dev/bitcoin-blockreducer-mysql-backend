// const express = require('express');
// const cors = require('cors');
// const path = require('path');
// const bodyParser = require('body-parser');

const appService = require('./app/app.service');
const bitmexService = require('./app/app.bitmex-service');
const config = require('./_core/config');
const port = process.env.PORT || config.server.port;
var cluster = require('cluster');
if (cluster.isMaster) {
    cluster.fork();

    cluster.on('exit', function(worker, code, signal) {
        cluster.fork();
    });
}

if (cluster.isWorker) {
    // put your code here
    bitmexService.readOrderBook();
    bitmexService.readTrade();
    bitmexService.commitOrdersData();
    // setTimeout(bitmexService.commitVolumeData, 60000);

    // bitmexService.downloadBitmexInstrumentData();

    setTimeout(bitmexService.getLastTimestamp4Bucket, 0, '5m', function (startTime) {
        bitmexService.downloadBitmexData('5m', startTime);
    });

    setTimeout(bitmexService.getLastTimestamp4Bucket, 30000, '1h', function (startTime) {
        bitmexService.downloadBitmexData('1h', startTime);
    });

    setTimeout(bitmexService.getLastTimestamp4Bucket, 15000, '1m', function (startTime) {
        bitmexService.downloadBitmexData('1m', startTime);
    });
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
// setTimeout(bitmexService.calculateFFT, 0, '5m');
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
