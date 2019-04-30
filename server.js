const express = require('express');
const cors = require('cors');
const path = require('path');
const bodyParser = require('body-parser');

const appService = require('./app/app.service');
const bitmexService = require('./app/app.bitmex-service');
const config = require('./_core/config');
const port = process.env.PORT || config.server.port;


const app = express();
app.use(cors());
app.use(bodyParser.json());
app.use(express.static( './public' ));

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

bitmexService.readOrderBook();
bitmexService.readTrade();

bitmexService.getLastTimestamp4Bucket('5m', function (startTime) {
    bitmexService.downloadBitmexData('5m', startTime);
    // if (startTime.length > 0) {
    //     bitmexService.downloadBitmexData('5m', startTime[0].timestamp);
    // } else {
    //     bitmexService.downloadBitmexData('5m', '');
    // }
});
// bitmexService.downloadBitmexData('5m', '');

app.get('/', (req, res, next) => {
    res.sendFile(path.join(__dirname, './public/index.html'));
})

app.listen(port, () => {
    console.log('BitmexToMysql server running on port : ' + port);
});