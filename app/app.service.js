var request = require('request');
const child_process = require('child_process'); 
var fork = require('child_process').fork;
var bitmexClient = require('./../_core/bitmexConn');
var dbConn = require('./../_core/dbConn');
var config = require('./../_core/config');

/**
 *  Define modules
 */
var service = {};
service.StoreAllTransactions = StoreAllTransactions;
service.Get5MLastTradePrice = Get5MLastTradePrice;
service.SetUrl = SetUrl;
service.StoreOrderBookData = StoreOrderBookData;
service.InsertInit = InsertInit;


module.exports = service;

var strUrl = config.bitmex.strUrl;
var tmpUrl = '';
var newHistoryDate = '';
var historyArray = [];

var newArray = [];
var oldValue = '';
var spliceCount = 10;
var startTime = '';
var endTime = '';
var startFlag = false;
var transactionArray = [];
var oldOrderArray = [];
var orderFlag = false;
var gBaseOrderList = [];  // store all old order data 
var initFlag = false;


function InsertInit() {
    var selectSql = 'SELECT * FROM orderbook_tbl WHERE BuySize > 0 or SellSize > 0';
    
    dbConn.query(selectSql, (error, results) => {
        if (error) { console.log(error) }

        for(var obj of results) {
            gBaseOrderList.push(obj);            
        }

        gBaseOrderList.sort(function(a,b) {
            return a.price - b.price;
        });
    });
}

function StoreAllTransactions() {
    bitmexClient.addStream('XBTUSD', 'trade', (data, symbol, tableName) => {
        if (data.length > 0) {
            for(var i = data.length - 1; i >= 0; i --) {
                if(data[i].trdMatchID == oldValue) {
                    break;
                } else {
                    newArray.push(data[i]);
                    transactionArray.push(data[i]);
                }
            }
            oldValue = data[data.length - 1].trdMatchID;

            if (startFlag == false) {
                startTime = data[data.length - 1].timestamp; //fromType value type isoDate 
                startFlag = true;
            }
            endTime = data[data.length - 1].timestamp;  
    
            if(newArray.length > spliceCount)
            {                
                var tempArray = newArray.splice(0, spliceCount);
                var insertSql = 'INSERT INTO transaction_tbl (isoDate, symbol, side, size, price, trdMatchID, state) VALUES ';
                var insertData = '';
                for (var obj of tempArray) {
                    insertData += "(" + "'" + obj.timestamp +  "'" + ',' +  "'" + obj.symbol + "'" + "," + "'" + obj.side + "'" + "," +"'" + obj.size +"'" + "," +"'" + obj.price +"'" +"," +"'" + obj.trdMatchID +"'" + "," + "1" +")" + ",";
                }
                insertData = insertData.slice(0, -1) + ";";
                insertSql += insertData;
    
                var workerProcess = fork(__dirname + './../_bin/storeTransactions.js');
                    //var baseOrderList = JSON.parse(JSON.stringify( gBaseOrderList ));                   
                    workerProcess.on('message', (response) => {
                        //console.log(response);                        
                        if(response == 1){                            
                            workerProcess.kill();
                            tempArray = [];                            
                        }
                    });
                    workerProcess.send(insertSql);  // invoke function withh old & new order list & transaction history

                // var workerProcess = child_process.exec('node ./_bin/storeTransactions.js "' + insertSql + '"',  
                // function (error, stdout, stderr) {  
                //     if (error) {  
                //         console.log(error.stack);  
                //         console.log('Error code: '+error.code);  
                //         console.log('Signal received: '+error.signal);  
                //     }  
                    
                // });  
                // workerProcess.on('exit', function (code) {                      
                // });  
            }
            
            // if start time + 5min >= end time then execute procedure and first time = end time to initialize start time to end time. 
            if ( new Date(startTime).getTime() + 300000 <= (new Date(endTime)).getTime()) {
                Store5MVolume(startTime, endTime);
                startTime = endTime;
            }
        }
    }); 
}

function StoreOrderBookData() {
    bitmexClient.addStream('XBTUSD', 'orderBookL2_25', (data, symbol, tableName) => {
        var tempData = [];
        var myData = [];
        for (var obj of data) {                 
            tempData.push(obj);                                            
        }

        tempData.sort(function(a, b) {
            return a.price - b.price;
        });

        myData = JSON.parse(JSON.stringify( tempData));

        if(initFlag == false)
        {
            gBaseOrderList = JSON.parse(JSON.stringify( myData));
            console.log("Init Length = " + gBaseOrderList.length);
            initFlag = true;
        }
        /* update base order data with new order data */
        var newOrderMaxPrice = 0;
        var newOrderMinPrice = 0;
        var extraOrderData = [];
        var kk = 0;

        newOrderMinPrice = myData[0].price;
        newOrderMaxPrice = myData[myData.length -1].price;

/***********    update baseline order data start         **************/
        /* identify include new order list in base order list   */
        if(gBaseOrderList[0].price > newOrderMinPrice)
        {
            for(var i = newOrderMinPrice; i <= gBaseOrderList[0].price; i+=0.5 )            
            {   var tmpdata;                
                if(typeof myData[kk] == 'undefined')
                {
                    tmpdata = {
                        symbol: 'XBTUSD',                    
                        sellSize: 0, 
                        buySize: 0,
                        price: i
                    }                    
                } else {   // mydata is not empty
                    if(myData[kk].side == 'Sell') {
                        tmpdata = {
                            symbol: 'XBTUSD',                    
                            sellSize: myData[kk].size, 
                            buySize: 0,
                            price: i
                        }
                    } else if (myData[kk].side == 'Buy') {
                        tmpdata = {
                            symbol: 'XBTUSD',                    
                            sellSize: 0, 
                            buySize: myData[kk].size,
                            price: i
                        }
                    }
                }
                extraOrderData.push(tmpdata);
                kk ++;
            }
        }

        kk = 0;

        if(gBaseOrderList[gBaseOrderList.length -1].price < newOrderMaxPrice)
        {
            for(var i = newOrderMaxPrice; i >= gBaseOrderList[gBaseOrderList.length -1].price;  i-=0.5 )  // loop desc dirct
            {
                var tmpdata;
                if(typeof myData[myData.length - kk] == 'undefined')
                {
                    tmpdata = {
                        symbol: 'XBTUSD',                    
                        sellSize: 0, 
                        buySize: 0,
                        price: i
                    }
                    
                } else {   // mydata is not empty                    
                    if(myData[myData.length - kk].side == 'Sell') {
                        tmpdata = {
                            symbol: 'XBTUSD',                    
                            sellSize: myData[myData.length - kk].size, 
                            buySize: 0,
                            price: i
                        }
    
                    } else if (myData[myData.length - kk].side == 'Buy') {
                        tmpdata = {
                            symbol: 'XBTUSD',                    
                            sellSize: 0, 
                            buySize: myData[myData.length - kk].size,
                            price: i
                        }
                    }
                }
                extraOrderData.push(tmpdata);
                kk ++;
            
            }
        }

        if(extraOrderData.length > 1)
        {
            //console.log(extraOrderData);
            for(var obj of extraOrderData)
            {
                gBaseOrderList.push(obj);
            }
            
            gBaseOrderList.sort(function(a, b){
                 return a.price - b.price;
            });
        }
        
/*  update base order list with new order witch crossed data  */

        for(var obj of myData)
        {        
            var index = (obj.price - gBaseOrderList[0].price) * 2 ;  
            if(typeof gBaseOrderList[index] != 'undefined')
            {                
                if (obj.side == "Buy") {
                    gBaseOrderList[index].buySize = obj.size;
                } else if (obj.side == "Sell") {
                // console.log(index);
                    //console.log(gBaseOrderList[index]);
                    gBaseOrderList[index].sellSize = obj.size;
                }       
            }
            else{                
                console.log("undefined : "+ index);
                console.log(myData[0].price);
                console.log(myData[myData.length-1].price);
                console.log(gBaseOrderList[0].price);
                console.log(gBaseOrderList[gBaseOrderList.length-1].price);
            }
        }
/*********** Update baseline order data End **************/

        if (orderFlag == false) {
            oldOrderArray = JSON.parse(JSON.stringify( gBaseOrderList )); //fromType value type isoDate
            orderFlag = true;
        } else {            
            if (transactionArray.length > 10) {
                var transactionData = JSON.parse(JSON.stringify( transactionArray ));
                transactionArray.splice(0);
                transactionArray = []; 
                ArrangeTransactionArray(transactionData, (callback) => {
                    var orderProcess = fork(__dirname + './../_bin/storeOrders.js');
                    var baseOrderList = JSON.parse(JSON.stringify( gBaseOrderList ));                   
                    orderProcess.on('message', (response) => {
                        if(response == 1){
                            //console.log(response);
                            orderProcess.kill();
                        }
                    });
                    orderProcess.send({baseOrderList, oldOrderArray, callback});  // invoke function withh old & new order list & transaction history
                });
                orderFlag = false;
                oldOrderArray = [];
            }
        }        
    });
}

function SetUrl() {
    GetMaxIsoDate((callback) => {
        strUrl = strUrl + callback[0].max;
       // console.log(strUrl);
    });
}

function Get5MLastTradePrice() {
    console.log(strUrl);
    if ( strUrl != tmpUrl ) {
        request(strUrl, function (error, response, body) {
            if (error) {
                console.log(error);
            };
            var data = JSON.parse(body);            
            if (response.statusCode == 200 && data.length > 0) {
                for (var obj of data) {
                    historyArray.push(obj);
                }
            }
    
            if (historyArray.length > 0) {
                tmpUrl = strUrl;
                newHistoryDate = historyArray[historyArray.length - 1].timestamp;
                newHistoryDate = new Date(new Date(newHistoryDate).getTime() + 300000).toISOString();
                strUrl = strUrl.slice(0, 118) + newHistoryDate;
                
                StoreLastTradePrice(data);
            }
            
            historyArray = [];
        });
    }
}

function GetMaxIsoDate(callback) {
    let selectSql = 'SELECT MAX(isoDate) as max FROM `price_5m_tbl`';
    dbConn.query(selectSql, (error, results, fields) => {
        if (error) return console.log(error);
        
        callback(results);
    });
}

function StoreLastTradePrice(data) {
    let insertSql = 'INSERT INTO price_5m_tbl SET ?';
    
    for (var obj of data) {
        var insertData = {
            isoDate: obj.timestamp,
            symbol: obj.symbol,
            open: obj.open, 
            high: obj.high,
            low: obj.low,
            close: obj.close
        }
        dbConn.query(insertSql, [insertData], (error, results, fields) => {
            if (error) { console.log(error)};
        });
    }
}

function Store5MVolume(startTime, endTime) {
    let procedureSql = 'CALL extractData (?, ?)';
    dbConn.query(procedureSql, [startTime, endTime], (error, result) => {
        if (error) { console.log(error)};
    });
}

function ArrangeTransactionArray(json, callback) {
    var sellJson = [];
    var buyJson = [];
    var newSellJson = [];
    var newBuyJson = [];

    for(var obj of json) {
        if (obj.side == "Sell") {
            sellJson.push(obj);
        } else if (obj.side == "Buy") {
            buyJson.push(obj);
        }
    }
    
    if(sellJson.length > 0)
    {
        sellJson.sort(function(a, b){
            return a.price - b.price;
        });

        newSellJson.push(sellJson[0]);

        for (var i = 1; i < sellJson.length; i++) {
            if ( sellJson[i].price == newSellJson[newSellJson.length - 1].price ) {       
                newSellJson[newSellJson.length - 1].size = sellJson[i].size + newSellJson[newSellJson.length - 1].size;
            } else {
                newSellJson.push(sellJson[i]);
            }
        }    
    }

    if(buyJson.length > 0)
    {    
        buyJson.sort(function(a, b){
            return a.price - b.price;
        }); 
        newBuyJson.push(buyJson[0]);
    
        for (var i = 1; i < buyJson.length; i++) {
            if ( buyJson[i].price == newBuyJson[newBuyJson.length - 1].price ) {       
                newBuyJson[newBuyJson.length - 1].size = buyJson[i].size + newBuyJson[newBuyJson.length - 1].size;
            } else {
                newBuyJson.push(buyJson[i]);
            }
        }  
    }

    if(newSellJson.length > 0)
    {
        for(var obj of newBuyJson)
        {
            newSellJson.push(obj);
        }
    } else
    {
        newSellJson = newBuyJson;
    }
        
    if(newSellJson.length < 1)
    {
        console.log(" ----- 0 ------ 0 ------ ");
        console.log(json);
    }
    callback({newSellJson});
}