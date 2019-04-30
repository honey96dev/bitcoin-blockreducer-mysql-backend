var dbConn1 = require('./../_core/dbConn');

process.on('message', function(data) {
    /*  1. group by transaction array with price value per side 
        2. compare transaction with order list (old and new) to find hidden order
        3. insert sql query with hidden order
        4. close connection.    
    */
    var insertHiddenList = [];

    for(var kk = 0; kk < data.baseOrderList.length; kk ++) 
    {
        for(var hh = 0; hh < data.oldOrderArray.length; hh++)
        {
            for(var jj = 0; jj < data.callback.newSellJson.length; jj++) 
            {
                if(data.oldOrderArray[hh].price == data.baseOrderList[kk].price && data.baseOrderList[kk].price == data.callback.newSellJson[jj].price)
                {
                    if(data.callback.newSellJson[jj].side == 'Sell' && data.oldOrderArray[hh].side == 'Buy')    // sell transaction and buy order
                    {
                        var deltaValue = data.callback.newSellJson[jj].size - (data.baseOrderList[kk].buySize - data.oldOrderArray[hh].size);
                        if(Math.abs(deltaValue) > data.callback.newSellJson[jj].size * 0.1)     //  within 10% of error tolerance , so not excatly  !=, as long as within 10% it is all alright
                        {
                            var insertData = {
                                isoDate: data.callback.newSellJson[jj].timestamp,
                                symbol: data.callback.newSellJson[jj].symbol,
                                side:  'Sell',
                                size: deltaValue,
                                price: data.callback.newSellJson[jj].price
                            }
                            
                            insertHiddenList.push(insertData);
                            StoreToHiddenOrder(insertData);
                        }
                    }

                    if(data.callback.newSellJson[jj].side == 'Buy' && data.oldOrderArray[hh].side == 'Sell')    // sell transaction and buy order
                    {
                        var deltaValue = data.callback.newSellJson[jj].size - (data.baseOrderList[kk].sellSize - data.oldOrderArray[hh].size);
                        if(Math.abs(deltaValue) > data.callback.newSellJson[jj].size * 0.1)     //  within 10% of error tolerance , so not excatly  !=, as long as within 10% it is all alright
                        {
                            var insertData = {
                                isoDate: data.callback.newSellJson[jj].timestamp,
                                symbol: data.callback.newSellJson[jj].symbol,
                                side:  'Buy',
                                size: deltaValue,
                                price: data.callback.newSellJson[jj].price
                            }
                            
                            insertHiddenList.push(insertData);
                            StoreToHiddenOrder(insertData);
                        }
                    }

                }
                
            }


        }
    }
   
    dbConn1.end((err) => {
        console.log("close connect");
        // The connection is terminated gracefully
        // Ensures all previously enqueued queries are still
        // before sending a COM_QUIT packet to the MySQL server.
            process.send(1);  // when close connect return 1;
    });
});



function GetOldOrderArray(start, end, callback) {
    let selectSql = 'SELECT * FROM orderbook_tbl WHERE price BETWEEN ? AND ?';
    
    dbConn1.query(selectSql, [start, end], (error, results, fields) => {
        if (error) {console.log(error)}
            
        callback(results);
        //dbConn.end();
    })
}

function UpdateOrderBook(data, price) {
    let updateSQL = 'update orderbook_tbl SET ?  WHERE  price = ?';

    dbConn1.query(updateSQL, [data, price], (error, results, fields) => {
        if (error) { console.log(error) }
                //dbConn.end();
    });
}

function StoreToHiddenOrder(data) {
    let insertSql = 'INSERT INTO hidden_order_tbl SET ?';

    dbConn1.query(insertSql, [data], (error, results, tableName) => {
        if (error) {console.log(error)}
    });
}
