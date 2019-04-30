
var dbConn = require('./../_core/dbConn');


process.on('message', function(data) {
    var sql = data;
    
    dbConn.query(sql, (error, results, fields) => {
        if (error) { console.log(error)};

        dbConn.end((err) => {
            console.log("close connect");
            // The connection is terminated gracefully
            // Ensures all previously enqueued queries are still
            // before sending a COM_QUIT packet to the MySQL server.
            process.send(1);  // when close connect return 1;
        });
    });
});