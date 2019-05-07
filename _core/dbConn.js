var mysql = require('mysql2');
var config = require('./config');

// const dbConn = new mysql.createConnection(config.mysql);
//
// dbConn.connect((error) => {
//     if (error) {
//         console.log("!!! Cannot connect !!! Error! ");
//         throw error;
//     } else {
//         // console.log("Connection established ID is " + dbConn.threadId);
//     }
// });
const dbConn = mysql.createPool(config.mysql);

module.exports = dbConn;
