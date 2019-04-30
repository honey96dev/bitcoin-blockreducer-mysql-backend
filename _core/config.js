var config = {
    server: {
        port: 8000
    },
    // mysql: {
    //     host: 'block-reducer-server.mysql.database.azure.com',
    //     user: 'blockadmin@block-reducer-server',
    //     password: '1:1234btc',
    //     database: 'blockreducerdb',
    //     port: 3306,
    //     ssl: {
    //         rejectUnauthorized: false
    //       }
    // },
    mysql: {
        host: '108.61.186.24',
        user: 'bitmex3536',
        password: 'BitMex*95645636',
        database: 'blockreducerdb',
        port: 3306
    },
    bitmex: {
        testnet: false,
        apiKeyID: '',
        apiKeySecret: '',
        maxTableLen: 100,
        strUrl: 'https://www.bitmex.com/api/v1/trade/bucketed?binSize=5m&partial=false&symbol=XBTUSD&count=500&reverse=false&startTime='
    }
};

module.exports = config;
