var debug = require('debug')('database-executor:clickhouse-executor');
var dbConnector = require('node-database-connectors')

// exports.executeRawQuery = function(requestData, cb) {
//   var dbConfig = requestData.dbConfig,
//     query = requestData.query;
//   debug(dbConfig, "*******************************")
//   var objConnection = dbConnector.identify(dbConfig);
//   objConnection.connect(dbConfig, function(err, connection) {
//     executeRawQuery(connection, query, cb)
//   })
// }

function executeRawQuery(connection, rawQuery, cb) {
  var stream = connection.query(rawQuery);
  var rows = [];
  // stream.on('metadata', function(columns) {
  // });

  stream.on('data', function(row) {
    rows.push(row);
  });

  stream.on('error', function(err) {
    debug(err);
    cb({ status: false, content: err });
  });

  stream.on('end', function() {
    cb({ status: true, content: rows });
  });
}

// exports.executeQuery = function(requestData, cb) {
//   var dbConfig = requestData.dbConfig;
//   var queryConfig = requestData.query;
//   debug(queryConfig,"typeof queryConfig")
//   if (typeof(queryConfig) == 'string') {
//     executeRawQuery(requestData, cb);
//   } else {
//     prepareQuery(dbConfig, queryConfig, function(data) {
//       debug('executeQuery', data);
//       if (data.status == true) {
//         executeRawQuery(dbConfig, data.content, cb);
//       } else {
//         cb(data);
//       }
//     });
//   }

// }
exports.executeQuery = function(connection, rawQuery, cb) {
  debug("******************", rawQuery, "******************")
  executeRawQuery(connection, rawQuery, cb);
}

// function prepareQuery(dbConfig, queryConfig, cb) {
//   try {
//     debug("PREPARE QUERY -----------", dbConfig)
//     var objConnection = dbConnector.identify(dbConfig);
//     var query = objConnection.prepareQuery(queryConfig);
//     cb({
//       status: true,
//       content: query
//     });
//   } catch (ex) {
//     debug('exception: ', ex);
//     var e = ex;
//     //e.exception=ex;
//     cb({
//       status: false,
//       error: e
//     });
//   }
// }

// module.exports = {
//   executeQuery: executeQuery,
//   executeRawQuery: executeRawQuery
// }
