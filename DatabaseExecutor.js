var debug = require('debug')('database-executor:database-executor');
var databaseConnector = require('node-database-connectors');
var databaseExecutor = require('./ConnectorIdentifier.js');
var axiomUtils = require("axiom-utils");
if (GLOBAL._connectionPools == null) {
  GLOBAL._connectionPools = {};
}
var oldResults = {};

function prepareQuery(dbConfig, queryConfig, cb) {
  try {
    var objConnection = databaseConnector.identify(dbConfig);
    var query = objConnection.prepareQuery(queryConfig);
    cb({
      status: true,
      content: query
    });
  } catch (ex) {
    console.log('exception: ', ex);
    var e = ex;
    //e.exception=ex;
    cb({
      status: false,
      error: e
    });
  }
}

function executeRawQueryWithConnection(dbConfig, rawQuery, cb) {
  try {
    var objConnection = databaseConnector.identify(dbConfig);
    objConnection.connect(dbConfig, function(err, connection) {
      if (err != undefined) {
        console.log('connection error: ', err);
        var e = err;
        //e.exception=ex;
        cb({
          status: false,
          error: e
        });
      } else {
        var objExecutor = databaseExecutor.identify(dbConfig);
        objExecutor.executeQuery(connection, rawQuery, function(result) {
          objConnection.disconnect(connection);
          cb(result);
        });
      }
    });
  } catch (ex) {
    console.log('exception: ', ex);
    var e = ex;
    //e.exception=ex;
    cb({
      status: false,
      error: e
    });
  }
}

exports.executeRawQuery = function(requestData, cb) {
  // debug('dbcon req:\nrequestData: %s', JSON.stringify(requestData));
  var dbConfig = requestData.dbConfig;
  var rawQuery = requestData.query;
  var tableName = requestData.table;
  var shouldCache = requestData.hasOwnProperty('shouldCache') ? requestData.shouldCache : false;
  executeRawQuery(dbConfig, rawQuery, shouldCache, tableName, cb);
}

exports.executeQuery = function(requestData, cb) {
  //debug('dbcon req:\nrequestData: %s', JSON.stringify(requestData));
  var dbConfig = requestData.dbConfig;
  var queryConfig = requestData.query;
  var shouldCache = requestData.hasOwnProperty('shouldCache') ? requestData.shouldCache : false;

  prepareQuery(dbConfig, queryConfig, function(data) {
    //     debug('prepareQuery', data);
    if (data.status == true) {
      executeRawQuery(dbConfig, data.content, shouldCache, queryConfig.table, cb);
    } else {
      cb(data);
    }
  });
}

exports.executeQueryStream = function(requestData, onResultFunction, cb) {
  var dbConfig = requestData.dbConfig;
  var query = requestData.rawQuery;
  var objConnection = databaseConnector.identify(dbConfig);
  objConnection.connect(dbConfig, function(err, connection) {
    if (err != undefined) {
      console.log('connection error: ', err);
      var e = err;
      //e.exception=ex;
      cb({
        status: false,
        error: e
      });
    } else {
      var objExecutor = databaseExecutor.identify(dbConfig);
      objExecutor.executeQueryStream(connection, query, onResultFunction, cb);
    }
  });
}


// DS : Handle Multiple Queries with same connection similar to batch queries;

function executeRawQueryWithConnectionPool(dbConfig, rawQuery, cb) {
  try {
    var startTime = new Date();
    getConnectionFromPool(dbConfig, function(result) {
      if (result.status === false) {
        cb(result);
      } else {
        var connection = result.content;
        if (rawQuery.length <= 100000000) {
          debug('query: %s', rawQuery);
        } else {
          debug('query: %s', rawQuery.substring(0, 500) + "\n...\n" + rawQuery.substring(rawQuery.length - 500, rawQuery.length));
        }
        var queryStartTime = new Date();
        var objExecutor = databaseExecutor.identify(dbConfig);
        objExecutor.executeQuery(connection, rawQuery, function(result) {
          if (result.status == false) {
            console.log("DB Executor Error", dbConfig, rawQuery);
          }
          debug("Total Time:", (new Date().getTime() - startTime.getTime()) / 1000, "Query Time:", (new Date().getTime() - queryStartTime.getTime()) / 1000);
          cb(result);
        });
      }
    });
  } catch (ex) {
    console.log('exception: ', ex);
    var e = ex;
    //e.exception=ex;
    cb({
      status: false,
      error: e
    });
  }
}


function executeRawQuery(dbConfig, rawQuery, shouldCache, tableName, cb) {
  if (!tableName) {
    tableName = "#$table_name_not_available$#";
  }
  if (shouldCache == true && oldResults[JSON.stringify(dbConfig)] && oldResults[JSON.stringify(dbConfig)][tableName] && oldResults[JSON.stringify(dbConfig)][tableName][rawQuery]) {
    cb({ status: true, content: oldResults[JSON.stringify(dbConfig)][tableName][rawQuery].result })
  } else {
    if (dbConfig.hasOwnProperty('connectionLimit') && dbConfig.connectionLimit == 0) {
      debug("With New Connection");
      executeRawQueryWithConnection(dbConfig, rawQuery, function(responseData) {
        cb(responseData)
        if (shouldCache == true && responseData.status == true) {
          saveToCache(responseData.content, dbConfig, rawQuery, tableName)
        }
      });
    } else {
      debug("With Connection Pool");
      executeRawQueryWithConnectionPool(dbConfig, rawQuery, function(responseData) {
        cb(responseData)
        if (shouldCache == true && responseData.status == true) {
          saveToCache(responseData.content, dbConfig, rawQuery, tableName)
        }
      });
    }
  }
}


function getConnectionFromPool(dbConfig, cb) {
  try {
    var connectionString = (dbConfig.databaseType + '://' + dbConfig.user + ':' + dbConfig.password + '@' + dbConfig.host + ':' + dbConfig.port + '/' + dbConfig.database);
    if (GLOBAL._connectionPools.hasOwnProperty(connectionString)) {
      cb({
        status: true,
        content: GLOBAL._connectionPools[connectionString]
      });
      return;
    } else {
      var objConnection = databaseConnector.identify(dbConfig);
      objConnection.connectPool(dbConfig, function(err, pool) {
        if (err != undefined) {
          console.log('connection error: ', err);
          var e = err;
          //e.exception=ex;
          cb({
            status: false,
            error: e
          });
        } else {
          GLOBAL._connectionPools[connectionString] = pool;
          cb({
            status: true,
            content: pool
          });
        }
      });
    }
  } catch (ex) {
    console.log('exception: ', ex);
    var e = ex;
    //e.exception=ex;
    cb({
      status: false,
      error: e
    });
  }
}


function saveToCache(finalData, dbConfig, queryString, tableName) {
  var dbConf = JSON.stringify({host:dbConfig.host,port:dbConfig.port});
  if (!oldResults[dbConf]) {
    oldResults[dbConf] = {};
  }
  if (!oldResults[dbConf][tableName]) {
    oldResults[dbConf][tableName] = {}
  }
  oldResults[dbConf][tableName][queryString] = {
    result: axiomUtils.extend(true, [], finalData)
  };
  // console.log("################################## JSON.stringify(oldResults) ###########################################")
  // console.log(JSON.stringify(oldResults))
  // console.log("################################## JSON.stringify(oldResults) ###########################################")

}


exports.flushCache = function(dbConfig, tableName) {
  var dbConf = JSON.stringify({host:dbConfig.host,port:dbConfig.port});
  if (oldResults[dbConf]) {
    if (oldResults[dbConf][tableName]) {
      oldResults[dbConf][tableName] = {};
    }
  }
}
