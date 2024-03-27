const debug = require('debug')('database-executor:database-executor');
const databaseConnector = require('node-database-connectors');
const databaseExecutor = require('./ConnectorIdentifier.js');
const axiomUtils = require('axiom-utils');
const http = require('http');
const { SecretsManagerClient, ListSecretsCommand, GetSecretValueCommand } = require("@aws-sdk/client-secrets-manager");
const dbPasswordMapping = {}

if (global._connectionPools == null) {
  global._connectionPools = {};
}
const oldResults = {};

function getPasswordFromAwsSecretsManager(secretName, accessKey, secretKey, region, cb) {
  const awsSecretsManagerClient = new SecretsManagerClient({
    region: region,
    credentials: {
      accessKeyId: accessKey,
      secretAccessKey: secretKey,
    },
  });
  const getSecretValueCommand = new GetSecretValueCommand({
    SecretId: secretName
  });
  awsSecretsManagerClient.send(getSecretValueCommand, (err, data) => {
    if (err != null) {
      console.error(err)
    } else {
      let secretValue = data
      secretValue = secretValue.SecretString;
      cb(secretValue);
    }
  })
}

function isValidJSON(inputString) {
  try {
    JSON.parse(inputString);
    return true;
  } catch (error) {
    return false;
  }
}

function getPasswordValue(dbConfig, cb) {
  let dbConfigCopy = axiomUtils.extend(true, {}, dbConfig)
  if (typeof (dbConfigCopy.password) === "object" || isValidJSON(dbConfigCopy.password)) {
    const passwordConfig = typeof (dbConfigCopy.password) === "object" ? dbConfigCopy.password : JSON.parse(dbConfig.password);
    if (passwordConfig && passwordConfig.passwordType == "keyVault" && process.env.key_Vault && process.env.key_Vault.length > 0) {
      try {
        const keyVault = JSON.parse(process.env.key_Vault)
        let vaultIdentifier = passwordConfig.vaultName;
        let secretName = passwordConfig.secretName;
        let type = keyVault[vaultIdentifier] && keyVault[vaultIdentifier].type;
        const accessKey = keyVault[vaultIdentifier].accessKey;
        const secretKey = keyVault[vaultIdentifier].secretKey;
        const region = keyVault[vaultIdentifier].region;
        if (type === "awsKeyVault") {
          if (dbPasswordMapping[type] && dbPasswordMapping[type][vaultIdentifier] && dbPasswordMapping[type][vaultIdentifier][secretName] && dbPasswordMapping[type][vaultIdentifier][secretName].password) {
            dbConfigCopy.password = dbPasswordMapping[type][vaultIdentifier][secretName].password
            cb(dbConfigCopy)
          } else {
            getPasswordFromAwsSecretsManager(secretName, accessKey, secretKey, region, (result) => {
              if (result) {
                dbConfigCopy.password = result
                dbPasswordMapping[type] = { [vaultIdentifier]: { [secretName]: { password: result } } }
                cb(dbConfigCopy)
              }
            });
          }
        }
      }
      catch (error) {
        console.error(error)
      }
    }
  } else {
    cb(dbConfigCopy)
  }
}



function prepareQuery(dbConfig, queryConfig, cb) {
  try {
    const objConnection = databaseConnector.identify(dbConfig);
    const query = objConnection.prepareQuery(queryConfig, dbConfig);
    cb({
      status: true,
      content: query
    });
  } catch (ex) {
    console.log('exception: ', ex);
    const e = ex;
    //e.exception=ex;
    cb({
      status: false,
      error: e
    });
  }
}

function executeRawQueryWithConnection(dbConfig, rawQuery, cb) {
  try {
    const objConnection = databaseConnector.identify(dbConfig);
    objConnection.connect(dbConfig, function (err, connection) {
      if (err != undefined) {
        console.log('connection error: ', err);
        const e = err;
        //e.exception=ex;
        cb({
          status: false,
          error: e
        });
      } else {
        const objExecutor = databaseExecutor.identify(dbConfig);
        objExecutor.executeQuery(connection, rawQuery, function (result) {
          if (result.status == false) {
            console.log('DB Executor Error', dbConfig, rawQuery);
          }
          objConnection.disconnect(connection);
          cb(result);
        });
      }
    });
  } catch (ex) {
    console.log('exception: ', ex);
    console.log('Error in Query: ', rawQuery);
    const e = ex;
    //e.exception=ex;
    cb({
      status: false,
      error: e
    });
  }
}

function executeRawQuery(requestData, cb) {
  // debug('dbcon req:\nrequestData: %s', JSON.stringify(requestData));
  const dbConfig = requestData.dbConfig;
  const rawQuery = requestData.query;
  const tableName = requestData.table;
  const shouldCache = requestData.hasOwnProperty('shouldCache') ? requestData.shouldCache : false;
  // executeRawQueryInner(dbConfig, rawQuery, shouldCache, tableName, cb);

  getPasswordValue(dbConfig, (updatedDBConfig) => {
    if (updatedDBConfig) {
      executeRawQueryInner(updatedDBConfig, rawQuery, shouldCache, tableName, cb);
    }
  })
}

function executeRawQueryPromise(requestData) {
  return new Promise((resolve, reject) => {
    executeRawQuery(requestData, output => {
      if (!output.status) {
        reject(output.error);
      } else {
        resolve(output.content);
      }
    });
  });
}

function executeQuery(requestData, cb) {
  //debug('dbcon req:\nrequestData: %s', JSON.stringify(requestData));
  const dbConfig = requestData.dbConfig;
  const queryConfig = requestData.query;
  const shouldCache = requestData.hasOwnProperty('shouldCache') ? requestData.shouldCache : false;
  prepareQuery(dbConfig, queryConfig, function (data) {
    //     debug('prepareQuery', data);
    if (data.status == true) {
      getPasswordValue(dbConfig, (updatedDBConfig) => {
        if (updatedDBConfig) {
          executeRawQueryInner(updatedDBConfig, data.content, shouldCache, queryConfig.table, cb);
        }
      })
    } else {
      cb(data);
    }
  });
}

function executeQueryPromise(requestData) {
  return new Promise((resolve, reject) => {
    executeQuery(requestData, output => {
      if (!output.status) {
        reject(output.error);
      } else {
        resolve(output.content);
      }
    });
  });
}

function executeQueryStream(requestData, onResultFunction, cb) {
  const dbConfig = requestData.dbConfig;
  const query = requestData.rawQuery;
  const objConnection = databaseConnector.identify(dbConfig);
  objConnection.connect(dbConfig, function (err, connection) {
    if (err != undefined) {
      console.log('connection error: ', err);
      const e = err;
      //e.exception=ex;
      cb({
        status: false,
        error: e
      });
    } else {
      const objExecutor = databaseExecutor.identify(dbConfig);
      objExecutor.executeQueryStream(connection, query, onResultFunction, cb);
    }
  });
};


function executeQueryStreamPromise(requestData, onResultFunction) {
  return new Promise((resolve, reject) => {
    const dbConfig = requestData.dbConfig;
    const query = requestData.rawQuery;

    getPasswordValue(dbConfig, (updatedDBConfig) => {
      if (updatedDBConfig) {
        const objConnection = databaseConnector.identify(updatedDBConfig);
        objConnection.connect(updatedDBConfig, (err, connection) => {
          if (err != undefined) {
            console.log('connection error: ', err);
            // const e = err;
            //e.exception=ex;
            // cb({
            //   status: false,
            //   error: e
            // });
            reject(err);
          } else {
            const objExecutor = databaseExecutor.identify(updatedDBConfig);
            objExecutor.executeQueryStream(connection, query, onResultFunction, output => {
              if (!output.status) {
                reject(output);
              } else {
                resolve()
              }
            });
          }
        });
      }
    })


  })
}

// DS : Handle Multiple Queries with same connection similar to batch queries;

function executeRawQueryWithConnectionPool(dbConfig, rawQuery, cb) {
  try {
    const startTime = new Date();
    getConnectionFromPool(dbConfig, function (result) {
      if (result.status === false) {
        cb(result);
      } else {
        const connection = result.content;
        if (dbConfig.databaseType != null && dbConfig.databaseType.toString().toLowerCase() != 'json') {
          if (rawQuery.length <= 100000000) {
            debug('query: %s', rawQuery);
          } else {
            debug('query: %s', rawQuery.substring(0, 500) + '\n...\n' + rawQuery.substring(rawQuery.length - 500, rawQuery.length));
          }
        }
        const queryStartTime = new Date();
        const objExecutor = databaseExecutor.identify(dbConfig);
        objExecutor.executeQuery(connection, rawQuery, function (result) {
          if (result.status == false) {
            console.log('DB Executor Error', dbConfig, rawQuery);
          }
          debug('Total Time:', (new Date().getTime() - startTime.getTime()) / 1000, 'Query Time:', (new Date().getTime() - queryStartTime.getTime()) / 1000);
          cb(result);
        });
      }
    });
  } catch (ex) {
    console.log('exception: ', ex);
    console.log('Error in Query: ', rawQuery);
    const e = ex;
    //e.exception=ex;
    cb({
      status: false,
      error: e
    });
  }
}

function executeRawQueryInner(dbConfig, rawQuery, shouldCache, tableName, cb) {
  if (!tableName) {
    tableName = '#$table_name_not_available$#';
  }
  const dbConf = JSON.stringify({ host: dbConfig.host, port: dbConfig.port ? dbConfig.port.toString() : dbConfig.port });
  if (shouldCache == true && oldResults[dbConf] && oldResults[dbConf][tableName] && oldResults[dbConf][tableName][rawQuery]) {
    let result = oldResults[dbConf][tableName][rawQuery].result;
    if (dbConfig.databaseType == 'redshift') {
      result = result.map(d => {
        return !Array.isArray(d)
          ? convertObject(d)
          : d.map(innerD => {
            return convertObject(innerD);
          });
      });
    } else {
      result = axiomUtils.extend(true, [], result);
    }
    cb({ status: true, content: result });
  } else {
    if (dbConfig.hasOwnProperty('connectionLimit') && dbConfig.connectionLimit == 0) {
      debug('With New Connection');
      executeRawQueryWithConnection(dbConfig, rawQuery, function (responseData) {
        cb(responseData);
        if (shouldCache == true && responseData.status == true) {
          saveToCache(responseData.content, dbConfig, rawQuery, tableName);
        }
      });
    } else {
      debug('With Connection Pool');
      executeRawQueryWithConnectionPool(dbConfig, rawQuery, function (responseData) {
        cb(responseData);
        if (shouldCache == true && responseData.status == true) {
          saveToCache(responseData.content, dbConfig, rawQuery, tableName);
        }
      });
    }
  }
}

function getConnectionFromPool(dbConfig, cb) {
  try {
    const connectionString = dbConfig.databaseType + '://' + dbConfig.user + ':' + dbConfig.password + '@' + dbConfig.host + ':' + dbConfig.port + '/' + dbConfig.database;
    // CHeck if expiresOnTimestamp exists & if its less then current timestamp
    // else fecth new instance of pool/connection  
    if (global._connectionPools.hasOwnProperty(connectionString) && (!global._connectionPools[connectionString].hasOwnProperty("config") || !global._connectionPools[connectionString]["config"]["expiresOnTimestamp"] || 
      global._connectionPools[connectionString]["config"]["expiresOnTimestamp"] > new Date().valueOf() ) ) {
      cb({
        status: true,
        content: global._connectionPools[connectionString]
      });
      return;
    } else {
      const objConnection = databaseConnector.identify(dbConfig);
      objConnection.connectPool(dbConfig, function (err, pool) {
        if (err != undefined) {
          console.log('connection error: ', err);
          const e = err;
          //e.exception=ex;
          cb({
            status: false,
            error: e
          });
        } else {
          global._connectionPools[connectionString] = pool;
          cb({
            status: true,
            content: pool
          });
        }
      });
    }
  } catch (ex) {
    console.log('exception: ', ex);
    const e = ex;
    //e.exception=ex;
    cb({
      status: false,
      error: e
    });
  }
}

function saveToCache(finalData, dbConfig, queryString, tableName) {
  if (dbConfig.databaseType != null && dbConfig.databaseType.toString().toLowerCase() == 'json') {
    const errorMessage = "Caching result is not supported in JSON type database, please set shouldCache to false or remove it";
    console.error(errorMessage);
    throw new Error(errorMessage);
  }
  const dbConf = JSON.stringify({ host: dbConfig.host, port: dbConfig.port ? dbConfig.port.toString() : dbConfig.port });
  if (!oldResults[dbConf]) {
    oldResults[dbConf] = {};
  }
  if (!oldResults[dbConf][tableName]) {
    oldResults[dbConf][tableName] = {};
  }
  oldResults[dbConf][tableName][queryString] = {
    result: finalData
  };
  // console.log("################################## after saving JSON.stringify(oldResults) ###########################################")
  // console.log(JSON.stringify(oldResults))
  // console.log("################################## after saving JSON.stringify(oldResults) ###########################################")
}

function flushCache(dbConfig, tableName) {
  const dbConf = JSON.stringify({ host: dbConfig.host, port: dbConfig.port ? dbConfig.port.toString() : dbConfig.port });
  // console.log("################################## flush cache ###########################################")
  // console.log({"oldResults": oldResults})
  // console.log({"dbConf": dbConf});
  // console.log({"dbConfig": dbConfig});
  // console.log({"oldResults[dbConf]": oldResults[dbConf]});
  // console.log({"tableName": tableName});
  // console.log("################################## flush cache ###########################################")
  if (oldResults[dbConf]) {
    if (oldResults[dbConf][tableName]) {
      oldResults[dbConf][tableName] = {};
    }
  }
};

function convertObject(row) {
  return new Proxy(row, {
    get: function (target, name) {
      if (typeof name !== 'string') {
        return undefined;
      }
      if (!(name.toLowerCase() in target)) {
        return undefined;
      }
      return target[name.toLowerCase()];
    },
    set: function (target, name, value) {
      if (typeof name !== 'string') {
        return undefined;
      }
      target[name.toLowerCase()] = value;
    }
  });
}

function executeRawQueriesWithSpecificConnection(dbConfig, connection, queries, cb) {
  const objExecutor = databaseExecutor.identify(dbConfig);
  let allErrs = [],
    allResults = [],
    allFields = [];
  function processQuery(index) {
    if (index >= queries.length || (allErrs.length && allErrs[allErrs.length - 1])) {
      cb(allErrs, allResults, allFields);
      return;
    }
    objExecutor.executeQuery(connection, queries[index], function (result) {
      if (result.status) {
        allErrs.push(null);
        allResults.push(result.content);
      } else {
        allErrs.push(result.error);
        allResults.push(null);
      }
      allFields.push(null);
      processQuery(index + 1);
    });
  }
  processQuery(0);
}

function executeRawQueriesWithConnection(requestData, cb) {
  try {
    const dbConfig = axiomUtils.extend({}, true, requestData.dbConfig);
    const rawQueries = requestData.rawQueries;
    getPasswordValue(dbConfig, (updatedDBConfig) => {
      if (updatedDBConfig) {
        const objConnection = databaseConnector.identify(updatedDBConfig);
        objConnection.connect(updatedDBConfig, function (err, connection) {
          if (err != undefined) {
            console.log('connection error: ', err);
            const e = err;
            //e.exception=ex;
            cb({
              status: false,
              error: e
            });
          } else {
            executeRawQueriesWithSpecificConnection(updatedDBConfig, connection, rawQueries, function (allErrs, allResults, allFields) {
              objConnection.disconnect(connection);
              cb(allErrs, allResults, allFields);
            });
          }
        });
      }
    })
  } catch (ex) {
    console.log('exception: ', ex);
    const e = ex;
    //e.exception=ex;
    cb({
      status: false,
      error: e
    });
  }
};


function executeRawQueriesWithConnectionPromise(requestData) {
  return new Promise((resolve, reject) => {
    try {
      const dbConfig = requestData.dbConfig;
      const rawQueries = requestData.rawQueries;
      const objConnection = databaseConnector.identify(dbConfig);
      objConnection.connect(dbConfig, function (err, connection) {
        if (err != undefined) {
          console.log('connection error: ', err);
          reject(err);
        } else {
          executeRawQueriesWithSpecificConnection(dbConfig, connection, rawQueries, function (allErrs, allResults, allFields) {
            objConnection.disconnect(connection);
            resolve(allErrs, allResults, allFields);
          });
        }
      });
    } catch (ex) {
      console.log('exception: ', ex);
      reject(ex);
    }
  })
};

function executeFluxQuery(requestData, cb) {
  const dbConfig = requestData.dbConfig;
  const query = requestData.query;
  getPasswordValue(dbConfig, (updatedDBConfig) => {
    if (updatedDBConfig) {
      const options = {
        hostname: updatedDBConfig.host,
        port: updatedDBConfig.port,
        path: '/api/v2/query',
        method: 'POST',
        headers: {
          'Accept': 'application/csv',
          'Content-Type': 'application/vnd.flux',
          'Content-Length': Buffer.byteLength(query)
        }
      }
      http.request(options, res => {
        let data = ""
        res.on("data", d => {
          data += d
        })
        res.on("end", () => {
          cb({
            status: true,
            content: data
          })
        })
      }).on("error", (err) => {
        cb({
          status: false,
          error: err
        })
      }).end(query);
    }
  })
}

function executeFluxQueryPromise(requestData) {
  return new Promise((resolve, reject) => {
    executeFluxQuery(requestData, output => {
      if (!output.status) {
        reject(output.error);
      } else {
        resolve(output.content);
      }
    });
  });
}

module.exports = {
  executeRawQuery: executeRawQuery,
  executeQuery: executeQuery,
  flushCache: flushCache,
  executeQueryStream: executeQueryStream,
  executeRawQueriesWithConnection: executeRawQueriesWithConnection,
  executeFluxQuery: executeFluxQuery,
  promise: {
    executeRawQuery: executeRawQueryPromise,
    executeQuery: executeQueryPromise,
    flushCache: flushCache,
    executeQueryStream: executeQueryStreamPromise,
    executeRawQueriesWithConnection: executeRawQueriesWithConnectionPromise,
    executeFluxQuery: executeFluxQueryPromise
  }
};
