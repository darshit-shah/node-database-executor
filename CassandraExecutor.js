var debug = require('debug')('database-executor:cassandra-executor');
const driver = require('cassandra-driver')

function executeQuery(connection, rawQuery,cb) {
  if (rawQuery.length <= 100000000) {
    debug('query: %s', rawQuery);
  } else {
    debug('query: %s', rawQuery.substring(0, 500) + "\n...\n" + rawQuery.substring(rawQuery.length - 500, rawQuery.length));
  }
  connection.execute(rawQuery, undefined, {consistency: driver.types.consistencies.quorum}, function(err, results) {
    if (err) {
      if(err.message.startsWith("PRIMARY KEY column ") && err.message.indexOf("cannot be restricted as preceding column")>-1 && err.message.endsWith(" is not restricted") && rawQuery.toLowerCase().indexOf("allow filtering")===-1) {
        if(rawQuery.trim().endsWith(";")){
          rawQuery = rawQuery.trim();
          rawQuery = rawQuery.substring(0,rawQuery.length-1);
        }
        return executeQuery(connection, rawQuery+" allow filtering;",cb);
      } else if(err.message.startsWith('Cannot execute this query') && err.message.endsWith('use ALLOW FILTERING') && err.message.indexOf('might involve data filtering') > -1 && err.message.indexOf('may have unpredictable performance') > -1) {
        if(rawQuery.trim().endsWith(";")) {
          rawQuery = rawQuery.trim();
          rawQuery = rawQuery.substring(0,rawQuery.length-1);
        }
        return executeQuery(connection, `${rawQuery} allow filtering;`,cb);
      }
      debug("query", err);
      var e = err;
      cb({
        status: false,
        error: err
      });
    } else {
      cb({
        status: true,
        content: results.rows
      });
    }
  });
}

function executeQueryStream(connection, query, onResultFunction, cb) {
  var queryExecutor = connection.stream(query);
  queryExecutor
    .on('error', function(err) {
      cb({
        status: false,
        error: err
      });
      // Handle error, an 'end' event will be emitted after this as well
    })
    .on('readable', function(row) {
      // Pausing the connnection is useful if your processing involves I/O

      // connection.pause();
      // 'readable' is emitted as soon a row is received and parsed
      var row;
      while (row = this.read()) {
        onResultFunction(row, function() {
          // connection.resume();
        });
      }
    })
    .on('end', function() {
      cb({
        status: true
      });

    });
}

module.exports = {
  executeQuery: executeQuery,
  executeQueryStream: executeQueryStream
}
