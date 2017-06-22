var debug = require('debug')('database-executor:cassandra-executor');
function executeQuery(connection, rawQuery,cb) {
  if (rawQuery.length <= 100000000) {
    debug('query: %s', rawQuery);
  } else {
    debug('query: %s', rawQuery.substring(0, 500) + "\n...\n" + rawQuery.substring(rawQuery.length - 500, rawQuery.length));
  }
  connection.execute(rawQuery, function(err, results) {
    if (err) {
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
