var debug = require('debug')('database-executor:redshift-executor');


console.log("In redshift executor---------------------")
function executeQuery(connection, rawQuery, cb) {
  if (rawQuery.length <= 100000000) {
    debug('query: %s', rawQuery);
  } else {
    debug('query: %s', rawQuery.substring(0, 500) + "\n...\n" + rawQuery.substring(rawQuery.length - 500, rawQuery.length));
  }
  rawQuery = rawQuery.replace(/`/g, "")
  console.log(rawQuery,"------------raw query")
  connection.query(rawQuery, function(err, results) {
    if (err) {
      debug("query", err);
      var e = err;
      cb({
        status: false,
        error: e
      });
    } else {
      cb({
        status: true,
        content: results
      });
    }
  });
}

function executeQueryStream(connection, query, onResultFunction, cb){
  query = query.replace(/`/g, "")
  console.log(query,"------------------- query stream")
  var queryExecutor = connection.query(query);
      queryExecutor
        .on('error', function(err) {
          cb({
            status: false,
            error: err
          });
          // Handle error, an 'end' event will be emitted after this as well
        })
        .on('fields', function(fields) {
          // the field packets for the rows to follow
        })
        .on('result', function(row) {
          // Pausing the connnection is useful if your processing involves I/O
          connection.pause();

          onResultFunction(row, function() {
            connection.resume();
          });
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
