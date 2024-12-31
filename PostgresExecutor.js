const debug = require('debug')('database-executor:postgres-executor');

function executeQuery(connection, rawQuery, cb) {
  if (typeof rawQuery === "string") {
    if (rawQuery.length <= 100000000) {
      if (connection?.debug !== false) {
        debug('query: %s', rawQuery);
      }
    } else {
      if (connection?.debug !== false) {
        debug('query: %s', rawQuery.substring(0, 500) + "\n...\n" + rawQuery.substring(rawQuery.length - 500));
      }
    }
  } else {
    if (rawQuery.sql && rawQuery.sql.length <= 100000000) {
      if (connection?.debug !== false) {
        debug('query: %s', rawQuery.sql); 
      }
    } else {
      if (connection?.debug !== false && rawQuery.sql && rawQuery.sql.length) { 
        debug('query: %s', rawQuery.sql.substring(0, 500) + "\n...\n" + rawQuery.sql.substring(rawQuery.sql.length - 500));
      }
    }
  }

  connection.query(rawQuery, (err, results) => {
    if (err) {
      debug("query error", err);
      cb({
        status: false,
        error: err
      });
    } else {
      cb({
        status: true,
        content: results.rows // **Changed from 'results' to 'results.rows'**
      });
    }
  });
}

function executeQueryStream(connection, query, onResultFunction, cb) {
  const queryExecutor = connection.query(query);

  queryExecutor
    .on('error', (err) => {
      cb({
        status: false,
        error: err
      });
    })
    .on('row', (row) => { 
      onResultFunction(row, () => {});
    })
    .on('end', () => {
      cb({
        status: true
      });
    });
}

module.exports = {
  executeQuery: executeQuery,
  executeQueryStream: executeQueryStream
};
