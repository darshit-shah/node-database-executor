const debug = require('debug')('database-executor:influx-executor');
async function executeQuery(connection, rawQuery, cb) {
  if (rawQuery.length <= 100000000) {
    debug('query: %s', rawQuery);
  } else {
    debug('query: %s', rawQuery.substring(0, 500) + "\n...\n" + rawQuery.substring(rawQuery.length - 500, rawQuery.length));
  }
  try {
    const results = await connection.query(rawQuery);
    cb({
      status: true,
      content: results
    });
  } catch (err) {
    debug("query", err);
    cb({
      status: false,
      error: err
    });
  }
}

module.exports = {
  executeQuery: executeQuery,
}
