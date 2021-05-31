var debug = require('debug')('database-executor:snowflake-executor');
function executeQuery(connection, rawQuery, cb) {
  if (rawQuery.length <= 100000000) {
    debug('query: %s', rawQuery);
  } else {
    debug('query: %s', rawQuery.substring(0, 500) + "\n...\n" + rawQuery.substring(rawQuery.length - 500, rawQuery.length));
  }
  connection.execute({
		sqlText: rawQuery,
		complete: function(err, stmt, rows) {
      if (err) {
        debug("query", err.message);
        var e = err;
        cb({
          status: false,
          error: e
        });
      } else {
        cb({
          status: true,
          content: rows
        });
      }
			// if (err) {
			// 	console.error('Failed to execute statement due to the following error: ' + err.message);
			// } else {
			// 	console.log('Successfully executed statement: ' + stmt.getSqlText());
			// 	console.log('Number of rows: ' + stmt.getNumRows());
			// 	console.log('Rows: ', JSON.stringify(rows[0],null,2));
			// }
		}
	});
}

function executeQueryStream(connection, query, onResultFunction, cb){
  throw Error("Not implemented")
}

module.exports = {
  executeQuery: executeQuery,
  executeQueryStream: executeQueryStream
}
