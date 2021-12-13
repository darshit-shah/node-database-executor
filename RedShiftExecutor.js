var debug = require('debug')('database-executor:redshift-executor');
var axiom_utils = require("axiom-utils");

function updateQueryAsPerRedShiftSyntax(query) {

  query = query.replace(/`\.`/g, "");
  query = query.replace(/`/g, '"');

  var limitMatch = query.toLowerCase().match(new RegExp(/limit [0-9]+,[0-9]+[;]*/ig));
  if (limitMatch && limitMatch.length) {
    query = query.replace(new RegExp(limitMatch[0], "ig"), "");
    var limitAndOffset = limitMatch[0].replace(/limit/ig, "").replace(/;/ig, "").trim().split(",");
    var offset = parseInt(limitAndOffset[0].trim());
    var limit = parseInt(limitAndOffset[1].trim());
    query = query + "LIMIT " + limit + " OFFSET " + offset;
  }
  return query;
}

function executeQuery(connection, rawQuery, cb) {
  rawQuery = updateQueryAsPerRedShiftSyntax(rawQuery);
  if (rawQuery.length <= 100000000) {
    debug('query: %s', rawQuery);
  } else {
    debug('query: %s', rawQuery.substring(0, 500) + "\n...\n" + rawQuery.substring(rawQuery.length - 500, rawQuery.length));
  }


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
        content: results.rows.map(d => {
          return (!Array.isArray(d) ? __convertToCaseInsensitiveAndNumberIfPossible(d) : d.map(innerD => {
            return __convertToCaseInsensitiveAndNumberIfPossible(innerD);
          }));
        })
      });
    }
  });
}

function executeQueryStream(connection, query, onResultFunction, cb) {
  query = updateQueryAsPerRedShiftSyntax(query);

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

      onResultFunction(__convertToCaseInsensitiveAndNumberIfPossible(row), function() {
        connection.resume();
      });
    })
    .on('end', function() {
      cb({
        status: true
      });

    });
}

function __convertToCaseInsensitiveAndNumberIfPossible(row){
  Object.keys(row).forEach((column)=>{ row[column] = axiom_utils.convertToNumericIfPossible(row[column]); });
  return axiom_utils.convertObjectKeysCaseInsensitive(row);
}

module.exports = {
  executeQuery: executeQuery,
  executeQueryStream: executeQueryStream
}


