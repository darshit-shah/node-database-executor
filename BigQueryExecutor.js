const debug = require("debug")("database-executor:bigquery");
async function executeQuery(connection, rawQuery, cb) {
  if (rawQuery.length <= 100000000) {
    debug("query: %s", rawQuery);
  } else {
    debug(
      "query: %s",
      rawQuery.substring(0, 500) +
        "\n...\n" +
        rawQuery.substring(rawQuery.length - 500, rawQuery.length)
    );
  }
  try {
    const [results] = await connection.query(rawQuery);
    const postResult = __processResult(results);
    cb({
      status: true,
      content: postResult,
    });
  } catch (err) {
    debug("query", err);
    cb({
      status: false,
      error: err,
    });
  }
}

function __processResult(postResult) {
  const newResult = [...postResult];
  postResult.forEach((row) => {
    Object.keys(row).forEach((prop) => {
      if (row[prop] === null || row[prop] === undefined) {
        row[prop] = null;
      } else if (typeof row[prop] == "object") {
        if (row[prop].hasOwnProperty("value")) {
          if (
            row[prop].constructor.name == "BigQueryDatetime" ||
            row[prop].constructor.name == "BigQueryDate" ||
            row[prop].constructor.name == "BigQueryTime" ||
            row[prop].constructor.name == "BigQueryTimestamp"
          ) {
            row[prop] = new Date(row[prop].value);
          } else {
            console.log(
              "Object recived with unexpected type :" +
                row[prop].constructor.name +
                " ",
              row[prop]
            );
            throw new Error("Object recived with unexpected type");
          }
        } else {
          console.log("Object recived expecting a value property :", row[prop]);
          throw new Error("Expecting Value inside object.");
        }
      } else {
        //Do Nothing
      }
    });
  });
  return newResult;
}

module.exports = {
  executeQuery: executeQuery,
};
