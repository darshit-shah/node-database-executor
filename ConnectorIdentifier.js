exports.identify = function () {
    return identifyConnection(arguments[0]);
}

function identifyConnection(json) {
    var objConnection = null;
    if (json.type == 'database') {
        if (json.databaseType == "mysql") {
            objConnection = require('./MySQLExecutor.js');
        }
        else if (json.databaseType == "mssql") {
            objConnection = require('./MSSQLExecutor.js');
        }
        else if (json.databaseType == "cassandra") {
            objConnection = require('./CassandraExecutor.js');
        }
        /*else if (json.databaseType == "mssql") {
            objConnection = require('./MSSQLConnector.js');
        }
        else if (json.databaseType == "elasticsearch") {
            objConnection = require('./ElasticSearchConnector.js');
        }*/
    }
    else {

    }
    return objConnection;
}