[![CodeQL](https://github.com/darshit-shah/node-database-executor/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/darshit-shah/node-database-executor/actions/workflows/github-code-scanning/codeql)
[![Node.js Package](https://github.com/darshit-shah/node-database-executor/actions/workflows/npm-publish.yml/badge.svg?branch=master)](https://github.com/darshit-shah/node-database-executor/actions/workflows/npm-publish.yml)

# node-database-executor

This module use for execute jsonQuery or rawQuery using your database config.
```js
const queryExecutor = require('node-database-executor').promise;
async function runQuery() {
  try {
    const query = {
      table: 'tablename',
      select: []
    };
    const queryConfig = {
      dbConfig: {
        type: 'database',
        databaseType: 'mysql',
        engine: 'MyISAM',
        database: 'database',
        host: 'host',
        port: 'port',
        user: 'user',
        password: 'password',
        connectionLimit:0,
        acquireTimeout: 2000
      },
      query: query
    };
    const jsonQueryData = await queryExecutor.executeQuery(queryConfig);  
    console.log(jsonQueryData);
  } catch (error) {
    console.log(error);
  }
}
runQuery();
```
