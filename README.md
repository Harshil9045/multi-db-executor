> **Fork notice.** `multi-db-executor` is a fork of [node-database-executor](https://www.npmjs.com/package/node-database-executor) 2.4.8 by Darshit Shah (MIT), with BigQuery fixes. It depends on `multi-db-connectors`. Install it under the original name so existing `require("node-database-executor")` calls keep working:
> `npm install node-database-executor@npm:multi-db-executor@1.0.0`
>
> Changes from 2.4.8:
> - `BigQueryExecutor.js`: client options on every query; `LIMIT a,b` rewrite for raw SQL; values returned the way the MySQL driver returns them (NUMERIC/BIGNUMERIC to number, BYTES to base64, GEOGRAPHY to WKT, TIME to string, DATE/DATETIME to `Date` in the configured timezone, ARRAY/STRUCT recursively); new `executeQueryStream`; `sqlMessage` on errors; no double callback.
> - `DatabaseExecutor.js`: BigQuery pool and result-cache keys use project/dataset (other databases' keys unchanged); passwords and key paths masked in `DB Executor Error` logs.

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
