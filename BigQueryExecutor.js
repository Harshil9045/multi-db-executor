const debug = require("debug")("database-executor:bigquery");

function updateQueryAsPerBigQuerySyntax(query) {
  return query.replace(/\blimit\s+(\d+)\s*,\s*(\d+)/gi, function (match, offset, count) {
    return "LIMIT " + count + " OFFSET " + offset;
  });
}

function __clientOptions(connection) {
  return (connection && connection.__axiomQueryOptions) || { queryOptions: {}, timezone: "local" };
}

function __logQuery(rawQuery) {
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
}

function __toError(err) {
  if (err && typeof err == "object" && err.sqlMessage === undefined) {
    err.sqlMessage = err.message;
  }
  return err;
}

function __acquireSlot(connection) {
  return new Promise((resolve, reject) => {
    const limiter = connection && connection.__axiomLimiter;
    if (!limiter) {
      resolve(function () {});
      return;
    }
    limiter.acquire((err, release) => (err ? reject(err) : resolve(release)));
  });
}

async function executeQuery(connection, rawQuery, cb) {
  rawQuery = updateQueryAsPerBigQuerySyntax(rawQuery);
  __logQuery(rawQuery);
  const options = __clientOptions(connection);
  let output;
  let release = null;
  try {
    release = await __acquireSlot(connection);
    const [results] = await connection.query(
      Object.assign({}, options.queryOptions, { query: rawQuery })
    );
    output = {
      status: true,
      content: __processResult(results, options.timezone),
    };
  } catch (err) {
    debug("query", err);
    output = {
      status: false,
      error: __toError(err),
    };
  } finally {
    if (release) release();
  }
  cb(output);
}

function executeQueryStream(connection, query, onResultFunction, cb) {
  const limiter = connection && connection.__axiomLimiter;
  if (!limiter) {
    __executeQueryStream(connection, query, onResultFunction, cb);
    return;
  }
  limiter.acquire((err, release) => {
    if (err) {
      cb({ status: false, error: __toError(err) });
      return;
    }
    __executeQueryStream(connection, query, onResultFunction, (output) => {
      release();
      cb(output);
    });
  });
}

function __executeQueryStream(connection, query, onResultFunction, cb) {
  query = updateQueryAsPerBigQuerySyntax(query);
  __logQuery(query);
  const options = __clientOptions(connection);
  let finished = false;
  function finish(output) {
    if (!finished) {
      finished = true;
      cb(output);
    }
  }
  let stream;
  try {
    stream = connection.createQueryStream(
      Object.assign({}, options.queryOptions, { query: query })
    );
  } catch (err) {
    finish({ status: false, error: __toError(err) });
    return;
  }
  stream
    .on("error", function (err) {
      debug("query", err);
      finish({ status: false, error: __toError(err) });
    })
    .on("data", function (row) {
      stream.pause();
      onResultFunction(__convertRow(row, options.timezone), function () {
        stream.resume();
      });
    })
    .on("end", function () {
      finish({ status: true });
    });
}

function __processResult(results, timezone) {
  return results.map((row) => __convertRow(row, timezone));
}

function __convertRow(row, timezone) {
  Object.keys(row).forEach((prop) => {
    row[prop] = __convertValue(row[prop], timezone);
  });
  return row;
}

function __offset(timezone) {
  if (!timezone || timezone == "local") return "";
  if (timezone == "Z" || timezone == "UTC") return "Z";
  return timezone;
}

function __convertValue(value, timezone) {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value != "object") {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((v) => __convertValue(v, timezone));
  }
  if (Buffer.isBuffer(value)) {
    return value.toString("base64");
  }
  if (value instanceof Date) {
    return value;
  }
  const typeName = value.constructor ? value.constructor.name : "";
  switch (typeName) {
    case "BigQueryDate":
      return new Date(value.value + "T00:00:00" + __offset(timezone));
    case "BigQueryDatetime":
      return new Date(value.value + __offset(timezone));
    case "BigQueryTimestamp":
      return new Date(value.value);
    case "BigQueryTime":
    case "Geography":
      return value.value;
    case "Big":
      return Number(value.toString());
    case "BigQueryInt":
      return Number(value.value);
  }
  if (Object.prototype.hasOwnProperty.call(value, "value") && Object.keys(value).length == 1) {
    return value.value;
  }
  const converted = {};
  Object.keys(value).forEach((key) => {
    converted[key] = __convertValue(value[key], timezone);
  });
  return converted;
}

module.exports = {
  executeQuery: executeQuery,
  executeQueryStream: executeQueryStream,
  updateQueryAsPerBigQuerySyntax: updateQueryAsPerBigQuerySyntax,
  convertValue: __convertValue,
};
