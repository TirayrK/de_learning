function transform(inJson) {
  try {
    var obj = JSON.parse(inJson);
    var results = [];

    // Handle multiple message formats
    if (Array.isArray(obj)) {
      // Array of Debezium messages
      for (var i = 0; i < obj.length; i++) {
        var transformed = processDebeziumMessage(obj[i]);
        if (transformed) {
          results.push(transformed);
        }
      }
    } else if (obj.messages && Array.isArray(obj.messages)) {
      // Batch object with messages array
      for (var i = 0; i < obj.messages.length; i++) {
        var transformed = processDebeziumMessage(obj.messages[i]);
        if (transformed) {
          results.push(transformed);
        }
      }
    } else if (obj.records && Array.isArray(obj.records)) {
      // Object with records array
      for (var i = 0; i < obj.records.length; i++) {
        var transformed = processDebeziumMessage(obj.records[i]);
        if (transformed) {
          results.push(transformed);
        }
      }
    } else if (obj.payload) {
      // Single Debezium message
      var transformed = processDebeziumMessage(obj);
      if (transformed) {
        results.push(transformed);
      }
    }

    // Return results
    if (results.length === 1) {
      return JSON.stringify(results[0]);
    } else if (results.length > 1) {
      return results.map(function(record) {
        return JSON.stringify(record);
      }).join('\n');
    }

    return null;

  } catch (e) {
    // No console.error - just return null for bad messages
    return null;
  }
}

function processDebeziumMessage(message) {
  try {
    if (!message.payload) {
      return null;
    }

    var payload = message.payload;
    var result = {};

    // Extract data based on operation type
    if (payload.op === 'c' || payload.op === 'r') {
      // CREATE or READ - use 'after' data
      if (payload.after) {
        copyFields(payload.after, result);
        return result;
      }
    } else if (payload.op === 'u') {
      // UPDATE - use 'after' data
      if (payload.after) {
        copyFields(payload.after, result);
        return result;
      }
    } else if (payload.op === 'd') {
      // DELETE - use 'before' data
      if (payload.before) {
        copyFields(payload.before, result);
        return result;
      }
    } else {
      // Fallback - try 'after' first, then 'before'
      if (payload.after) {
        copyFields(payload.after, result);
        return result;
      } else if (payload.before) {
        copyFields(payload.before, result);
        return result;
      }
    }

    return null;

  } catch (e) {
    return null;
  }
}

function copyFields(source, target) {
  for (var key in source) {
    if (source.hasOwnProperty(key)) {
      target[key] = source[key];
    }
  }
}