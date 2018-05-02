
var util = require('util');
var Writable = require('stream').Writable;
var AWS = require('aws-sdk');
var safeJsonStringify = require('safe-json-stringify');

var jsonStringify = safeJsonStringify ? safeJsonStringify : JSON.stringify;

module.exports = createCloudWatchStream;

function createCloudWatchStream(opts) {
  return new CloudWatchStream(opts);
}

util.inherits(CloudWatchStream, Writable);
function CloudWatchStream(opts) {
  Writable.call(this, {objectMode: true});
  this.logGroupName = opts.logGroupName;
  this.logStreamName = opts.logStreamName;
  this.writeInterval = opts.writeInterval || 0;
  this.instantWriteLevel = opts.instantWriteLevel || 40;
  this.onError = opts.onError || console.log;

  if (opts.AWS) {
    AWS = opts.AWS;
  }

  this.cloudwatch = new AWS.CloudWatchLogs(opts.cloudWatchLogsOptions);
  this.sequenceToken = null;
}

CloudWatchStream.prototype._write = function _write(record, _enc, cb) {
  this._writeLogs(record);
  cb();
};

CloudWatchStream.prototype._writeLogs = function _writeLogs(record) {
  if (this.sequenceToken === null) {
    return this._getSequenceToken(this._writeLogs.bind(this), record);
  }
  var log = {
    logGroupName: this.logGroupName,
    logStreamName: this.logStreamName,
    sequenceToken: this.sequenceToken,
    logEvents: [createCWLog(record)],
  };
  var obj = this;
  writeLog();

  function writeLog() {
    obj.cloudwatch.putLogEvents(log, function (err, res) {
      if (err) {
        if (err.retryable) return setTimeout(writeLog, obj.writeInterval);
        if (err.code === 'InvalidSequenceTokenException') {
          return obj._getSequenceToken(function () {
            log.sequenceToken = obj.sequenceToken;
            setTimeout(writeLog, obj.writeInterval);
          }, record);
        }
        return obj._error(err);
      }
      obj.sequenceToken = res.nextSequenceToken;
    });
  }
};

CloudWatchStream.prototype._getSequenceToken = function _getSequenceToken(done, record) {
  var params = {
    logGroupName: this.logGroupName,
    logStreamNamePrefix: this.logStreamName
  };
  var obj = this;
  this.cloudwatch.describeLogStreams(params, function (err, data) {
    if (err) {
      if (err.name === 'ResourceNotFoundException') {
        createLogGroupAndStream(obj.cloudwatch, obj.logGroupName, obj.logStreamName, createStreamCb);
        return;
      }
      obj._error(err);
      return;
    }
    if (data.logStreams.length === 0) {
      createLogStream(obj.cloudwatch, obj.logGroupName, obj.logStreamName, createStreamCb);
      return;
    }
    obj.sequenceToken = data.logStreams[0].uploadSequenceToken;
    done(record);
  });

  function createStreamCb(err) {
    if (err) return obj._error(err);
    // call again to verify stream was created - silently fails sometimes!
    obj._getSequenceToken(() => {
      done(record);
    });
  }
};

CloudWatchStream.prototype._error = function _error(err) {
  if (this.onError) return this.onError(err);
  throw err;
};

function createLogGroupAndStream(cloudwatch, logGroupName, logStreamName, cb) {
  cloudwatch.createLogGroup({
    logGroupName: logGroupName
  }, function (err) {
    if (err) return err;
    createLogStream(cloudwatch, logGroupName, logStreamName, cb);
  });
}

function createLogStream(cloudwatch, logGroupName, logStreamName, cb) {
  cloudwatch.createLogStream({
    logGroupName: logGroupName,
    logStreamName: logStreamName
  }, cb);
}

function createCWLog(bunyanLog) {
  var message = {};
  for (var key in bunyanLog) {
    if (key === 'time') continue;
    message[key] = bunyanLog[key];
  }

  var log = {
    message: jsonStringify(message),
    timestamp: new Date(bunyanLog.time).getTime()
  };

  return log;
}
