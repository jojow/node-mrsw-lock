var debug = require('debug')('mrsw-lock');
var async = require('async');
var _ = require('lodash');
var uuid = require('uuid');
var RedisPool = require('redis-poolify');

var clientPool = new RedisPool();



var stringifyId = function(id) {
  if (_.isString(id)) return id;
  else return _(id).toArray().sort().value().toString();
};

var releaseScript = 'if redis.call("get", KEYS[1]) == ARGV[1]\n\
                     then\n\
                       return redis.call("del", KEYS[1])\n\
                     else\n\
                       return 0\n\
                     end';

var MRSWLock = function(config) {
  debug('new instance of MRSWLock', config);

  config = config || {};

  var maxReadLockTime = config.maxReadLockTime || 240;
  var maxWriteLockTime = config.maxWriteLockTime || 30;
  var baseDelay = config.baseDelay || 0.1;
  var delayOffsetMin = config.delayOffsetMin || 0.01;
  var delayOffsetMax = config.delayOffsetMax || 0.1;
  var maxRetries = config.maxRetries || 3;

  this.readLock = function(id, callback) {
    var token = uuid.v4();

    var idStr = stringifyId(id);

    var readKey = 'read:' + idStr + ':' + token;
    var writeKey = 'write:' + idStr;

    debug('try to lock for read', id, token);

    async.waterfall([
      async.apply(clientPool.acquire, config),
      function(client, cb) {
        var retries = 0;

        var delay = baseDelay;

        var lock = function(timeout) {
          setTimeout(function() {
            client.multi().
              set([ readKey, 'anyvalue', 'NX', 'EX', maxReadLockTime ]).
              get(writeKey).
              exec(function(err, replies) {
                retries++;
                delay = (delay * 2) + _.random(delayOffsetMin, delayOffsetMax);

                var handleErr = function(err) {
                  client.del(readKey, function(delErr, reply) {
                    if (delErr) console.error(delErr);

                    cb(err);

                    clientPool.release(config, client);
                  });
                };

                if (err) return handleErr(err);
                else if ((replies[0] === 'OK' || replies[0] === 1) && replies[1] === null) cb(null, token);
                else if (retries === maxRetries) return handleErr(new Error('Cannot get read lock after ' + retries + ' retries'));
                else return lock(delay);

                debug('locked for read', id, token);

                clientPool.release(config, client);
              });
          }, timeout * 1000);
        };

        lock(0);
      }
    ], callback);
  };

  this.writeLock = function(id, callback) {
    var token = uuid.v4();

    var idStr = stringifyId(id);

    var readKey = 'read:' + idStr + ':*';
    var writeKey = 'write:' + idStr;

    debug('try to lock for write', id, token);

    async.waterfall([
        async.apply(clientPool.acquire, config),
        function(client, cb) {
          var retries = 0;

          var delay = baseDelay;

          var lock = function(timeout) {
            setTimeout(function() {
              client.multi().
                set([ writeKey, token, 'NX', 'EX', maxWriteLockTime ]).
                keys(readKey).
                exec(function(err, replies) {
                  retries++;
                  delay = (delay * 2) + _.random(delayOffsetMin, delayOffsetMax);

                  var delWriteKey = function(cb) {
                    client.eval([ releaseScript, 1, writeKey, token ], function(evalErr, reply) {
                      if (evalErr) console.error(evalErr);

                      cb();
                    });
                  };

                  var handleErr = function(err) {
                    delWriteKey(function() {
                      cb(err);

                      clientPool.release(config, client);
                    });
                  };

                  if (err) return handleErr(err);
                  else if ((replies[0] === 'OK' || replies[0] === 1) && _.size(replies[1]) === 0) cb(null, token);
                  else if (retries === maxRetries) return handleErr(new Error('Cannot get write lock after ' + retries + ' retries'));
                  else return delWriteKey(function() { lock(delay); });

                  debug('locked for write', id, token);

                  clientPool.release(config, client);
                });
            }, timeout * 1000);
          };

          lock(0);
        }
    ], callback);
  };

  this.readRelease = function(id, token, callback) {
    var idStr = stringifyId(id);

    var readKey = 'read:' + idStr + ':' + token;

    debug('try to release read lock', id, token);

    async.waterfall([
      async.apply(clientPool.acquire, config),
      function(client, cb) {
        client.del(readKey, function(err, reply) {
          if (err) {
              debug('error while releasing read lock', err, id, token);

              return cb(err);
          } else if (reply === 'OK' || reply === 1) {
              cb(null, token);
          } else {
              cb(null);
          }

          debug('read lock released', id, token);

          clientPool.release(config, client);
        });
      }
    ], callback);
  };

  this.writeRelease = function(id, token, callback) {
    var idStr = stringifyId(id);

    var writeKey = 'write:' + idStr;

    debug('try to release write lock', id, token);

    async.waterfall([
      async.apply(clientPool.acquire, config),
      function(client, cb) {
        client.eval([ releaseScript, 1, writeKey, token ], function(err, reply) {
          if (err) {
              debug('error while releasing write lock', err, id, token);

              return cb(err);
          } else if (reply === 'OK' || reply === 1) {
              cb(null, token);
          } else {
              cb(null);
          }

          debug('write lock released', id, token);

          clientPool.release(config, client);
        });
      }
    ], callback);
  };
};

module.exports = MRSWLock;
