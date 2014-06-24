var async = require('async');
var Padlock = require('padlock').Padlock;
var lupBatch = require('levelup/lib/batch');
var util = require('util');
var through = require('through');
var concat = require('concat-stream');

function Batch(db) {
    this.db = db;
    lupBatch.call(this, db);
}

util.inherits(Batch, lupBatch);

Batch.prototype.put = function (key, value, opts) {
    key = this._levelup.preProcessKey(key, opts);
    return lupBatch.prototype.put.call(this, key, value, opts);
};

Batch.prototype.del = function (key, opts) {
    key = this._levelup.preProcessKey(key, opts);
    return lupBatch.prototype.del.call(this, key, opts);
}


function AtomicHooks(indb) {

    function DB () {}
    DB.prototype = indb
    var db = new DB()

    db.parent = indb;

    var putHooks = [];
    var delHooks = [];
    var getOverride = null;
    var readStreamOverride = null;
    var keyPreProcessors = [];
    var keyPostProcessors = [];

    db.atomichooks = true;

    db.writeLock = new Padlock();

    db.registerPutHook = function (hook) {
        putHooks.push(hook);
    };

    db.registerGetOverride = function (hook) {
        getOverride = hook;
    };

    db.registerReadStreamOverride = function (hook) {
        readStreamOverride = hook;
    };

    db.registerDelHook = function (hook) {
        delHooks.push(hook);
    };

    db.registerKeyPreProcessor = function (hook) {
        keyPreProcessors.push(hook);
    };

    db.registerKeyPostProcessor = function (hook) {
        keyPostProcessors.unshift(hook);
    };

    db.batch = function (arr, opts, callback) {
        var outarrs;
        var batch;
        if (typeof arr !== 'undefined') {
            outarrs = arr.map(function (item) {
                item.key = db.preProcessKey(item.key, opts);
                return item;
            });
            return db.parent.batch.call(db, outarrs, opts, callback);
        }
        return new Batch(db);
    }

    db.preProcessKey = function (key, opts) {
        key = key || '';
        for (var i = 0, l = keyPreProcessors.length; i < l; i++) {
            key = keyPreProcessors[i](key, opts);
        }
        return key;
    };

    db.postProcessKey = function (key, opts) {
        for (var i = 0, l = keyPostProcessors.length;
             i < l;
             key = keyPostProcessors[i](key, opts), i++);
        return key;
    };

    db.preProcessKV = function (kv, opts, callback) {
        async.reduce(kvPreProcessors, kv, function (kv, processor, rcb) {
            processor(kv, opts, rcb);
        }, callback);
    };

    db.postProcessKV = function (kv, opts, callback) {
        async.reduce(kvPreProcessors, kv, function (kv, processor, rcb) {
            processor(kv, opts, rcb);
        }, callback);
    };

    db.nonlockPut = function (key, value, opts, callback) {
        key = db.preProcessKey(key, opts);
        db.parent.put(key, value, opts, callback);
    }
    
    db.nonlockDel = function (key, value, opts, callback) {
        key = db.preProcessKey(key, opts);
        db.parent.del(key, opts, callback);
    }

    db.put = function (key, value, opts, callback) {
        if (typeof opts === 'function') {
            callback = opts;
            opts = {};
        }
        db.writeLock.runwithlock(function () {
            var batch = db.batch();
            //batch preprocesses key for us
            batch.put(key, value, opts);
            async.each(putHooks,
                function (hook, ecb) {
                    hook(key, value, opts, batch, ecb);
                },
                function (err) {
                    if (err) {
                        db.writeLock.release();
                        callback(err);
                    } else {
                        batch.write(function (err) {
                            db.writeLock.release();
                            callback(err);
                        });
                    }
                }
            );
        });
    };

    db.get = function (key, opts, callback) {
        if (typeof opts === 'function') {
            callback = opts;
            opts = {};
        }
        key = db.preProcessKey(key, opts);
        if (typeof getOverride === 'function') {
            getOverride(key, opts, callback);
        } else {
            db.parent.get.call(db, key, opts, callback);
        }
    };

    db.del = function (key, opts, callback) {
        if (typeof opts === 'function') {
            callback = opts;
            opts = {};
        }

        db.writeLock.runwithlock(function () {
            var batch = db.batch();
            batch.del(key, opts);
            async.each(delHooks,
                function (hook, ecb) {
                    hook(key, opts, batch, ecb);
                },
                function (err) {
                    if (err) {
                        db.writeLock.release();
                        callback(err);
                    } else {
                        batch.write(function (err) {
                            db.writeLock.release();
                            callback(err);
                        });
                    }
                }
            );
        });
    };

    db.createReadStream = function (opts) {
        opts.start = db.preProcessKey(opts.start, opts);
        opts.end = db.preProcessKey(opts.end, opts);

        var readStreamTransform = through(function (data) {
            console.log("data", data);
            if (typeof data === 'object') {
                if (data.hasOwnProperty('key')) {
                    data.key = db.postProcessKey(data.key, opts);
                }
            } else if (opts.keys === true) {
                data = db.postProcessKey(data, opts);
            }
            this.emit('data', data);
        }, undefined, {objectMode: true});
        var rs;
        if (typeof readStreamOverride === 'function') {
            rs = readStreamOverride(opts);
        } else {
            rs = db.parent.createReadStream(opts);
        }
        return rs.pipe(readStreamTransform);
    };

    db.createWriteStream = function (opts) {
        var writeStreamTransform = through(function (data) {
            data.key = db.postProcessKey(data.key, opts);
            this.emit('data', data);
        }, undefined, {objectMode: true});
        var rs = db.parent.createWriteStream(opts);
        return rs.pipe(writeStreamTransform);
    };

    return db;
}

module.exports = AtomicHooks;
