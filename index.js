var async = require('async');
var Padlock = require('padlock').Padlock;
var lupBatch = require('levelup/lib/batch');
var util = require('util');

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


function AtomicHooks(db) {
    var putHooks = [];
    var delHooks = [];
    var keyPreProcessors = [];
    var keyPostProcessors = [];

    db.atomichooks = true;

    db.writeLock = new Padlock();

    db.registerPutHook = function (hook) {
        putHooks.push(hook);
    };

    db.registerDelHook = function (hook) {
        delHooks.push(hook);
    };

    db.registerKeyPreProcessor = function (hook) {
        keyPreProcessors.push(hook);
    };

    db.registerKeyPostprocessor = function (hook) {
        keyPostProcessors.push(hook);
    };

    db.original = {
        put: db.put,
        del: db.del,
        get: db.get,
        batch: db.batch
    };

    db.batch = function (arr, opts, callback) {
        var outarrs;
        var batch;
        if (typeof arr !== 'undefined') {
            outarrs = arr.map(function (item) {
                item.key = db.preProcessKey(item.key, opts);
                return item;
            });
            return db.original.batch.call(db, outarrs, opts, callback);
        }
        return new Batch(db);
    }

    db.preProcessKey = function (key, opts) {
        for (var i = 0, l = keyPreProcessors.length; i < l; i++) {
            key = keyPreProcessors[i](key, opts);
        }
        return key;
    };

    db.postProcessKey = function (key, opts) {
        for (var i = 0, l = keyPostProcessors.l;
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

    db.put = function (key, value, opts, callback) {
        if (typeof opts === 'function') {
            callback = opts;
            opts = {};
        }
        db.writeLock.runwithlock(function () {
            var batch = db.batch();
            if (putHooks.length === 0) {
                batch.put(key, value, opts);
                batch.write(callback);
            } else {
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
            }
        });
    };

    db.get = function (key, opts, callback) {
        if (typeof opts === 'function') {
            callback = opts;
            opts = {};
        }
        key = db.preProcessKey(key, opts);
        console.log("getting key", key);
        db.original.get.call(db, key, opts, callback);
    };
    
    db.del = function (key, opts, callback) {
        if (typeof opts === 'function') {
            callback = opts;
            opts = {};
        }
        key = db.preProcessKey(key, opts);

        db.writeLock.runwithlock(function () {
            var batch = db.batch();
            if (delHooks.length === 0) {
                batch.del(key);
                batch.write(callback);
            } else {
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
            }
        });
    };

    return db;
}

module.exports = AtomicHooks;
