var async = require('async');
var Padlock = require('padlock').Padlock;

function AtomicHooks(db) {
    var putHooks = [];
    var delHooks = [];
    var getHooks = [];
    var readStreamHooks = [];
    var writeStreamHooks = [];

    db.writeLock = new Padlock();

    db.registerPutHook = function (hook) {
        putHooks.push(hook);
    };

    db.registerDelHook = function (hook) {
        delHooks.push(hook);
    };

    db.registerGetHook = function (hook) {
        getHooks.push(hook);
    };

    db.registerReadstreamHook = function (hook) {
        readStreamHook.push(hook);
    };

    db.registerWritestreamHook = function (hook) {
        writeStreamHooks.push(hook);
    };

    db.root = {
        put: db.put,
        del: db.del,
        get: db.get,
    };

    db.put = function (key, value, opts, callback) {
        if (typeof opts === 'function') {
            callback = opts;
            opts = {};
        }
        db.writeLock.runwithlock(function () {
            var batch = db.batch();
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
    
    db.del = function (key, opts, callback) {
        if (typeof opts === 'function') {
            callback = opts;
            opts = {};
        }
        db.writeLock.runwithlock(function () {
            var batch = db.batch();
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

    return db;
}

module.exports = AtomicHooks;
