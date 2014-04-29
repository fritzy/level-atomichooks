var atomichooks = require('../index');
var levelup = require('levelup');

module.exports = {
    "put hook": function (test) {
        var db = levelup('./testdb');
        atomichooks(db);
        db.registerPutHook(function (key, value, opts, batch, done) {
            batch.put(key, 'nope');
            test.equals(key, 'testkey');
            test.equals(value, 'yup');
            test.equals(opts.random, 'howdy');
            done();
        });
        db.put('testkey', 'yup', {random: 'howdy'}, function (err) {
            db.get('testkey', function (err, value) {
                test.equals(value, 'nope');
                db.close();
                test.done();
            });
        });
    },
    "del hook": function (test) {
        var db = levelup('./testdb');
        atomichooks(db);
        db.registerDelHook(function (key, opts, batch, done) {
            batch.del('testkey');
            test.equals(key, 'testkey-bad');
            test.equals(opts.crap, 'cheese');
            done();
        });
        db.del('testkey-bad', {crap: 'cheese'}, function (err) {
            db.get('testkey', function (err, value) {
                test.equals(value, undefined);
                db.close();
                test.done();
            });
        });
    },
};
