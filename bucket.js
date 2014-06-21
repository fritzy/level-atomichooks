var AtomicHooks = require('./index');
var concat = require('concat-stream');
var util = require('util');

function LevelBucket(db, opts) {

    db._configBucket = opts || {};
    db._configBucket.default = db._configBucket.default || 'default';
    db._configBucket.sep = db._configBucket.sep || '!';

    if (!db.atomichooks) {
        db = AtomicHooks(db);
    }

    db.registerKeyPreProcessor(function (key, opts) {
        opts = opts || {};
        var newkey = (opts.bucket || db._configBucket.default) + db._configBucket.sep + key;
        console.log("KPP", key, "->", newkey);
        return newkey;
    });
    
    db.registerKeyPostProcessor(function (key, opts) {
        opts = opts || {};
        var idx = key.indexOf(db._configBucket.sep);
        if (idx !== -1) {
            key = key.slice(idx + 1);
        }
        return key;
    });

    return db;
}

var levelup = require('./node_modules/levelup');
var tst = levelup('./testdbforme', {valueEncoding: 'json'});
tst = LevelBucket(tst);

tst.put('testkey', {test: 'no'}, {bucket: 'weee'}, function (err) {
});

/*
tst.batch()
    .put('hi', {derp: 1}, {bucket: 'ham'})
    .put('wat', {derp: 2}, {bucket: 'ham'})
    .write(function (err) {
        tst.get('hi', {bucket: 'ham'}, function (err, value) {
            console.log(1);
            console.log("%j", arguments);
        });
        tst.createReadStream({bucket: 'ham', start: '!', end: '~'}).pipe(concat(function () {
            console.log(2);
            console.log(util.inspect(arguments, {depth: 10}));
        }));
        tst.createKeyStream({bucket: 'ham', start: '!', end: '~'}).pipe(concat(function () {
            console.log(3);
            console.log(arguments);
        }));
        tst.createValueStream({bucket: 'ham', start: '!', end: '~'}).pipe(concat(function () {
            console.log(4);
            console.log(arguments);
        }));
    });
    */
