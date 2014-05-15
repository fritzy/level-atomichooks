var AtomicHooks = require('./index');
var concat = require('concat-stream');

function LevelBucket(db, opts) {

    db._configBucket = opts || {};
    db._configBucket.default = db._configBucket.default || 'default';
    db._configBucket.sep = db._configBucket.sep || '!';

    if (!db.atomichooks) {
        AtomicHooks(db);
    }

    db.registerKeyPreProcessor(function (key, opts) {
        opts = opts || {};
        return (opts.bucket || db._configBucket.default) + db._configBucket.sep + key;
    });
    
    db.registerKeyPostProcessor(function (key, opts) {
        opts = opts || {};
        var idx = key.indexOf(db._configBucket.sep);
        if (idx !== -1) {
            key = key.slice(idx + 1);
        }
        return key;
    });

}

var levelup = require('./node_modules/levelup');
var tst = levelup('./testdbforme', {valueEncoding: 'json'});
LevelBucket(tst);

tst.put('testkey', {test: 'no'}, {bucket: 'weee'}, function (err) {
});

tst.batch()
    .put('hi', {derp: 1}, {bucket: 'ham'})
    .put('wat', {derp: 2}, {bucket: 'ham'})
    .write(function (err) {
        tst.get('hi', {bucket: 'ham'}, function (err, value) {
            console.log(err, value);
        });
        tst.createReadStream({bucket: 'ham', start: '!', end: '~'}).pipe(concat(function () {
            console.log(arguments);
        }));
        tst.createKeyStream({bucket: 'ham', start: '!', end: '~'}).pipe(concat(function () {
            console.log(arguments);
        }));
        tst.createValueStream({bucket: 'ham', start: '!', end: '~'}).pipe(concat(function () {
            console.log(arguments);
        }));
    });
