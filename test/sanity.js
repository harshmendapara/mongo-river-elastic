'use strict';

const global = {
    mongo: {
        host: "mongodb://localhost:27017",
        db: "test_db",
        collection: 'river_coll'
    },
    elastic: {
        host: 'localhost:9200'
    },

    river: {
        _collection_index_dict:
            {'river_coll': {index: 'river', type: 'test', primaryKeyField: 'primaryKey'}},
        options: {logLevel: 'debug', retryCount: 3}
    }
};


const elasticsearch = require('elasticsearch');
const MongoClient = require('mongodb').MongoClient;

// execution start
MongoClient.connect(global.mongo.host, {useNewUrlParser: true}, function (err, mongoClient) {
    console.debug('info', "Mongo Connected successfully");

    const _mongo_db_ref = mongoClient.db(global.mongo.db);
    _mongo_db_ref.collection(global.mongo.collection).createIndex({primaryKey: 1}, {unique: true});

    const _es_ref = new elasticsearch.Client({host: global.elastic.host, log: 'error'});


    const River = require('../index');
    let river = new River(_mongo_db_ref, _es_ref, global.river._collection_index_dict, global.river.options);

    let timeout = 0;
    setTimeout(() => {
        insert(river)
    }, timeout);
    timeout = timeout + 2000;


    setTimeout(() => {
        insertOne(river)
    }, timeout);
    timeout = timeout + 2000;


    setTimeout(() => {
        insertMany(river)
    }, timeout);
    timeout = timeout + 2000;

    setTimeout(() => {
        insertWithOptions(river)
    }, timeout);
    timeout = timeout + 2000;

    setTimeout(() => {
        update(river)
    }, timeout);
    timeout = timeout + 2000;

    setTimeout(() => {
        updateOne(river)
    }, timeout);
    timeout = timeout + 2000;

    setTimeout(() => {
        updateMany(river)
    }, timeout);
    timeout = timeout + 2000;


    setTimeout(() => {
        deleteOne(river)
    }, timeout);
    timeout = timeout + 2000;


    setTimeout(() => {
        deleteMany(river)
    }, timeout);
    timeout = timeout + 2000;


    setTimeout(() => {
        replaceOne(river)
    }, timeout);
    timeout = timeout + 2000;

    setTimeout(() => {
        bulkWrite(river);
    }, timeout);
    timeout = timeout + 2000;


});
let primaryKey = 1;

function insert(river) {
    river.insert(global.mongo.collection, [{'primaryKey': primaryKey++, 'operation': 'insert'}, {
        'primaryKey': primaryKey++,
        'operation': 'insert'
    }], (err, response) => {
        if (err && err.code !== 11000) {
            console.error(err)
        }
        else {
            console.info('Passed : insert')
        }
    });
}

function insertOne(river) {
    river.insertOne(global.mongo.collection, {
        'primaryKey': primaryKey++,
        'operation': 'insertOne'
    }, (err, response) => {
        if (err && err.code !== 11000) {
            console.error(err)
        }
        else {
            console.info('Passed : insertOne')
        }
    });

}

function insertMany(river) {
    river.insertMany(global.mongo.collection, [
        {'primaryKey': primaryKey++, 'operation': 'insertMany'},
        {'primaryKey': primaryKey++, 'operation': 'insertMany'},
        {'primaryKey': primaryKey++, 'operation': 'update'},
        {'primaryKey': primaryKey++, 'operation': 'update', 'preRemark': 'shouldnot update'},
        {'primaryKey': primaryKey++, 'operation': 'updateMany'},
        {'primaryKey': primaryKey++, 'operation': 'updateMany'},
        {'primaryKey': primaryKey++, 'operation': 'updateOne'},
        {'primaryKey': primaryKey++, 'operation': 'updateOne', 'preRemark': 'shouldnot update'},
        {'primaryKey': primaryKey++, 'operation': 'deleteOne'},
        {'primaryKey': primaryKey++, 'operation': 'deleteOne', 'preRemark': ' shouldnot delete'},
        {'primaryKey': primaryKey++, 'operation': 'deleteMany'},
        {'primaryKey': primaryKey++, 'operation': 'deleteMany'},
        {'primaryKey': primaryKey++, 'operation': 'bulkOutDeleteOne'},
        {'primaryKey': primaryKey++, 'operation': 'bulkOutDeleteMany'},
        {'primaryKey': primaryKey++, 'operation': 'bulkOutDeleteMany'},
        {'primaryKey': 100, 'operation': 'insertReplaceOne'}
    ], (err, response) => {
        if (err && err.code !== 11000) {
            console.error(err)
        }
        else {
            console.info('Passed : insertMany')
        }
    });
}

function insertWithOptions(river) {
    river.insert(global.mongo.collection, [{
        'primaryKey': primaryKey++,
        'operation': 'insertWithOptions'
    }, {'primaryKey': primaryKey++, 'operation': 'insertWithOptions'}], {}, (err, response) => {
        if (err && err.code !== 11000) {
            console.error(err)
        }
        else {
            console.info('Passed : insertWithOptions')
        }
    });
}


function update(river) {
    river.update(global.mongo.collection, {operation: 'update'}, {$set: {'postRemark': 'update done'}}, (err, response) => {
        if (err && err.code !== 11000) {
            console.error(err)
        }
        else {
            console.info('Passed : update')
        }
    });
}

function updateMany(river) {
    river.updateMany(global.mongo.collection, {operation: 'updateMany'}, {$set: {'postRemark': 'updateMany done'}}, (err, response) => {
        if (err && err.code !== 11000) {
            console.error(err)
        }
        else {
            console.info('Passed : updateMany')
        }
    });
}


function updateOne(river) {
    river.updateOne(global.mongo.collection, {operation: 'updateOne'}, {$set: {'postRemark': 'updateOne done'}}, (err, response) => {
        if (err && err.code !== 11000) {
            console.error(err)
        }
        else {
            console.info('Passed : updateOne')
        }
    });
}

function deleteOne(river) {
    river.deleteOne(global.mongo.collection, {operation: 'deleteOne'}, {}, (err, response) => {
        if (err && err.code !== 11000) {
            console.error(err)
        }
        else {
            console.info('Passed : deleteOne')
        }
    });
}

function deleteMany(river) {
    river.deleteMany(global.mongo.collection, {operation: 'deleteMany'}, (err, response) => {
        if (err && err.code !== 11000) {
            console.error(err)
        }
        else {
            console.info('Passed : deleteMany')
        }
    });
}


function replaceOne(river) {
    river.replaceOne(global.mongo.collection, {operation: 'insertReplaceOne'}, {
        primaryKey: 100,
        operation: 'insertReplaceOne',
        postRemark: 'replaceOne done'
    }, (err, response) => {
        if (err && err.code !== 11000) {
            console.error(err)
        }
        else {
            console.info('Passed : replaceOne')
        }
    });
}


function bulkWrite(river) {
    river.bulkWrite(global.mongo.collection, [
        {insertOne: {document: {primaryKey: primaryKey++, operation: 'bulkInsertOne'}}},
        {insertOne: {document: {primaryKey: primaryKey++, operation: 'bulkUpdateOne'}}},
        {insertOne: {document: {primaryKey: primaryKey++, operation: 'bulkUpdateMany'}}},
        {insertOne: {document: {primaryKey: primaryKey++, operation: 'bulkUpdateMany'}}},
        {
            insertOne: {
                document: {
                    primaryKey: primaryKey++,
                    operation: 'bulkInDeleteOne',
                    preRemark: 'bulk shouldnot delete'
                }
            }
        },
        {
            insertOne: {
                document: {
                    primaryKey: primaryKey++,
                    operation: 'bulkInDeleteMany',
                    preRemark: 'bulk shouldnot delete'
                }
            }
        },
        {insertOne: {document: {primaryKey: 200, operation: 'bulkReplaceOne'}}},

        {
            updateOne: {
                filter: {operation: 'bulkUpdateOne'},
                update: {$set: {postRemark: 'updateOne done'}},
                upsert: true
            }
        },

        {
            updateMany: {
                filter: {operation: 'bulkUpdateMany'},
                update: {$set: {postRemark: 'updateMany done'}},
                upsert: true
            }
        },

        //delete 'out' ones
        {deleteOne: {filter: {operation: 'bulkOutDeleteOne'}}},
        {deleteMany: {filter: {operation: 'bulkOutDeleteMany'}}},

        //delete 'in' ones
        {deleteOne: {filter: {operation: 'bulkInDeleteOne'}}},
        {deleteMany: {filter: {operation: 'bulkInDeleteMany'}}},

        {
            replaceOne: {
                filter: {operation: 'bulkReplaceOne'},
                replacement: {primaryKey: 200, operation: 'bulkReplaceOne', postRemark: 'replaceOne done'},
                upsert: true
            }
        }
    ], (err, response) => {
        if (err && err.code !== 11000) {
            console.error(err)
        }
        else {
            console.info('Passed : bulkWrite')
        }
    });
}