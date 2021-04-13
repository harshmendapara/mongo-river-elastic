# mongo-river-elastic
https://www.npmjs.com/package/mongo-river-elastic
The mongo-river-elastic project provides a nodejs wrapper over [mongodb nodejs](http://mongodb.github.io/node-mongodb-native/3.2/) , which synchronize data from your mongodb to elasticsearch.
It is tested and works best with following:
* Mongodb v4.0+
* Mongodb Nodejs Driver v3.2
* elasticsearch v6.0+ , v7.0+

Following Mongodb functions are supported:
* [insert](http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#insert)
* [insertMany](http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#insertMany)
* [insertOne](http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#insertOne)
* [update](http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#update)
* [updateMany](http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#updateMany)
* [updateOne](http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#updateOne)
* [deleteOne](http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#deleteOne)
* [deleteMany](http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#deleteMany)
* [replaceOne](http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#replaceOne)
* [bulkWrite](http://mongodb.github.io/node-mongodb-native/3.1/api/Collection.html#bulkWrite)

## Getting Started

#### Before you use the library, following points are crucial
###### Collections
* It will not auto-create elasticsearch index, all the indexes has to be created by you.
* All objects in a collection is appended with an extra key called ```river``` during insert/update and it is indexed. This key stores timestamp and is used for syncing with elasticsearch
* To ensure consistency when elasticsearch is unreachable/down/busy, a mongodb collection ```river_backlog``` is created, which contains a single document having meta-information about objects yet to be synced. It will be deleted once elasticsearch is reachable and sync is complete.
###### Primary Key
* Elasticsearch expects a primary key for each insert. You have to specify your ```primaryKeyField``` in mongodb document which will be used by the library.
* If you dont have any primary key field, use mongodb bson id ```_id```
* Primary key in elasticsearch is always ```string```.

Elasticsearch v7.0
* This elasticsearch version has ```_type``` field depricated.
* If your elasticsearch is v7.0+, library will ignore the type value provided in ```_collection_index_dict```

### Prerequisites

Dependent packages

```
npm install mongodb
npm install elasticsearch
```

### Installing

```
npm install mongo-river-elastic
npm install mongo-river-elastic --save
```

### Initialize

```javascript
const River = require('mongo-river-elastic')
let river = new River(_mongo_db_ref, _es_ref, _collection_index_dict, options)
```
Parameters:
* **_mongo_db_ref** &nbsp; --  *Object*

    * mongodb database connection reference object
* **_es_ref** &nbsp; -- *Object*
    * elasticsearch connection reference object
* **_collection_index_dict** &nbsp; --  *Object*
    * Object dictionary that contains mapping between mongodb collections and elasticsearch index/type
    * Example
        ```javascript
        _collection_index_dict =
        { 'collection1': {index: 'index1', type: 'type1', primaryKeyField: 'primaryKeyField1'},
          'collection2': {index: 'index2', type: 'type2', primaryKeyField: 'primaryKeyField2'}}
        ```
        where,
        * ```collection1```: mongodb collection name
        * ```index1```: elasticsearch index name
        * ```type```: elasticsearch index mapping name. Set to ```null``` if not applicable.
        * ```primaryKeyField1```: mongodb collection object ```key``` to be used as primary key in elasticsearch


* **options** &nbsp; -- *Object*
    * loglevel &nbsp; &nbsp;  *String*
        * Set log level for River logging (elasticsearch log)
        * info | error | debug
    * retryCount  &nbsp; &nbsp;  *number*
        * No. of retries if elasticsearch write fails

### Examples
##### Setup River Object

```javascript
const elasticsearch = require('elasticsearch');
const MongoClient = require('mongodb').MongoClient;
const River = require('mongo-river-elastic');

// Connect Mongodb
MongoClient.connect('localhost:27017', function (err, mongoClient) {
    const _mongo_db_ref = mongoClient.db('test_database');

    // connect elasticsearch
    const _es_ref = new elasticsearch.Client({host: 'localhost:9200'});

    // init River
    const ._collection_index_dict = {
    'collection1': {index: 'index1', type: 'type1', primaryKeyField: '_id'},
    'collection2': {index: 'index2', type: 'type2', primaryKeyField: 'uuid'}
    };
    const options= {logLevel: 'debug', retryCount: 3};
    let river = new River(_mongo_db_ref, _es_ref, _collection_index_dict, options);
```

##### CRUDS
* River CRUDS accepts options exactly similar to mongodb CRUDS
* Only difference is, the first argument to any CRUD is name the of **collection**
* **mongodb_options** is optional, similar to mongodb
* INSERT
```javascript
    const mongodb_options = {}
    //insert, insertOne, insertMany
    river.insertOne('collection1', {'_id': '1', 'key': 'value'}, mongodb_options, (err, response) => {
        ...
    });

    river.insertMany('collection2', [
        {'uuid': '1', 'key': 'value1'},
        {'uuid': '2', 'key': 'value2'},
        {'uuid': '3', 'key': 'value3'}], (err, response) => {
        ...
    });
```
* UPDATE
```javascript
 //update, updateOne, updateMany
    river.updateOne('collection1', {'key': 'value'}, {$set: {'anotherKey': 'anotherValue'}}, mongodb_options, (err, response) => {
        ...
    });
```
* DELETE
```javascript
    //deleteOne, deleteMany
    river.deleteOne('collection1', {'key': 'value'},mongodb_options, (err, response) => {
        ...
    });
```
* REPLACE
```javascript
    river.replaceOne('collection1', {'key': 'value'}, {
        '_id': 10,
        'key': 'value',
        'anotherKey': 'another value'
    }, (err, response) => {

    });
```
* BULK WRITE
```javascript
    river.bulkWrite('collection1',
    [
        {insertOne: {document: {'_id': '1', 'key': 'value'}}},
         {updateOne: {filter: {'key': 'value'},update: {$set: {'anotherKey': 'anotherValue'}},upsert: true}},
         {deleteOne: {filter: {'key': 'value'}}},
    ],mongodb_options, (err, response) => {

    });
```



## Contributing

1.  Fork it!
2.  Create your feature branch: `git checkout -b my-new-feature`
3.  Commit your changes: `git commit -am 'Add some feature'`
4.  Push to the branch: `git push origin my-new-feature`
5.  Submit a pull request :D

## Author

**mongo-river-elastic** © [Harsh Mendapara](https://github.com/harshmendapara/), Released under the [MIT](./LICENSE) License.<br>
Authored and maintained by Harsh Mendapara with help from contributors ([list](https://github.com/Harsh02051998/countries-states-cities-countries-states-cities-database-all-formate/graphs/contributors)).

> GitHub [@Harsh Mendapara](https://github.com/harshmendapara)

> Gmail [@Harsh Mendapara](mendaparaharsh02@gmail.com)

> Linkedin [@Harsh Mendapara](https://www.linkedin.com/in/harsh-mendapara-44883a165/)

> Facebook [@Harsh Mendapara](https://www.facebook.com/mhb0205)
> 
Let's fly together :)


