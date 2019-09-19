var Utility = module.exports = {};

var config = require('./config.json');
var MongoClient = require('mongodb').MongoClient
    , assert = require('assert');
var url = config.url;
var db;

console.log('|| ..... Connecting to mongodb server..... ||');

MongoClient.connect(url, function (err, connDB) {

    if (err) {
        console.log('Unable to connect to the mongoDB server. Error:', err);
        process.exit(1);
    }
    else {
        db = connDB;
        // require('../models/listner');

        console.log('|| ..... Connected to mongodb server..... ||');
    }
});

/**Insert function is use to insert parameters to database.
 * It's a generic function, tablename is use for Tablename in the db.
 * data is array to insert documents in table.
 * if inserted then go callback function else error.
 * **/
Utility.Insert = function (tablename, data, callback) {
    //conn.DbConnection(function (result) {
    //    var db = result;
    var collection = db.collection(tablename);

    collection.insert([data], function (err, result) {
        if (err) {
            console.log(err);
        } else {
            return callback(null, "data Inserted SucessFully");
        }

    });
    //});
};

/**Select function is use to fetch data from database.
 * It's a generic function, tablename is use for Tablename in the db.
 * data is array to provide where condition in table.
 * if success then go callback function else error.
 * Result is use to store all data resulted from select function.
 * **/
Utility.Select = function (tablename, data, callback) {
    //conn.DbConnection(function (result) {
    //    var db = result;
    var collection = db.collection(tablename);

    collection.find(data).toArray(function (err, result) {
        if (err) {
            console.log(err);
        } else {
            return callback(null, result);
        }
    });
    //});
}


Utility.SelectWIthLimitAndIndex = function (tablename, data, pagelimt, callback) {
    //conn.DbConnection(function (result) {
    //    var db = result;
    var collection = db.collection(tablename);

    collection.find(data).sort({ 'order_id': -1 }).limit(11).skip(pagelimt).toArray(function (err, result) {
        if (err) {
            console.log(err);
        } else {
            return callback(null, result);
        }
    });
    //});
}

/*
 * 
 * @param {type} tablename
 * @param {type} data
 * @param {type} callback
 * @returns {undefined} 
 * get lastId
 */
Utility.getLastIdInCollection = function (tablename, filledName, callback) {
    //conn.DbConnection(function (result) {
    //    var db = result;
    var collection = db.collection(tablename);
    var filed = filledName;
    collection.find({}, { 'order_id': 1 }).sort({ 'order_id': -1 }).limit(1).toArray(function (err, result) {
        if (err) {
            console.log(err);
        } else {
            return callback(null, result);
        }
    });
    //});
}



Utility.SelectWithProjection = function (tablename, data, projection, callback) {
    var collection = db.collection(tablename);

    collection.find(data, projection).toArray(function (err, result) {
        if (err) {
            callback(err);
        } else {
            return callback(null, result);
        }
    });
}



Utility.Count = function (tablename, condition, callback) {

    var collection = db.collection(tablename);
    collection.count(condition, function (err, count) {
        if (err) {
            callback(err);
        } else {
            return callback(null, count);
        }
    });
};


/**Select function is use to fetch data from database.
 * It's a generic function, tablename is use for Tablename in the db.
 * data is array to provide where condition in table.
 * if success then go callback function else error.
 * Result is use to store all data resulted from select function.
 * **/
Utility.SelectWithLimit = function (tablename, data, limit, skipCount, callback) {
    //conn.DbConnection(function (result) {
    //    var db = result;
    var collection = db.collection(tablename);

    collection.find(data).limit(limit).skip(skipCount).toArray(function (err, result) {
        if (err) {
            console.log(err);
        } else {
            return callback(null, result);
        }
    });
    //});
}


/**SelectOne function is use to fetch data from database but fetch only one row from table.
 * It's a generic function, tablename is use for Tablename in the db.
 * data is array to provide where condition in table.
 * if success then go callback function else error.
 * Result is use to store all data resulted from select function.
 * **/
Utility.SelectOne = function (tablename, data, callback) {
    //conn.DbConnection(function (result) {
    //    var db = result;
    var collection = db.collection(tablename);

    collection.findOne(data, (function (err, result) {
        if (err) {
            console.log(err);
        } else {
            return callback(null, result);
        }
    }));
    //});
}


Utility.SelectOR = function (tablename, data1, data2, callback) {
    //conn.DbConnection(function (result) {
    //    var db = result;
    var collection = db.collection(tablename);
    collection.findOne({ $or: [data1, data2] }, (function (err, result) {
        if (err) {
            console.log(err);
        } else {
            //console.log(result);
            return callback(null, result);
        }

    }));
    //});
}


/**Update function is use to Update parameters to database.
 * It's a generic function, tablename is use for Tablename in the db.
 * data is array to Update documents in table.
 * if Updated then go callback function else error.
 * **/
Utility.Update = function (tablename, condition, data, callback) {
    //conn.DbConnection(function (result) {
    //    var db = result;
    var collection = db.collection(tablename);
    collection.update(condition, { $set: data }, (function (err, result) {
        if (err) {
            console.log(err);
        } else {
            //console.log(result);
            return callback(null, result);
        }

    }));
    //});
}


/**Update function is use to Update parameters to database.
 * It's a generic function, tablename is use for Tablename in the db.
 * data is array to Update documents in table.
 * if Updated then go callback function else error.
 * **/
Utility.UpdatePush = function (tablename, condition, data, callback) {
    var collection = db.collection(tablename);
    collection.update(condition, { $push: data }, (function (err, result) {
        return callback(err, result);
    }));
}

/*
 * 
 * @param {type} tablename
 * @param {type} condition
 * @param {type} data
 * @param {type} callback
 * @returns {undefined}
 *  this once will update and push  spacific data to inner collection
 */

Utility.UpdateAndPush = function (tablename, condition, push, updationdata, callback) {
    var collection = db.collection(tablename);
    collection.update(condition, { $push: push, $set: updationdata }, (function (err, result) {
        if (err) {
            console.log(err);
        } else {
            return callback(null, result);
        }
    }));
}


/**Update function is use to Update parameters to database.
 * It's a generic function, tablename is use for Tablename in the db.
 * data is array to Update documents in table.
 * if Updated then go callback function else error.
 * **/
Utility.UpdatePull = function (tablename, condition, data, callback) {
    //conn.DbConnection(function (result) {
    //    var db = result;
    var collection = db.collection(tablename);
    collection.update(condition, { $pull: data }, (function (err, result) {
        if (err) {
            console.log(err);
        } else {
            return callback(null, result);
        }

    }));
    //});
}


Utility.UpdateField = function (tablename, condition, data, callback) {
    //conn.DbConnection(function (result) {
    //    var db = result;
    var collection = db.collection(tablename);
    collection.update(condition, { $pull: data }, (function (err, result) {
        if (err) {
            console.log(err);
        } else {
            //console.log(result);
            return callback(null, result);
        }

    }));
    //});

};


Utility.UpdateArray = function (tablename, condition, data, callback) {
    var collection = db.collection(tablename);
    collection.update(condition, { $addToSet: data }, (function (err, result) {
        if (err) {
            console.log(err);
        } else {
            return callback(null, result);
        }
    }));
};


/**This function is use to delete parameters to database.
 * It's a generic function, tablename is use for Tablename in the db.
 * data is array to give the condition.
 * if deleted then go callback function else error.
 * **/
Utility.Delete = function (tablename, condition, callback) {
    //conn.DbConnection(function (result) {
    //    var db = result;
    var collection = db.collection(tablename);
    collection.remove(condition, function (err, numberOfRemovedDocs) {
        if (err) {
            console.log(err);
        } else {
            //console.log(numberOfRemovedDocs);
            return callback(null, numberOfRemovedDocs);
        }
        //assert.equal(null, err);
        //assert.equal(1, numberOfRemovedDocs);
        //db.close();
    });
    //});
};


Utility.GeoNear = function (tablename, condition, location, callback) {
    db.command({
        geoNear: tablename,
        near: {
            longitude: parseFloat(location.lng),
            latitude: parseFloat(location.lat)
        },
        spherical: true,
        maxDistance: 500000 / 6378137,
        distanceMultiplier: 6378137,
        query: condition
    }, function (err, geoResult) {
        if (err) {
            console.log(err);
        } else {
            return callback(null, geoResult);
        }
    });
};


/*Function to find the documents in a given radious
 * name the distance field and sort the result  */
Utility.GeoNearAggregate = function (tablename, condition, location, callback) {
    var collection = db.collection(tablename);
    collection.aggregate([{
        $geoNear: {
            near: {
                longitude: parseFloat(location.lng),
                latitude: parseFloat(location.lat)
            },
            spherical: true,
            distanceField: "distance",
            maxDistance: 10000 / 6378137,
            distanceMultiplier: 6378137,
            query: condition
        }
    }, {
        "$sort": { "distance": 1 }
    }], function (geoErr, geoResult) {
        if (geoErr) {
            console.log(geoErr);
        } else {
            return callback(null, geoResult);
        }
    });
};


Utility.count = function (tablename, data, callback) {
    var collection = db.collection(tablename);

    collection.count(data, function (err, numrow) {
        if (err) {
            console.log(err);
        } else {
            return callback(null, numrow);
        }
    });
}


Utility.UpdateUnset = function (tablename, condition, data, callback) {
    var collection = db.collection(tablename);
    collection.update(condition, { $unset: data }, { multi: true }, (function (err, result) {
        if (err) {
            console.log(err);
        } else {
            return callback(null, result);
        }
    }));
};

Utility.aggregate_ = function (tablename, condition, callback) {
    var collection = db.collection(tablename);
    collection.aggregate(condition, (function (err, result) {
        if (err) {
            console.log(err);
        } else {
            return callback(null, result);
        }
    }));
};



Utility.Cmd = function (tablename, condition, threshHold, callback) {
    var query = {};
    if (tablename == 'masters')
        //        query = {status: 3, inBooking: 1};
        query = { status: { "$in": [3] }, "carId": { "$ne": 0 } };
    var geonearObj = {
        geoNear: tablename,
        near: {
            longitude: parseFloat(condition.long),
            latitude: parseFloat(condition.lat)
        },
        spherical: true,
        // maxDistance: 20000 / 6378137,
        // distanceMultiplier: 6378137,

        maxDistance: 20 / 6378.1,
        distanceMultiplier: 6378.1,
        query: query
    };
    if (tablename == 'vehicleTypes')
        geonearObj.maxDistance = 800000 / 6378137;
    //    if (!(threshHold == 0))
    //        geonearObj.limit = parseInt(threshHold);

    db.command(geonearObj, function (err, result) {
        // if (err) {
        //     console.log(err);
        // } else {
        //     console.log(result);
        //     return callback(null, result);
        // }
        return callback(null, result);
    });

};


Utility.isvalid = function (id, callback) {
    //    var mongodb = require("mongodb"),
    //            objectid = mongodb.BSONPure.ObjectID;
    var checkForHexRegExp = new RegExp("^[0-9a-fA-F]{24}$")

    callback(checkForHexRegExp.test(id));
}

/* +_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+ BY CHETHAN +_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+ */

/**
 * Method to get documents from specified collection
 * @param {*} collection - Name of the collection
 * @param {*} queryObj - queryObj contains all the query parameters
 * @param {*} cb - callback
 * @param {} q - query/condition to filter the documents
 * @param {} p - projection i.e, fields to include/exclude
 * @param {} s - sort, docs to sort on filed
 * @param {} skip - the number of documents to skip
 * @param {} limit - the number of documents to get
 * @values - q = {} - to get all the docs from the collection
 * @values - p = {} - to get all the fields of docs from the collection
 * @values - p = { fieldName: 0 } - to exclude the specified field from the doc
 * @values - p = { fieldName: 1 } - to include the specified filed from the doc
 * @values - s = {} - to get docs with default sorting
 * @values - s = { fieldName: 1 } - ascending sort on specified fields
 * @values - s = { fieldName: -1 } - descending sort on specified fields
 * @default queryObj = {
                  q: {},
                  p: {},
                  s: {},
                  skip: 0,
                  limit: 0
                }
 */
Utility.SELECT = (collection, queryObj, cb) => {

    db.collection(collection)
        .find(queryObj.q || {}, queryObj.p || {})
        .sort(queryObj.s || {})
        .skip(queryObj.skip || 0)
        .limit(queryObj.limit || 0)
        .toArray((err, docs) => { return cb(err, docs); });
};

/**
 * Method to insert documents to the specifed collection
 * @param {*} collection - Name of the collection
 * @param {*} data - JSON object to insert into the collection
 * @param {*} cb - callback
 * @author Chethan
 * REF: https://docs.mongodb.com/manual/reference/method/db.collection.insert/
 */
Utility.INSERT = (collection, data, cb) => {
    db.collection(collection)
        .insert(data, (err, result) => { return cb(err, result); });
};

/**
 * Method to update the documents in a specified collectino
 * @param {*} collection - Name of the collection
 * @param {*} queryObj - the queryObj contains all the query parameters
 * @param queryObj = {
                  query: {}, -> the selection criteria
                  data: {}, -> data to update
                  options: {} -> options like multi, upsert
                }
 * REF: https://docs.mongodb.com/manual/reference/method/db.collection.update/
 */
Utility.UPDATE = (collection, queryObj, cb) => {

    db.collection(collection)
        .update(queryObj.query, queryObj.data, queryObj.options || {}, (err, result) => {
            return cb(err, result);
        });
};

/**
 * Method to Updates a single document based on the filter and sort criteria.
 * @param {*} collection - Name of the collection
 * @param {*} queryObj - the queryObj contains all the query parameters
 * @param queryObj = {
                  query: {}, -> the selection criteria
                  data: {}, -> data to update
                  options: {} -> options like projection, sort, maxTimeMS, upsert, returnNewDocument
                }
 * REF: https://docs.mongodb.com/v3.2/reference/method/db.collection.findOneAndUpdate/
 */
Utility.FINDONEANDUPDATE = (collection, queryObj, cb) => {

    db.collection(collection)
        .findOneAndUpdate(queryObj.query, queryObj.data, queryObj.options || {}, (err, result) => {
            return cb(err, result);
        });
};

/**
 * Method to perform aggregation on documents
 * @param {*} collection - Name of the collection
 * @param {*} queryObj - the queryObj contains all the query parameters
 * REF: https://docs.mongodb.com/manual/reference/operator/aggregation/
 */
Utility.AGGREGATE = (collection, queryObj, cb) => {

    db.collection(collection)
        .aggregate(queryObj, (err, result) => {
            return cb(err, result);
        });
};

/* +_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+ END +_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+ */

Utility.UpdatePushOrCreate = function (tablename, condition, data, callback) {
    var collection = db.collection(tablename);
    collection.update(condition, { $push: data }, { upsert: true }, (function (err, result) {
        // if (err) {
        //     console.log(err);
        // } else {
        //     return callback(null, result);
        // }

        return callback(err, result);;
    }));
}

Utility.CMD = (tablename, query, condition, callback) => {

    var geonearObj = {
        geoNear: tablename,
        near: {
            longitude: parseFloat(condition.long),
            latitude: parseFloat(condition.lat)
        },

        spherical: true,

        // maxDistance: 20000 / 6378137,
        // distanceMultiplier: 6378137,

        maxDistance: 20 / 6378.1,
        distanceMultiplier: 6378.1,
        query: query
    };

    db.command(geonearObj, (err, result) => {
        return callback(null, result);
    });

};

Utility.SelectWIthLimitSortSkip = function (tablename, data, sortBy, limit, skipCount, callback) {
    //conn.DbConnection(function (result) {
    //    var db = result;
    var collection = db.collection(tablename);

    collection.find(data).sort(sortBy).limit(limit).skip(skipCount).toArray(function (err, result) {
        if (err) {
            console.log(err);
        } else {
            return callback(null, result);
        }
    });
    //});
}