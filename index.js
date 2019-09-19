const http = require('http');
const express = require('express');
const app = express();

//--------------------------------------------------------------

var Mongo = require('./mongo_module');
ObjectID = require('mongodb').ObjectID;

//--------------------------------------------------------------



app.post('/select', (req, res) => {

    var condition = {
        _id: new ObjectID("592febcae579bd16625aa664")
    };
    var data = {};
    Mongo.Select(
            "collectionName",
            condition,
            function (err, colData) {
                if (err) {
                    data = {err: 1, message: err};
                    console.log(err);
                } else {
                    data = {err: 0, data: colData};
                }
                res.writeHead(200, {"Content-Type": "application/json"});
                res.end(data);
            });
});


app.post('/SelectOne', (req, res) => {

    var collectionName = "test";
    var condition = {
        _id: new ObjectID("592febcae579bd16625aa664")
    };
    var data = {};

    Mongo.SelectOne(collectionName, condition, function (err, resData) {
        if (err)
            data = {err: 1, message: err};

        else if (resData === null)
            data = {err: 1, message: null};
        else
            data = {err: 0, message: resData};

        res.writeHead(200, {"Content-Type": "application/json"});
        res.end(data);
    });
});

//--------------------------------------------------------------

http.createServer(app).listen(1234, () => {
    console.log('server listening on port 1234');
});