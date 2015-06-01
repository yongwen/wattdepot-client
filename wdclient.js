/**
 * Created by yxu on 5/28/2015.
 */
var MONGO_URL = 'mongodb://127.0.0.1:3001/meteor';
var MEASUREMENT_COUNT = 10000000;
var SENSOR_NAME = "Sensor Client";
var START_VALUE = 100;

var MongoClient = require('mongodb').MongoClient;
var ObjectID = require('mongodb').ObjectID;

var closeConnection = function(db, measurements) {
    // Let's close the db
    measurements.count(function(err, count) {
        console.log("count=" + count);
        if (count >= MEASUREMENT_COUNT) {
            db.close();
            process.exit();
        }
    });
};

var insertMeasurementsEverySecond = function(db, sensorId, callback) {
    //insert to measurements
    var measurements = db.collection('measurements');
    var value = START_VALUE;
    setInterval (function () {
            value = value + Math.floor(Math.random() * 20 - 10);
            if (value < 0) {
                value = START_VALUE;
            }
            measurements.insert({sensorId: sensorId, value: value, timeStamp: new Date()},
                function (err, result) {
                    if (err) console.log("measurement error"+ err);
                    // update measurementcount in sensor
                    var sensors = db.collection('sensors');
                    sensors.updateOne({_id: new ObjectID(sensorId)}, {$inc: {measurementCount: 1}});
                    callback(db, measurements);
                });
        }, 1000);
};

var insertMeasurements = function(db, sensorId, callback) {
    //insert to measurements
    var measurements = db.collection('measurements');
    var value = START_VALUE;
    measurements.findOne({},{sort: {timeStamp: 1}}, function(err, result){
        var timeStampDate;
        if (err || result == null) {
            timeStampDate = new Date();
        }
        else {
            timeStampDate = result.timeStamp;
        }
        console.log("starting from "+timeStampDate);
        var timeStamp = timeStampDate.valueOf();
        setInterval (function () {
            value = value + Math.floor(Math.random() * 20 - 10);
            if (value < 0) {
                value = START_VALUE;
            };
            timeStamp = timeStamp - 1000;
            measurements.insert({sensorId: sensorId, value: value, timeStamp: new Date(timeStamp)},
                function (err, result) {
                    if (err) console.log("measurement error"+ err);
                    // update measurementcount in sensor
                    var sensors = db.collection('sensors');
                    sensors.updateOne({_id: new ObjectID(sensorId)}, {$inc: {measurementCount: 1}});
                    callback(db, measurements);
                });
        }, 1);
    });
};

var insertSensor = function(db, callback) {

    var sensors = db.collection('sensors');
    sensors.insert({name: SENSOR_NAME, location: "my loc", measurementCount: 0},
        function (err, result) {
            if (err) console.log("sensor error"+ err);
            callback(result.ops[0]._id.toHexString());
        });
};

MongoClient.connect(MONGO_URL, function(err, db) {
    if (err) throw err;

    // find
    db.collection("sensors").find({name: SENSOR_NAME}).toArray(function (err, results) {
        if (results.length > 0) {
            insertMeasurements(db, results[0]._id.toHexString(), closeConnection);
        }
        else {
            // insert to sensors
            insertSensor(db, function (sensorId) {
                insertMeasurements(db, sensorId, closeConnection);
            });
        }
    });
});