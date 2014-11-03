var EventEmitter = require('events').EventEmitter;
var log = require('debug')('sourced-repo-mongo');
var client = require('mongodb').MongoClient;
var util = require('util');

function Mongo () {
  this.db = null;
  EventEmitter.call(this);
}

util.inherits(Mongo, EventEmitter);

Mongo.prototype.connect = function connect (mongoUrl) {
  var self = this;
  client.connect(mongoUrl, function (err, db) {
    if (err) {
      log('✗ MongoDB Connection Error. Please make sure MongoDB is running: ', err);
      self.emit('error', err);
    }
    self.db = db;
    log('initialized connection to mongo at %s', mongoUrl);
    self.emit('connected', db);
  });
};

module.exports = new Mongo();