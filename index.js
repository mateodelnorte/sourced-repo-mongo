var Entity = require('sourced').Entity;
var log = require('debug')('sourced-repo-mongo');
var mongo = require('./mongo');
var Promise = require('bluebird');
var util = require('util');
var _ = require('lodash');

module.exports.config = {
  mongoUrl: 'mongodb://127.0.0.1:27017'
};

function Repository (entityType, indices) {
  indices = _.union(indices, ['id']);
  var self = this;
  self.entityType = entityType;
  self.indices = indices;
  self.mongo = mongo;
  self.initialized = new Promise(function (resolve, reject) {
    self.mongo.once('connected', function (db) {
      var snapshotCollectionName = util.format('%s.snapshots', entityType.name);
      var snapshots = db.collection(snapshotCollectionName);
      self.snapshots = snapshots;
      var eventCollectionName = util.format('%s.events', entityType.name);
      var events = db.collection(eventCollectionName);
      self.events = events;
      self.indices.forEach(function (index) {
        snapshots.ensureIndex(index, reject);
        events.ensureIndex(index, reject);
      });
      log('initialized %s entity store', self.entityType.name);
      resolve();
    });
    self.mongo.once('error', reject);
  });
  log('connecting to %s entity store', this.entityType.name);
  self.mongo.connect(module.exports.config.mongoUrl);  
}

Repository.prototype.get = function get (id, cb) {
  var self = this;
  log('getting %s for id %s', this.entityType.name, id);
  this.initialized.done(function () {
    self.snapshots
      .find({ id: id })
      .sort({ version: -1 })
      .limit(-1)
      .toArray(function (err, docs) {
        if (err) return cb(err);
        var snapshot = docs[0];
        if (snapshot === undefined) {
          return self.empty(id, cb);
        } else {
          self.events.find({ id: id, version: { $gt: snapshot.version } })
            .sort({ version: 1 })
            .toArray(function (err, events) {
              if (err) return cb(err);
              delete snapshot._id;
              return self.deserialize(snapshot, events, cb);
            });
        }
    });
  });
};

Repository.prototype.commit = function commit (entity, cb) {
  var self = this;
  log('committing %s for id %s', this.entityType.name, entity.id);
  this.initialized.done(function () {
    // save snapshots before saving events
    new Promise(function (resolve, reject) {
      if (entity.version >= entity.snapshotVersion + 10) {
        var snapshot = entity.snapshot();  
        self.snapshots.insert(snapshot, function (err) {
          if (err) return reject(err);
          log('committed %s.snapshot for id %s %j', self.entityType.name, entity.id, snapshot);
          resolve(entity);
        })
      } else {
        resolve(entity);
      }  
    }).done(function (entity) {
      // when finished, save events
      var events = entity.newEvents;
      events.forEach(function (event) {
        self.indices.forEach(function (index) {
          event[index] = entity[index];
        });
      });
      self.events.insert(events, function (err) {
        if (err) return cb(err);
        log('committed %s.events for id %s', self.entityType.name, entity.id);
        return cb();
      });
    });
  });
};

Repository.prototype.empty = function empty (id, cb) {
  log('creating empty %s for id %s', this.entityType.name, id);
  var entity = new this.entityType();
  entity.id = id;
  return cb(null, entity);
};

Repository.prototype.deserialize = function deserialize (snapshot, events, cb) {
  log('deserializing %s snapshot', this.entityType.name);
  return cb(null, new this.entityType(snapshot, events));
};

module.exports.Repository = Repository;