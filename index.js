var Entity = require('sourced').Entity;
var EventEmitter = require('events').EventEmitter;
var log = require('debug')('sourced-repo-mongo');
var mongo = require('./mongo');
var Promise = require('bluebird');
var util = require('util');
var _ = require('lodash');

function Repository (entityType, options) {
  options = options || {};
  EventEmitter.call(this);
  if ( ! mongo.db) {
    throw new Error('mongo has not been initialized. you must call require(\'sourced-repo-mongo/mongo\').connect(config.MONGO_URL); before instantiating a Repository');
  }
  var indices = _.union(options.indices, ['id', 'version']);
  var self = this;
  var db = mongo.db;
  self.entityType = entityType;
  self.indices = indices;
  self.snapshotFrequency = options.snapshotFrequency || 10;
  self.initialized = new Promise(function (resolve, reject) {
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
    events.ensureIndex({ id: 1, version: 1 }, reject);
    snapshots.ensureIndex({ id: 1, version: 1 }, reject);
    snapshots.ensureIndex('snapshotVersion', reject);
    log('initialized %s entity store', self.entityType.name);
    resolve();
    self.emit('ready');
  });
  log('connecting to %s entity store', this.entityType.name); 
}

util.inherits(Repository, EventEmitter);

Repository.prototype.commit = function commit (entity, options, cb) {
  if (typeof options === 'function') {
    cb = options;
    options = {};
  }
  var self = this;
  log('committing %s for id %s', this.entityType.name, entity.id);
  this.initialized.done(function () {
    self._commitEvents(entity).then(function _afterCommitEvents () {
      self._commitSnapshots(entity, options).then(function _afterCommitSnapshots () {
        self._emitEvents(entity).then(function _afterEmitEvents () {
          return cb();
        });
      }).catch(cb);
    }).catch(cb);
  });
};

Repository.prototype.commitAll = function commit (entities, options, cb) {
  if (typeof options === 'function') {
    cb = options;
    options = {};
  }
  var self = this;
  log('committing %s for id %j', this.entityType.name, _.pluck(entities, 'id'));
  this.initialized.done(function () {
    self._commitAllEvents(entities).then(function _afterCommitEvents () {
      self._commitAllSnapshots(entities, options).then(function _afterCommitSnapshots () {
        var promises = [];
        entities.forEach(function (entity) {
          promises.push(self._emitEvents(entity));
        });
        Promise.all(promises).then(function _afterEmitAllEvents () {
          return cb();
        });
      }).catch(cb);
    }).catch(cb);
  });
};

Repository.prototype.get = function get (id, cb) {
  var self = this;
  log('getting %s for id %s', this.entityType.name, id);
  this.initialized.done(function () {
    self.snapshots
      .find({ id: id })
      .sort({ version: -1 })
      .limit(-1)
      .toArray(function (err, snapshots) {
        if (err) return cb(err);
        var snapshot = snapshots[0];
        var criteria = (snapshot) ? { id: id, version: { $gt: snapshot.version } } : { id: id };
        self.events.find(criteria)
          .sort({ version: 1 })
          .toArray(function (err, events) {
            if (err) return cb(err);
            if (snapshot) delete snapshot._id;
            var entity = self._deserialize(id, snapshot, events);
            return cb(null, entity);
          });
    });
  });
};

Repository.prototype.getAll = function getAll (ids, cb) {
  var self = this;
  log('getting %ss for ids %j', this.entityType.name, ids);
  this.initialized.done(function () {
    self._getAllSnapshots(ids)
      .catch(cb)
      .done(function _afterGetAllSnapshots (snapshots) {
        self._getAllEvents(ids, snapshots)
          .catch(cb)
          .done(function (entities) {
            cb(null, entities);
          });
    });
  });
};

Repository.prototype._commitEvents = function _commitEvents (entity) {
  var self = this;
  return new Promise(function (resolve, reject) {
    if (entity.newEvents.length === 0) return resolve();
    var events = entity.newEvents;
    events.forEach(function (event) {
      if (event && event._id) delete event._id; // mongo will blow up if we try to insert multiple _id keys
      self.indices.forEach(function (index) {
        event[index] = entity[index];
      });
    });
    self.events.insert(events, function (err) {
      if (err) return reject(err);
      log('committed %s.events for id %s', self.entityType.name, entity.id);
      entity.newEvents = [];
      return resolve();
    });
  });
};

Repository.prototype._commitAllEvents = function _commitEvents (entities) {
  var self = this;
  return new Promise(function (resolve, reject) {
    var events = [];
    entities.forEach(function (entity) {
      if (entity.newEvents.length === 0) return;
      var evnts = entity.newEvents;
      evnts.forEach(function _applyIndices (event) {
        if (event && event._id) delete event._id; // mongo will blow up if we try to insert multiple _id keys
        self.indices.forEach(function (index) {
          event[index] = entity[index];
        });
      });
      Array.prototype.unshift.apply(events, evnts);
    });
    if (events.length === 0) return resolve();
    self.events.insert(events, function (err) {
      if (err) return reject(err);
      log('committed %s.events for ids %j', self.entityType.name, _.pluck(entities, 'id'));
      entities.forEach(function (entity) {
        entity.newEvents = [];
      });
      return resolve();
    });
  });
};

Repository.prototype._commitSnapshots = function _commitSnapshots (entity, options) {
  var self = this;
  return new Promise(function (resolve, reject) {
    if (options.forceSnapshot || entity.version >= entity.snapshotVersion + self.snapshotFrequency) {
      var snapshot = entity.snapshot();  
      if (snapshot && snapshot._id) delete snapshot._id; // mongo will blow up if we try to insert multiple _id keys
      self.snapshots.insert(snapshot, function (err) {
        if (err) return reject(err);
        log('committed %s.snapshot for id %s %j', self.entityType.name, entity.id, snapshot);
        resolve(entity);
      });
    } else {
      resolve(entity);
    }  
  });
};

Repository.prototype._commitAllSnapshots = function _commitAllSnapshots (entities, options) {
  var self = this;
  return new Promise(function (resolve, reject) {
    var snapshots = [];
    entities.forEach(function (entity) {
      if (options.forceSnapshot || entity.version >= entity.snapshotVersion + self.snapshotFrequency) {
        var snapshot = entity.snapshot();  
        if (snapshot) {
          if (snapshot._id) delete snapshot._id; // mongo will blow up if we try to insert multiple _id keys)
          snapshots.push(snapshot);
        }
      }
    });
    if (snapshots.length === 0) return resolve();
    self.snapshots.insert(snapshots, function (err) {
      if (err) return reject(err);
      log('committed %s.snapshot for ids %s %j', self.entityType.name, _.pluck(entities, 'id'), snapshots);
      resolve(entities);
    });
  });
};

Repository.prototype._deserialize = function _deserialize (id, snapshot, events) {
  log('deserializing %s entity ', this.entityType.name);
  var entity = new this.entityType(snapshot, events);
  entity.id = id;
  return entity;
};

Repository.prototype._emitEvents = function _emitEvents (entity) {
  var self = this;
  return new Promise(function (resolve, reject) {
    var eventsToEmit = entity.eventsToEmit;
    entity.eventsToEmit = [];
    eventsToEmit.forEach(function (eventToEmit) {
      var args = Array.prototype.slice.call(eventToEmit);
      self.entityType.prototype.emit.apply(entity, args);
    });
    log('emitted local events for id %s', entity.id);
    return resolve();
  });
};

Repository.prototype._getAllSnapshots = function _getAllSnapshots (ids) {
  var self = this;
  return new Promise(function (resolve, reject) {
    var match = { $match: { id: { $in: ids } } };
    var group = { $group: { _id: '$id', snapshotVersion: { $last: '$snapshotVersion' } } };    
    self.snapshots.aggregate([match, group], function (err, idVersionPairs) {
      if (err) return reject(err);
      var criteria = {};
      if (idVersionPairs.length === 0) {
        return resolve([]);
      } else if (idVersionPairs.length === 1) {
        criteria = { id: idVersionPairs[0]._id, snapshotVersion: idVersionPairs[0].snapshotVersion };
      } else {
        criteria.$or = [];
        idVersionPairs.forEach(function (pair) {
          var cri = { id: pair._id, snapshotVersion: pair.snapshotVersion };
          criteria.$or.push(cri);
        });
      }
      self.snapshots
        .find(criteria)
        .toArray(function (err, snapshots) {
          if (err) reject(err);
          resolve(snapshots);
        });
    });
  });
};

Repository.prototype._getAllEvents = function _getAllEvents (ids, snapshots) {
  var self = this;
  return new Promise(function (resolve, reject) {
    var criteria = { $or: [] };
    ids.forEach(function (id) {
      var snapshot;
      if ( ! (snapshot = _.find(snapshots, function (snapshot) {
        return id === snapshot.id;
      }))) {
        criteria.$or.push({ id: id });
      } else {
        criteria.$or.push({ id: snapshot.id, version: { $gt: snapshot.snapshotVersion } });
      }
    });
    self.events.find(criteria)
      .sort({ id: 1, version: 1 })
      .toArray(function (err, events) {
        if (err) return reject(err);
        var results = [];
        ids.forEach(function (id) {
          var snapshot = _.find(snapshots, function (snapshot) {
            return snapshot.id === id;
          });
          if (snapshot) delete snapshot._id;
          var evnts = _.filter(events, function (event) {
            return event.id === id;
          });
          var entity = self._deserialize(id, snapshot, evnts);
          results.push(entity);
        });
        return resolve(results);
      });
  });
};

module.exports.Repository = Repository;