var Entity = require('sourced').Entity;
var EventEmitter = require('events').EventEmitter;
var log = require('debug')('sourced-repo-mongo');
var mongo = require('./mongo');
var util = require('util');
var _ = require('lodash');

function Repository (entityType, options) {
  options = options || {};
  EventEmitter.call(this);
  if ( ! mongo.db) {
    throw new Error('mongo has not been initialized. you must call require(\'sourced-repo-mongo/mongo\').connect(config.MONGO_URL); before instantiating a Repository');
  }
  var indices = _.union(options.indices, ['id']);
  var self = this;
  var db = mongo.db;
  self.entityType = entityType;
  self.indices = indices;
  self.snapshotFrequency = options.snapshotFrequency || 10;

  var snapshotCollectionName = util.format('%s.snapshots', entityType.name);
  var snapshots = db.collection(snapshotCollectionName);
  self.snapshots = snapshots;
  var eventCollectionName = util.format('%s.events', entityType.name);
  var events = db.collection(eventCollectionName);
  self.events = events;

  var error = function (err) {
    if (err) return this.emit('error', err);
  };

  self.indices.forEach(function (index) {
    snapshots.ensureIndex(index, error);
    events.ensureIndex(index, error);
  });
  events.ensureIndex({ id: 1, version: 1 }, error);
  snapshots.ensureIndex({ id: 1, version: 1 }, error);
  snapshots.ensureIndex('snapshotVersion', error);

  log('initialized %s entity store', self.entityType.name);

  self.emit('ready');
}

util.inherits(Repository, EventEmitter);

Repository.prototype.commit = function commit (entity, options, cb) {
  if (typeof options === 'function') {
    cb = options;
    options = {};
  }

  var self = this;

  log('committing %s for id %s', this.entityType.name, entity.id);

  this._commitEvents(entity, function _afterCommitEvents (err) {
    if (err) return cb(err);

    self._commitSnapshots(entity, options, function _afterCommitSnapshots (err) {
      if (err) return cb(err);
      self._emitEvents(entity);
      return cb();
    });
  });

};

Repository.prototype.commitAll = function commit (entities, options, cb) {
  if (typeof options === 'function') {
    cb = options;
    options = {};
  }

  var self = this;

  log('committing %s for id %j', this.entityType.name, _.pluck(entities, 'id'));

  this._commitAllEvents(entities, function _afterCommitEvents (err) {
    if (err) return cb(err);
    self._commitAllSnapshots(entities, options, function _afterCommitSnapshots (err) {
      if (err) return cb(err);
      var promises = [];
      entities.forEach(function (entity) {
        promises.push(self._emitEvents(entity));
      });
      return cb();
    });
  });

};

Repository.prototype.get = function get (id, cb) {
  return this._getByIndex('id', id, cb);
};

Repository.prototype._getByIndex = function _getByIndex (index, value, cb) {
  var self = this;

  if (this.indices.indexOf(index) === -1) throw new Error('Cannot get sourced entity type [%s] by index [%s]', this.entityType, index);

  log('getting %s for %s %s', this.entityType.name, index, value);

  var criteria = { };
  criteria[index] = value;

  this.snapshots
    .find(criteria)
    .sort({ version: -1 })
    .limit(-1)
    .toArray(function (err, snapshots) {
      if (err) return cb(err);

      var snapshot = snapshots[0];

      if (snapshot) criteria.version = { $gt: snapshot.version };
      self.events.find(criteria)
        .sort({ version: 1 })
        .toArray(function (err, events) {
          if (err) return cb(err);
          if (snapshot) delete snapshot._id;
          if ( ! snapshot && ! events.length) return cb(null, null);

          var id = (index === 'id') ? value : (snapshot) ? snapshot.id : events[0].id;

          var entity = self._deserialize(id, snapshot, events);
          return cb(null, entity);
        });
  });
};

Repository.prototype.getAll = function getAll (ids, cb) {
  var self = this;

  if (typeof ids === 'function') {
    cb = ids;
    ids = null;
    return this.events.distinct('id', function (err, distinctEventIds) {
      self.getAll(distinctEventIds, cb);
    });
  }

  log('getting %ss for ids %j', this.entityType.name, ids);

  if (ids.length === 0) return cb(null, []);

  this._getAllSnapshots(ids, function _afterGetAllSnapshots (err, snapshots) {
    if (err) return cb(err);
    self._getAllEvents(ids, snapshots, function (err, entities) {
      if (err) return cb(err);
      return cb(null, entities);
    });
  });
};

Repository.prototype._commitEvents = function _commitEvents (entity, cb) {
  var self = this;

  if (entity.newEvents.length === 0) return cb();

  if ( ! entity.id) return cb(new Error('Cannot commit an entity of type [%s] without an [id] property', this.entityType));

  var events = entity.newEvents;
  events.forEach(function _applyIndices (event) {
    if (event && event._id) delete event._id; // mongo will blow up if we try to insert multiple _id keys
    self.indices.forEach(function (index) {
      event[index] = entity[index];
    });
  });
  self.events.insert(events, function (err) {
    if (err) return cb(err);
    log('committed %s.events for id %s', self.entityType.name, entity.id);
    entity.newEvents = [];
    return cb();
  });

};

Repository.prototype._commitAllEvents = function _commitEvents (entities, cb) {
  var self = this;

  var events = [];
  entities.forEach(function (entity) {
    if (entity.newEvents.length === 0) return;

    if ( ! entity.id) return cb(new Error('Cannot commit an entity of type [%s] without an [id] property', self.entityType));

    var evnts = entity.newEvents;
    evnts.forEach(function _applyIndices (event) {
      if (event && event._id) delete event._id; // mongo will blow up if we try to insert multiple _id keys
      self.indices.forEach(function (index) {
        event[index] = entity[index];
      });
    });
    Array.prototype.unshift.apply(events, evnts);
  });

  if (events.length === 0) return cb();

  self.events.insert(events, function (err) {
    if (err) return cb(err);
    log('committed %s.events for ids %j', self.entityType.name, _.pluck(entities, 'id'));
    entities.forEach(function (entity) {
      entity.newEvents = [];
    });
    return cb();
  });

};

Repository.prototype._commitSnapshots = function _commitSnapshots (entity, options, cb) {
  var self = this;

  if (options.forceSnapshot || entity.version >= entity.snapshotVersion + self.snapshotFrequency) {
    var snapshot = entity.snapshot();
    if (snapshot && snapshot._id) delete snapshot._id; // mongo will blow up if we try to insert multiple _id keys
    self.snapshots.insert(snapshot, function (err) {
      if (err) return cb(err);
      log('committed %s.snapshot for id %s %j', self.entityType.name, entity.id, snapshot);
      return cb(null, entity);
    });
  } else {
    return cb(null, entity);
  }

};

Repository.prototype._commitAllSnapshots = function _commitAllSnapshots (entities, options, cb) {
  var self = this;

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

  if (snapshots.length === 0) return cb();

  self.snapshots.insert(snapshots, function (err) {
    if (err) return cb(err);
    log('committed %s.snapshot for ids %s %j', self.entityType.name, _.pluck(entities, 'id'), snapshots);
    return cb(null, entities);
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

  var eventsToEmit = entity.eventsToEmit;
  entity.eventsToEmit = [];
  eventsToEmit.forEach(function (eventToEmit) {
    var args = Array.prototype.slice.call(eventToEmit);
    self.entityType.prototype.emit.apply(entity, args);
  });

  log('emitted local events for id %s', entity.id);

};

Repository.prototype._getAllSnapshots = function _getAllSnapshots (ids, cb) {
  var self = this;

  var match = { $match: { id: { $in: ids } } };
  var group = { $group: { _id: '$id', snapshotVersion: { $last: '$snapshotVersion' } } };

  self.snapshots.aggregate([match, group], function (err, idVersionPairs) {
    if (err) return cb(err);
    var criteria = {};
    if (idVersionPairs.length === 0) {
      return cb(null, []);
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
        if (err) cb(err);
        return cb(null, snapshots);
      });
  });

};

Repository.prototype._getAllEvents = function _getAllEvents (ids, snapshots, cb) {
  var self = this;

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
      if (err) return cb(err);
      if ( ! snapshots.length && ! events.length) return cb(null, null);
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
      return cb(null, results);
    });

};

module.exports.Repository = Repository;