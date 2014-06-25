var Entity = require('sourced').Entity;
var log = require('debug')('sourced-repo-mongo');
var sourcedRepoMongo = require('../index');
var Repository = sourcedRepoMongo.Repository;
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var _ = require('lodash');

require('should');

/* Market model/entity */
function Market () {
  this.orders = [];
  Entity.apply(this, arguments);
}

util.inherits(Market, Entity);

Market.prototype.init = function (param) {
  this.id = param.id;
  this.digest('init', param);
  this.emit('initialized', param, this);
}

Market.prototype.createOrder = function (param) {
  this.orders.push(param);
  var total = 0;
  this.orders.forEach(function (order) {
    total += order.price;
  });
  this.price = total / this.orders.length;
  this.digest('createOrder', param);
  this.emit('done', param, this);
};
/* end Market model/entity */

describe('Repository', function () {

  // point us at a local test database
  sourcedRepoMongo.config.mongoUrl = 'mongodb://127.0.0.1:27017/sourced'

  var repository;

  before(function (done) {
    var mongo = require('../mongo');
    mongo.once('connected', function (db) {
      db.collection('Market.events').drop();
      db.collection('Market.snapshots').drop();
      repository = new Repository(Market);
      done();
    });
    mongo.connect(sourcedRepoMongo.config.mongoUrl);
  });

  it('should initialize market entity and digest 12 events, setting version, snapshotVersion, and price', function (done) {

    var id = 'somecusip';
    var mrkt = new Market();

    mrkt.init({ id: id });

    mrkt.createOrder({ side: 'b', price: 90, quantity: 1000 });
    mrkt.createOrder({ side: 's', price: 91, quantity: 1000 });
    mrkt.createOrder({ side: 'b', price: 92, quantity: 1000 });
    mrkt.createOrder({ side: 's', price: 93, quantity: 1000 });
    mrkt.createOrder({ side: 'b', price: 94, quantity: 1000 });
    mrkt.createOrder({ side: 's', price: 95, quantity: 1000 });
    mrkt.createOrder({ side: 'b', price: 90, quantity: 1000 });
    mrkt.createOrder({ side: 's', price: 91, quantity: 1000 });
    mrkt.createOrder({ side: 'b', price: 92, quantity: 1000 });
    mrkt.createOrder({ side: 's', price: 93, quantity: 1000 });
    mrkt.createOrder({ side: 'b', price: 94, quantity: 1000 });

    mrkt.should.have.property('version', 12);
    mrkt.should.have.property('snapshotVersion', 0);
    mrkt.should.have.property('price', 92.27272727272727);

    repository.commit(mrkt, function (err) {
      if (err) throw err;

      repository.get(id, function (err, market) {
        if (err) throw err;

        market.should.have.property('version', 12);
        market.should.have.property('snapshotVersion', 12);
        market.should.have.property('price', 92.27272727272727);

        done();

      });
    });
  });

  it('should load deserialize market entity from snapshot, digest two events, and update version, snapshotVersion, and price', function (done) {

    var id = 'somecusip';

    repository.get(id, function (err, mrkt) {
      if (err) throw err;

      mrkt.should.have.property('version', 12);
      mrkt.should.have.property('snapshotVersion', 12);
      mrkt.should.have.property('price', 92.27272727272727);

      mrkt.createOrder({ side: 'b', price: 90, quantity: 1000 });
      mrkt.createOrder({ side: 's', price: 91, quantity: 1000 });

      mrkt.should.have.property('version', 14);
      mrkt.should.have.property('snapshotVersion', 12);
      mrkt.should.have.property('price', 92);
      mrkt.newEvents.should.have.property('length', 2);

      repository.commit(mrkt, function (err) {
        if (err) throw err;

          repository.get(id, function (err, market) {
            if (err) throw err;

            market.should.have.property('version', 14);
            market.should.have.property('snapshotVersion', 12);
            market.should.have.property('price', 92);
            market.newEvents.should.have.property('length', 0);

            done();

          });

      });
    });

  });

  it('should emit all enqueued eventsToEmit after only after committing', function (done) {

    var id = 'somecusip';

    repository.get(id, function (err, market) {
      if (err) throw err;

      market.on('myEventHappened', function (data, data2) {
        market.eventsToEmit.should.have.property('length', 0);
        market.newEvents.should.have.property('length', 0);
        data.should.have.property('data', 'data');
        data2.should.have.property('data2', 'data2');
        done();
      });

      market.enqueue('myEventHappened', { data: 'data' }, { data2: 'data2' });
      
      repository.commit(market, function (err) {
        if (err) throw err;

          repository.get(id, function (err, market) {
            if (err) throw err;

            market.should.have.property('version', 14);
            market.should.have.property('snapshotVersion', 12);
            market.should.have.property('price', 92);
            market.newEvents.should.have.property('length', 0);

          });

      });
    })

  });
});