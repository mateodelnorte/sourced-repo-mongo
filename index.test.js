const Entity = require('sourced').Entity;
const log = require('debug')('sourced-repo-mongo');
const mongo = require('./mongo.js');
const sourcedRepoMongo = require('./index.js');
const Repository = sourcedRepoMongo.Repository;

const should = require('should');

/* Market model/entity */
class Market extends Entity {
  constructor(snapshot, events) {
    super()
    this.orders = [];
    this.price = 0;

    this.rehydrate(snapshot, events)
  }

  init(param) {
    this.id = param.id;
    this.digest('init', param);
    this.emit('initialized', param, this);
  }

  createOrder(param) {
    this.orders.push(param);
    var total = 0;
    this.orders.forEach(function (order) {
      total += order.price;
    });
    this.price = total / this.orders.length;
    this.digest('createOrder', param);
    this.emit('done', param, this);
  };
}

/* end Market model/entity */

describe('Repository', function () {

  let repository;

  beforeEach(function (done) {
    log('connecting to mongo')
    mongo.once('connected', function (db) {
      db.collection('Market.events').drop(function () {
        db.collection('Market.snapshots').drop(function () {
          log('connected to mongo, creating repo')
          repository = new Repository(Market);
          done();
        });
      });
    });
    mongo.connect('mongodb://127.0.0.1:27017/sourced');
  });

  afterAll(function (done) {
    mongo.close(done);
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

    expect(mrkt.version).toBe(12);
    expect(mrkt.snapshotVersion).toBe(0);
    expect(mrkt.price).toBe(92.27272727272727);

    repository.commit(mrkt, function (err) {
      if (err) throw err;

      repository.get(id, function (err, market) {
        if (err) throw err;

        expect(market.version).toBe(12);
        expect(market.snapshotVersion).toBe(12);
        expect(market.price).toBe(92.27272727272727);

        done();

      });
    });
  });

  it('should load deserialized market entity from snapshot, digest two events, and update version, snapshotVersion, and price', function (done) {

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

    expect(mrkt.version).toBe(12);
    expect(mrkt.snapshotVersion).toBe(0);
    expect(mrkt.price).toBe(92.27272727272727);

    repository.commit(mrkt, function (err) {
      if (err) throw err;

      repository.get(id, function (err, mrkt) {
        if (err) throw err;

        expect(mrkt.version).toBe(12);
        expect(mrkt.snapshotVersion).toBe(12);
        expect(mrkt.price).toBe(92.27272727272727);

        mrkt.createOrder({ side: 'b', price: 90, quantity: 1000 });
        mrkt.createOrder({ side: 's', price: 91, quantity: 1000 });

        expect(mrkt.version).toBe(14);
        expect(mrkt.snapshotVersion).toBe(12);
        expect(mrkt.price).toBe(92);
        expect(mrkt.newEvents.length).toBe(2);

        repository.commit(mrkt, function (err) {
          if (err) throw err;

            repository.get(id, function (err, market) {
              if (err) throw err;

              expect(market.version).toBe(14);
              expect(market.snapshotVersion).toBe(12);
              expect(market.price).toBe(92);
              expect(market.newEvents.length).toBe(0);

              done();

            });

        });
      });
    });

  });

  it('should emit all enqueued eventsToEmit after only after committing', function (done) {

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

    expect(mrkt.version).toBe(12);
    expect(mrkt.snapshotVersion).toBe(0);
    expect(mrkt.price).toBe(92.27272727272727);

    repository.commit(mrkt, function (err) {
      if (err) throw err;


      repository.get(id, function (err, market) {
        if (err) throw err;

        market.on('myEventHappened', function (data, data2) {
          expect(market.eventsToEmit.length).toBe(0);
          expect(market.newEvents.length).toBe(0);
          expect(data.data).toBe('data');
          expect(data2.data2).toBe('data2');
          done();
        });

        market.enqueue('myEventHappened', { data: 'data' }, { data2: 'data2' });

        repository.commit(market, function (err) {
          if (err) throw err;

            repository.get(id, function (err, market) {
              if (err) throw err;

              expect(market.version).toBe(12);
              expect(market.snapshotVersion).toBe(12);
              expect(market.price).toBe(92.27272727272727);
              expect(market.newEvents.length).toBe(0);

            });

        });
      });
    });

  });

  it('should load multiple deserialized market entities from snapshot, and commit in bulk', function (done) {

    var id = 'somecusip2';
    var mrkt = new Market();

    var id2 = 'somecusip3';
    var mrkt2 = new Market();

    var id3 = 'somecusip4';
    var mrkt3 = new Market();

    var id4 = 'somecusip5';
    var mrkt4 = new Market();

    mrkt.init({ id: id });
    mrkt2.init({ id: id2 });
    mrkt3.init({ id: id3 });
    mrkt4.init({ id: id4 });

    mrkt.createOrder({ side: 'b', price: 90, quantity: 1001 });

    mrkt2.createOrder({ side: 'b', price: 90, quantity: 1002 });
    mrkt2.createOrder({ side: 'b', price: 90, quantity: 1003 });

    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1004 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1005 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1006 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1007 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1008 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1009 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1010 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1011 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1012 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1013 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1014 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1015 });

    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1016 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1017 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1018 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1019 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1020 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1022 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1023 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1024 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1025 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1026 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1027 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1028 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1029 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1030 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1031 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1032 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1033 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1034 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1035 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1036 });

    repository.commitAll([mrkt, mrkt2, mrkt3, mrkt4], function (err) {
      if (err) return done(err);

      repository.getAll([ id, id2, id3, id4 ], function (err, markets) {
        if (err) return done(err);

        var market = markets[0];
        var market2 = markets[1];
        var market3 = markets[2];
        var market4 = markets[3];

        expect(market.id).toBe(id);
        expect(market.version).toBe(2);
        expect(market.snapshotVersion).toBe(0);

        expect(market2.id).toBe(id2);
        expect(market2.version).toBe(3);
        expect(market2.snapshotVersion).toBe(0);

        expect(market3.id).toBe(id3);
        expect(market3.version).toBe(13);
        expect(market3.snapshotVersion).toBe(13);

        expect(market4.id).toBe(id4);
        expect(market4.version).toBe(21);
        expect(market4.snapshotVersion).toBe(21);

        done();

      });
    });

  });

  it('should load all entities when getAll called with callback only', function (done) {

    var id = 'somecusip6';
    var mrkt = new Market();

    var id2 = 'somecusip7';
    var mrkt2 = new Market();

    var id3 = 'somecusip8';
    var mrkt3 = new Market();

    var id4 = 'somecusip9';
    var mrkt4 = new Market();

    mrkt.init({ id: id });
    mrkt2.init({ id: id2 });
    mrkt3.init({ id: id3 });
    mrkt4.init({ id: id4 });

    mrkt.createOrder({ side: 'b', price: 90, quantity: 1001 });

    mrkt2.createOrder({ side: 'b', price: 90, quantity: 1002 });
    mrkt2.createOrder({ side: 'b', price: 90, quantity: 1003 });

    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1004 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1005 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1006 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1007 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1008 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1009 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1010 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1011 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1012 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1013 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1014 });
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1015 });

    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1016 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1017 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1018 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1019 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1020 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1022 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1023 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1024 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1025 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1026 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1027 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1028 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1029 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1030 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1031 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1032 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1033 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1034 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1035 });
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1036 });

    repository.commitAll([mrkt, mrkt2, mrkt3, mrkt4], function (err) {
      if (err) return done(err);

      repository.getAll(function (err, markets) {
        if (err) return done(err);

        var market = markets[0];
        var market2 = markets[1];
        var market3 = markets[2];
        var market4 = markets[3];

        expect(market.id).toBe(id);
        expect(market.version).toBe(2);
        expect(market.snapshotVersion).toBe(0);

        expect(market2.id).toBe(id2);
        expect(market2.version).toBe(3);
        expect(market2.snapshotVersion).toBe(0);

        expect(market3.id).toBe(id3);
        expect(market3.version).toBe(13);
        expect(market3.snapshotVersion).toBe(13);

        expect(market4.id).toBe(id4);
        expect(market4.version).toBe(21);
        expect(market4.snapshotVersion).toBe(21);

        done();

      });
    });

  });

  it('should take snapshot when forceSnapshot provided', function (done) {

    var id = 'somecusip6';

    var mrkt = new Market();

    mrkt.init({ id: id });

    mrkt.createOrder({ side: 'b', price: 90, quantity: 1000 });

    expect(mrkt.version).toBe(2);
    expect(mrkt.snapshotVersion).toBe(0);
    expect(mrkt.price).toBe(90);

    repository.commit(mrkt, { forceSnapshot: true }, function (err) {
      if (err) throw err;

      repository.get(id, function (err, market) {
        if (err) throw err;

        expect(market.version).toBe(2);
        expect(market.snapshotVersion).toBe(2);
        expect(market.price).toBe(90);

        done();

      });

    });

  });

  it('should return null when get called with id of nonexisting entity', function (done) {

    repository.get('fake', function (err, market) {
      if (err) throw err;

      should(market).eql(null);

      done();

    });

  });

  it('should return null when getAll called with only ids of nonexisting entities', function (done) {

    repository.getAll(['fake'], function (err, market) {
      if (err) throw err;

      should(market).eql(null);

      done();

    });

  });

});