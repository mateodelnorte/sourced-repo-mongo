var Entity = require('sourced').Entity;
var sourcedRepoMongo = require('../index');
var Repository = sourcedRepoMongo.Repository;
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var _ = require('lodash');

sourcedRepoMongo.config.mongoUrl = 'mongodb://127.0.0.1:27017/sourced'

var repository = new Repository(Market);

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

var market = new Market();

market.init({ id: 'somecusip' });

market.createOrder({ side: 'b', price: 90, quantity: 1000 });
market.createOrder({ side: 's', price: 91, quantity: 1000 });
market.createOrder({ side: 'b', price: 92, quantity: 1000 });
market.createOrder({ side: 's', price: 93, quantity: 1000 });
market.createOrder({ side: 'b', price: 94, quantity: 1000 });
market.createOrder({ side: 's', price: 95, quantity: 1000 });
market.createOrder({ side: 'b', price: 90, quantity: 1000 });
market.createOrder({ side: 's', price: 91, quantity: 1000 });
market.createOrder({ side: 'b', price: 92, quantity: 1000 });
market.createOrder({ side: 's', price: 93, quantity: 1000 });
market.createOrder({ side: 'b', price: 94, quantity: 1000 });

repository.commit(market, function (err) {
  if (err) throw err;

  repository.get(market.id, function (err, market1) {
    if (err) throw err;


    market1.createOrder({ side: 'b', price: 90, quantity: 1000 });
    market1.createOrder({ side: 's', price: 91, quantity: 1000 });


    repository.commit(market1, function (err) {
      if (err) throw err;

        repository.get(market.id, function (err, market4) {
          if (err) throw err;

          console.log('current market state: ', market4);

        });

    });
  })
})