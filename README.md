sourced-repo-mongo
==================

mongo data store and repository for sourced-style event sourcing models

Connecting to mongo:

```
// run at application start
var mongo = require('sourced-repo-mongo/mongo');

mongo.once('connected', function () {
  /*  code away! /*
});

mongo.connect(config.MONGO_URL);
```

sourced-repo-mongo provides a exports Repository, which you can inherit. Doing so for each of your `sourced` entities will provide you a means of getting and persisting entities from mongo: 

```
const Customer = require('./models/customer');
const MongoRepository = require('sourced-repo-mongo').Repository;
const util = require('util');

function CustomerRepository () {
  MongoRepository.call(this, Trader);
}

util.inherits(CustomerRepository, MongoRepository);

module.exports = new CustomerRepository();
```
Repositories can `get` entities from mongo, and `commit` them to the db. Entities are event-sourced, meaning they are saved and rehydrated using streams of events that have been applied to them, and snapshots for performance reasons. When you `get` an entity, `sourced-repo-mongo`'s `Repository` will get the latest snapshot for an entity, plus all events which were persisted since that snapshot. It will merge the snapshot into the newly instantiated entity, then replay all remaining events - bringing the entity to its most recent state:

```
const customerRepository = require('../customerRepository');

// get a customer, by entity id. this will get the latest snapshot and events, and replay the entity to its current state
customerRepository.get(event.data.customerId, (err, customer) => {
  if (err) return cb(err);
 
  customer.changeAddress(param);

  // to ensure atomic operations, sourced entities can enqueue events to be emitted only after the entity is successfully committed. this event will be emitted after the commit callback, below. 
  customer.once('address.changed', (c) => {
    bus.publish('trader.settings.updated', c, cb);
  });

  traderRepository.commit(trader, function (err) {
    if (err) return cb(err);
  });

};

```