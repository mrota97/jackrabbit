import { describe, it, expect, beforeAll } from "vitest";
import { promisify } from "node:util";
import amqp from "amqplib/callback_api";
import { v4 as uuid } from "uuid";
import exchange from "../lib/exchange.js";
import queue from "../lib/queue.js";

describe("queue", function() {
  describe("consume", function() {
    let connection, name, _exchange, _queue;
    beforeAll(async function() {
      connection = await createConnection();
    });

    beforeAll(function() {
      name = `test.queue.consume.${uuid()}`;
      _exchange = exchange("", "direct");
      _exchange.connect(connection);
      _queue = queue({ name: name });
      _queue.connect(connection);
    });

    it("calls the message handler when a message arrives", async function() {
      var message = uuid();
      var n = 3;
      var received = 0;

      await promisify((done) => {
        _queue.consume(onMessage, { noAck: true });
  
        for (var i = 0; i < n; i++) {
          _exchange.publish(message, { key: name });
        }
  
        function onMessage(data, ack, nack, msg) {
          expect(data).toBe(message);
          received++;
          if (received === n) done();
        }
      })();
    });
  });

  describe("cancel", function() {
    let connection, name, _exchange, _queue;
    beforeAll(async function() {
      connection = await createConnection();
    });

    beforeAll(function() {
      name = `test.queue.cancel.${uuid()}`;
      _exchange = exchange("", "direct");
      _exchange.connect(connection);
      _queue = queue({ name: name });
      _queue.connect(connection);
    });

    it("initially consumes messages", async function() {
      var message = uuid();

      await promisify((done) => {
        _queue.consume(onMessage, { noAck: true });
        _exchange.publish(message, { key: name });
  
        function onMessage(data, ack, nack, msg) {
          expect(data).toBe(message);
          done();
        }
      })();
    });

    it("calls back with ok", async function() {
      await promisify(_queue.cancel)();
    });

    it("stops consuming after cancel", async function() {
      _exchange.publish("should not consume", {
        key: name,
        noAck: true
      });

      await new Promise((resolve) => setTimeout(resolve, 250));
    });
  });

  describe("purge", function() {
    let connection, name, _exchange, _queue;
    beforeAll(async function() {
      connection = await createConnection();
    });

    beforeAll(function() {
      name = `test.queue.purge.${uuid()}`;
      _exchange = exchange("", "direct");
      _exchange.connect(connection);
      _queue = queue({ name: name });
      _queue.connect(connection);
    });

    beforeAll(async function() {
      var n = 10;
      while (n--) {
        _exchange.publish("test", { key: name });
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    });

    it("returns the number of messages purged", async function() {
      const count = await promisify(_queue.purge)();
      expect(count).toBeGreaterThan(5);
    });
  });
});

/**
 * 
 * @returns {Promise<amqp.Connection>}
 */
async function createConnection() {
  return await promisify((done) => amqp.connect(
    process.env.RABBIT_URL,
    function(err, conn) {
      expect(err).toBeFalsy();
      done(null, conn);
    }
  ))();
}
