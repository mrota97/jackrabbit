import { describe, it, expect, beforeAll } from "vitest";
import { promisify } from "node:util";
import amqp from "amqplib/callback_api";
import { v4 as uuid } from "uuid";
import exchange, { Exchange } from "../src/exchange.js";
import queue, { AckCallback, NackCallback, Queue } from "../src/queue.js";

declare global {
    namespace NodeJS {
        interface ProcessEnv {
            RABBIT_URL: string;
        }
    }
}

describe("queue", function() {
  describe("consume", function() {
    let connection: amqp.Connection, name: string, _exchange: Exchange, _queue: Queue;
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

      await new Promise<void>((resolve) => {
        _queue.consume(onMessage, { noAck: true });
  
        for (var i = 0; i < n; i++) {
          _exchange.publish(message, { key: name });
        }
  
        function onMessage(data: unknown, ack: AckCallback, nack: NackCallback, msg: amqp.Message) {
          expect(data).toBe(message);
          received++;
          if (received === n) resolve();
        }
      });
    });
  });

  describe("cancel", function() {
    let connection: amqp.Connection, name: string, _exchange: Exchange, _queue: Queue;
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

      await new Promise<void>((resolve) => {
        _queue.consume(onMessage, { noAck: true });
        _exchange.publish(message, { key: name });
  
        function onMessage(data: unknown, ack: AckCallback, nack: NackCallback, msg: amqp.Message) {
          expect(data).toBe(message);
          resolve();
        }
      });
    });

    it("calls back with ok", async function() {
      await new Promise<void>((resolve, reject) => {
        _queue.cancel((err, ok) => {
          if (err) return reject(err);
          expect(ok).toBeTruthy();
          resolve();
        });
      });
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
    let connection: amqp.Connection, name: string, _exchange: Exchange, _queue: Queue;
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
      const count = await new Promise<number>((resolve, reject) => {
        _queue.purge((err, count) => {
          if (err) return reject(err);
          resolve(count!);
        });
      });
      expect(count).toBeGreaterThan(5);
    });
  });
});

/**
 * 
 * @returns {Promise<amqp.Connection>}
 */
async function createConnection() {
  return new Promise<amqp.Connection>((resolve) => amqp.connect(
    process.env.RABBIT_URL,
    function(err, conn) {
      expect(err).toBeFalsy();
      resolve(conn);
    }
  ));
}
