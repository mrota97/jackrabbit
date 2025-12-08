import { describe, it, expect, beforeAll } from "vitest";
import { promisify } from "node:util";
import amqp from "amqplib/callback_api";
import exchange, { Exchange } from "../src/exchange.js";
import { v4 as uuid } from "uuid";
import { Queue } from "../src/queue.js";

declare global {
    namespace NodeJS {
        interface ProcessEnv {
            RABBIT_URL: string;
        }
    }
}

describe("exchange", function() {
  describe("constructor", function() {
    describe("with empty name ('') and direct type", function() {
      var e = exchange("", "direct");
      it("returns an exchange", function() {
        expect(e.name).toBe("");
        expect(e.type).toBe("direct");
        expect(e.queue).toBeTruthy();
        expect(e.publish).toBeTruthy();
      });
    });

    describe("with no name", function() {
      describe("and a direct type", function() {
        var e = exchange(undefined, "direct");
        it("receives the default name amq.direct", function() {
          expect(e.name).toBe("amq.direct");
        });
      });

      describe("and a fanout type", function() {
        var e = exchange(undefined, "fanout");
        it("receives the default name amq.fanout", function() {
          expect(e.name).toBe("amq.fanout");
        });
      });

      describe("and a topic type", function() {
        var e = exchange(undefined, "topic");
        it("receives the default name amq.topic", function() {
          expect(e.name).toBe("amq.topic");
        });
      });

      describe("and no type", function() {
        it("throws an error", function() {
          // @ts-ignore
          expect(() => exchange(undefined, undefined)).toThrow("missing exchange type");
        });
      });
    });
  });

  describe("#connect", function() {
    let connection: amqp.Connection;
    beforeAll(async function() {
      connection = await new Promise<amqp.Connection>((resolve) => {
        amqp.connect(
          process.env.RABBIT_URL,
          function(err, conn) {
            expect(err).toBeFalsy();
            resolve(conn);
          }
        );
      });
    });
    it('emits a "connected" event', async function() {
      await new Promise<void>((resolve) => {
        exchange("", "direct")
          .connect(connection)
          .once("connected", resolve);
      });
    });
  });

  describe("#queue", function() {
    describe("with no options", function() {
      let connection: amqp.Connection;
      beforeAll(async function() {
        connection = await promisify<amqp.Connection>((done) => {
          amqp.connect(
            process.env.RABBIT_URL,
            function(err, conn) {
              expect(err).toBeFalsy();
              done(null, conn);
            }
          );
        })();
      });
      let q: Queue;
      beforeAll(function() {
        q = exchange("", "direct")
          .connect(connection)
          .queue();
      });
      it("returns a queue instance", function() {
        expect(q.consume).toBeTruthy();
      });
    });

    describe("with key bindings", async function() {
      let _exchange: Exchange;
      beforeAll(async function() {
        const connection = await new Promise<amqp.Connection>((resolve) => {
          amqp.connect(
            process.env.RABBIT_URL,
            function(err, conn) {
              expect(err).toBeFalsy();
              resolve(conn);
            }
          );
        });

        _exchange = exchange("test.topic.bindings", "topic").connect(connection);
        await new Promise<void>((resolve) => {
          _exchange.once("connected", resolve);
        });
      });

      it('emits a "bound" event when all routing keys have been bound to the queue', async function() {
        var keys = "abcdefghijklmnopqrstuvwxyz".split("");
        var finalKey = keys[keys.length - 1];
        var queue = _exchange.queue({ keys: keys });
        var message = uuid();

        await new Promise<void>((resolve) => {
          queue.consume(function(data, ack, nack, msg) {
            expect(data).toBe(message);
            expect(msg.fields.routingKey).toBe(finalKey);
            ack();
            resolve();
          });
  
          queue.once(
            "bound",
            function() {
              _exchange.publish(message, { key: finalKey });
            }
          );
        });
      });
    });
  });

  // describe("#publish", function() {});
});
