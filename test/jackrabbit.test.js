import { describe, it, expect, beforeAll } from "vitest";
import { promisify } from "node:util";
import jackrabbit from "../lib/jackrabbit.js";

describe("jackrabbit", function() {
  describe("constructor", function() {
    describe("with a valid server url", function() {
      let r;
      it('emits a "connected" event', async function() {
        await promisify((done) => {
          r = jackrabbit(process.env.RABBIT_URL);
          r.once("connected", done);
        })();
      });
      it("references a Connection object", function() {
        var c = r.getInternals().connection;
        expect(c.connection.stream.writable).toBeTruthy();
      });
    });
    describe("without a server url", function() {
      it('throws a "url required" error', function() {
        expect(() => jackrabbit()).toThrow("url required");
      });
    });
    // describe('with an invalid url', function() {
    //   it('emits an "error" event', function(done) {
    //     jackrabbit('amqp://1.2')
    //       .once('error', function(err) {
    //         assert.ok(err);
    //         done();
    //       });
    //   });
    // });
  });

  describe("#default", function() {
    describe('without a "name" argument', function() {
      let e;
      beforeAll(function() {
        var r = jackrabbit(process.env.RABBIT_URL);
        e = r.default();
      });
      it("returns a direct, nameless exchange", function() {
        expect(e.queue).toBeTruthy();
        expect(e.publish).toBeTruthy();
        expect(e.type).toBe("direct");
        expect(e.name).toBe("");
      });
    });
    describe('with a "name" argument', function() {
      let e;
      beforeAll(function() {
        var r = jackrabbit(process.env.RABBIT_URL);
        e = r.default("foobar");
      });
      it("returns a direct, nameless exchange", function() {
        expect(e.queue).toBeTruthy();
        expect(e.publish).toBeTruthy();
        expect(e.type).toBe("direct");
        expect(e.name).toBe("");
      });
    });
    describe("before connection is established", function() {
      it("passes the connection to the exchange", async function() {
        await promisify((done) => {
          jackrabbit(process.env.RABBIT_URL)
            .default()
            .once("connected", done);
        })();
      });
    });
    describe("after connection is established", function() {
      let r;
      beforeAll(async function() {
        r = jackrabbit(process.env.RABBIT_URL);
        await promisify((done) => r.once("connected", done))();
      });
      it("passes the connection to the exchange", async function() {
        await promisify((done) => r.default().once("connected", done))();
      });
    });
  });

  describe("#direct", function() {
    describe('without a "name" argument', function() {
      let e;
      beforeAll(function() {
        var r = jackrabbit(process.env.RABBIT_URL);
        e = r.direct();
      });
      it('returns the direct exchange named "amq.direct"', function() {
        expect(e.queue).toBeTruthy();
        expect(e.publish).toBeTruthy();
        expect(e.type).toBe("direct");
        expect(e.name).toBe("amq.direct");
      });
    });
    describe('with a "name" argument of "foobar.direct"', function() {
      let e;
      beforeAll(function() {
        var r = jackrabbit(process.env.RABBIT_URL);
        e = r.direct("foobar.direct");
      });
      it('returns a direct exchange named "foobar.direct"', function() {
        expect(e.queue).toBeTruthy();
        expect(e.publish).toBeTruthy();
        expect(e.type).toBe("direct");
        expect(e.name).toBe("foobar.direct");
      });
    });
    describe("before connection is established", function() {
      it("passes the connection to the exchange", async function() {
        await promisify((done) => {
          jackrabbit(process.env.RABBIT_URL)
            .direct()
            .once("connected", done);
        })();
      });
    });
    describe("after connection is established", function() {
      let r;
      beforeAll(async function() {
        r = jackrabbit(process.env.RABBIT_URL);
        await promisify((done) => r.once("connected", done))();
      });
      it("passes the connection to the exchange", async function() {
        await promisify((done) => r.direct().once("connected", done))();
      });
    });
  });

  describe("#fanout", function() {
    describe('without a "name" argument', function() {
      let e;
      beforeAll(function() {
        var r = jackrabbit(process.env.RABBIT_URL);
        e = r.fanout();
      });
      it('returns the direct exchange named "amq.fanout"', function() {
        expect(e.queue).toBeTruthy();
        expect(e.publish).toBeTruthy();
        expect(e.type).toBe("fanout");
        expect(e.name).toBe("amq.fanout");
      });
    });
    describe('with a "name" argument of "foobar.fanout"', function() {
      let e;
      beforeAll(function() {
        var r = jackrabbit(process.env.RABBIT_URL);
        e = r.fanout("foobar.fanout");
      });
      it('returns a direct exchange named "foobar.fanout"', function() {
        expect(e.queue).toBeTruthy();
        expect(e.publish).toBeTruthy();
        expect(e.type).toBe("fanout");
        expect(e.name).toBe("foobar.fanout");
      });
    });
    describe("before connection is established", function() {
      it("passes the connection to the exchange", async function() {
        await promisify((done) => {
          jackrabbit(process.env.RABBIT_URL)
            .fanout()
            .once("connected", done);
        })();
      });
    });
    describe("after connection is established", function() {
      let r;
      beforeAll(async function() {
        r = jackrabbit(process.env.RABBIT_URL);
        await promisify((done) => r.once("connected", done))();
      });
      it("passes the connection to the exchange", async function() {
        await promisify((done) => r.fanout().once("connected", done))();
      });
    });
  });

  describe("#close", function() {
    let r;
    beforeAll(async function() {
      r = jackrabbit(process.env.RABBIT_URL);
      await promisify((done) => r.once("connected", done))();
    });
    it('emits a "close" event', async function() {
      r.close();
      await promisify((done) => r.once("close", done))();
    });
    it("clears the connection", function() {
      expect(r.getInternals().connection).toBeFalsy();
    });
  });
});
