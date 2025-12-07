var amqp = require("amqplib/callback_api");
var _ = require("lodash");
var EventEmitter = require("node:events").EventEmitter;

/**
 * @typedef QueueOptions
 * @prop {boolean} [exclusive]
 * @prop {boolean} [durable]
 * @prop {number | undefined} [prefetch]
 * @prop {number | undefined} [messageTtl]
 * @prop {number | undefined} [maxLength]
 */

/** @type {QueueOptions} */
var DEFAULT_QUEUE_OPTIONS = {
  exclusive: false,
  durable: true,
  prefetch: 1, // can be set on the queue because we use a per-queue channel
  messageTtl: undefined,
  maxLength: undefined
};

/**
 * @typedef ConsumeOptions
 * @prop {string | undefined} [consumerTag]
 * @prop {boolean} [noAck]
 * @prop {boolean} [exclusive]
 * @prop {number | undefined} [priority]
 */

/** @type {ConsumeOptions} */
var DEFAULT_CONSUME_OPTIONS = {
  consumerTag: undefined,
  noAck: false,
  exclusive: false,
  priority: undefined
};

/**
 * @typedef NackOptions
 * @prop {boolean} [allUpTo]
 * @prop {boolean} [requeue]
 */

/**
 * @typedef {(reply?: any) => void} AckCallback
 * @typedef {(opts: NackOptions) => void} NackCallback
 */

/**
 * @typedef {(data: *, ack: AckCallback, nack: NackCallback, msg: amqp.Message) => void} CallbackFn
 */


module.exports = queue;

/**
 * 
 * @param {*} options 
 * @returns 
 */
function queue(options) {
  options = options || {};
  /** @type {amqp.Channel} */
  var channel;
  /** @type {string} */
  var consumerTag;
  var emitter = _.extend(new EventEmitter(), {
    name: options.name,
    options: _.extend({}, DEFAULT_QUEUE_OPTIONS, options),
    connect: connect,
    consume: consume,
    cancel: cancel,
    purge: purge
  });

  return emitter;

  /**
   * 
   * @param {amqp.Connection} connection 
   */
  function connect(connection) {
    connection.createChannel(onChannel);
  }

  /**
   * 
   * @param {CallbackFn} callback 
   * @param {ConsumeOptions} options 
   */
  function consume(callback, options) {
    emitter.once("ready", function() {
      var opts = _.extend({}, DEFAULT_CONSUME_OPTIONS, options);
      channel.consume(emitter.name, onMessage, opts, onConsume);
    });

    /**
     * 
     * @param {amqp.Message | null} msg 
     * @returns 
     */
    function onMessage(msg) {
      var data = parseMessage(msg);
      if (!data) return;

      callback(data, ack, nack, msg);

      /**
       * 
       * @param {*} reply 
       */
      function ack(reply) {
        var replyTo = msg.properties.replyTo;
        var id = msg.properties.correlationId;
        if (replyTo && id) {
          var buffer = encodeMessage(reply, msg.properties.contentType);
          channel.publish("", replyTo, buffer, {
            correlationId: id,
            contentType: msg.properties.contentType
          });
        }
        channel.ack(msg);
      }

      /**
       * 
       * @param {NackOptions} opts 
       */
      function nack(opts) {
        opts = opts || {};
        opts.allUpTo = opts.allUpTo !== undefined ? opts.allUpTo : false;
        opts.requeue = opts.requeue !== undefined ? opts.requeue : true;
        channel.nack(msg, opts.allUpTo, opts.requeue);
      }
    }
  }

  /**
   * 
   * @param {*} done 
   * @returns 
   */
  function cancel(done) {
    if (!consumerTag) return;
    if (!channel) return;
    channel.cancel(consumerTag, done);
  }

  /**
   * 
   * @param {(err?: *, messageCount?: number) => void} done 
   * @returns 
   */
  function purge(done) {
    if (!channel) return;
    channel.purgeQueue(emitter.name, onPurged);

    /**
     * 
     * @param {*} err 
     * @param {amqp.Replies.PurgeQueue} obj 
     */
    function onPurged(err, obj) {
      if (err) return done(err);
      done(undefined, obj.messageCount);
    }
  }

  /**
   * 
   * @param {*} message 
   * @param {string} contentType 
   * @returns 
   */
  function encodeMessage(message, contentType) {
    if (contentType === "application/json") {
      return new Buffer(JSON.stringify(message));
    }
    return new Buffer(message.toString());
  }

  /**
   * 
   * @param {amqp.Message} msg 
   * @returns
   */
  function parseMessage(msg) {
    if (msg.properties.contentType === "application/json") {
      try {
        return JSON.parse(msg.content.toString());
      } catch (e) {
        emitter.emit("error", new Error("unable to parse message as JSON"));
        return;
      }
    }
    return msg.content;
  }

  /**
   * 
   * @param {*} err 
   * @param {amqp.Replies.Consume} info 
   * @returns 
   */
  function onConsume(err, info) {
    if (err) return bail(err);
    consumerTag = info.consumerTag; // required to stop consuming
    emitter.emit("consuming");
  }

  /**
   * 
   * @param {*} err 
   */
  function bail(err) {
    // TODO: close the channel if still open
    channel = undefined;
    emitter.name = undefined;
    consumerTag = undefined;
    emitter.emit("close", err);
  }

  /**
   * 
   * @param {*} err 
   * @param {amqp.Channel} chan 
   * @returns 
   */
  function onChannel(err, chan) {
    if (err) return bail(err);
    channel = chan;
    channel.prefetch(emitter.options.prefetch);
    channel.on("close", bail.bind(this, new Error("channel closed")));
    emitter.emit("connected");
    channel.assertQueue(emitter.name, emitter.options, onQueue);
  }

  /**
   * 
   * @param {*} err 
   * @param {amqp.Replies.AssertQueue} info 
   * @returns 
   */
  function onQueue(err, info) {
    if (err) return bail(err);
    emitter.name = info.queue;
    emitter.emit("ready");
  }
}
