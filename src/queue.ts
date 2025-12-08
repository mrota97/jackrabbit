import * as amqp from "amqplib/callback_api.js";
import { extend } from "lodash"
import { EventEmitter } from "events";

export interface QueueOptions {
    name?: string | undefined;
    exclusive?: boolean;
    durable?: boolean;
    prefetch?: number | undefined;
    messageTtl?: number | undefined;
    maxLength?: number | undefined;
    key?: string | undefined;
    keys?: string[] | undefined;
}

interface ConsumeOptions {
    consumerTag?: string | undefined;
    noAck?: boolean;
    exclusive?: boolean;
    priority?: number | undefined;
}

interface NackOptions {
    allUpTo?: boolean;
    requeue?: boolean;
}

interface QueueEvents {
    connected: [];
    ready: [];
    close: [err: unknown];
    consuming: []
    bound: [];
}

export type AckCallback = (reply?: unknown) => void;
export type NackCallback = (opts?: NackOptions) => void;
type ConsumeCallback = (data: unknown, ack: AckCallback, nack: NackCallback, message: amqp.Message) => void;

export class Queue extends EventEmitter<QueueEvents> {
    channel: amqp.Channel | undefined;
    consumerTag: string | undefined;
    name: string | undefined;
    options: QueueOptions;

    static DEFAULT_QUEUE_OPTIONS = {
        exclusive: false,
        durable: true,
        prefetch: 1, // can be set on the queue because we use a per-queue channel
        messageTtl: undefined,
        maxLength: undefined
    } satisfies QueueOptions;

    static DEFAULT_CONSUME_OPTIONS = {
        consumerTag: undefined,
        noAck: false,
        exclusive: false,
        priority: undefined
    } satisfies ConsumeOptions;

    constructor(options: QueueOptions) {
        super();
        this.name = options.name;
        this.options = options;
    }

    connect(connection: amqp.Connection) {
        connection.createChannel((err, channel) => {
            if (err) return this.bail(err);
            this.channel = channel;
            this.channel.prefetch(this.options.prefetch ?? 1);
            this.channel.on("close", this.bail.bind(this, new Error("Channel closed")));
            this.emit("connected");
            this.channel.assertQueue(this.name, this.options, (err, ok) => {
                if (err) return this.bail(err);
                this.name = ok.queue;
                this.emit("ready");
            })
        });
    }

    consume(callback: ConsumeCallback, options?: ConsumeOptions) {
        this.once("ready", () => {
            const opts = extend({}, Queue.DEFAULT_CONSUME_OPTIONS, options);
            this.channel!.consume(
                this.name!,
                onMessage.bind(this),
                opts,
                this.onConsume.bind(this)
            );
        })


        function onMessage(this: Queue, msg: amqp.Message | null) {
            if (!msg) return;
            const data = this.parseMessage(msg);
            if (!data) return;

            callback(data, ack.bind(this), nack.bind(this), msg);

            function ack(this: Queue, reply?: unknown) {
                const replyTo = msg?.properties.replyTo;
                const id = msg?.properties.correlationId;
                if (replyTo && id) {
                    const buffer = this.encodeMessage(reply, msg.properties.contentType);
                    this.channel!.publish("", replyTo, buffer, {
                        correlationId: id,
                        contentType: msg.properties.contentType
                    })
                }
                this.channel!.ack(msg!);
            }

            function nack(this: Queue, opts?: NackOptions) {
                this.channel!.nack(msg!, opts?.allUpTo ?? false, opts?.requeue ?? true);
            }
        }
    }

    cancel(done?: (err: any, ok: amqp.Replies.Empty) => void) {
        if (!this.consumerTag) return;
        if (!this.channel) return;
        this.channel.cancel(this.consumerTag, done);
    }

    purge(done: (err?: unknown, messageCount?: number) => void) {
        if (!this.channel || !this.name) return;
        this.channel.purgeQueue(this.name, (err, obj) => {
            if (err) this.bail(err);
            else done(undefined, obj.messageCount);
        })
    }

    encodeMessage(message: unknown, contentType: string): Buffer {
        if (contentType === "application/json") {
            return Buffer.from(JSON.stringify(message));
        } else if (Buffer.isBuffer(message)) {
            return message;
        } else {
            throw new Error("Unsupported message type for encoding");
        }
    }

    parseMessage(msg: amqp.Message) {
        if (msg.properties.contentType === "application/json") {
            return JSON.parse(msg.content.toString());
        } else {
            return msg.content;
        }
    }

    onConsume(err: unknown, info: amqp.Replies.Consume) {
        if (err) return this.bail(err);
        this.consumerTag = info.consumerTag; // required to stop consuming
        this.emit("consuming")
    }

    bail(err: unknown) {
        this.channel = undefined;
        this.name = undefined;
        this.consumerTag = undefined;
        this.emit("close", err);
    }
}

export default (options: QueueOptions = {}) => new Queue(options);