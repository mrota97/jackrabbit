import * as amqp from "amqplib/callback_api.js";
import { extend } from "lodash"
import { EventEmitter } from "events";
import { v4 } from "uuid"

import queue, { type Queue, type QueueOptions, type AckCallback, type NackCallback } from "./queue.js"

export interface ExchangeOptions {
    durable?: boolean;
    internal?: boolean;
    autoDelete?: boolean;
    alternateExchange?: string | undefined;
    noReply?: boolean;
}

interface PublishOptions extends amqp.Options.Publish {
    key?: string;
    noAck?: boolean;
    reply?: (data: unknown) => void;
};


type DefaultExchangeTypes = "direct" | "fanout" | "topic";
function isDefaultType(type: string): type is DefaultExchangeTypes {
    return ["direct", "fanout", "topic"].includes(type);
}
type DefaultExchangeNames = "amq.direct" | "amq.fanout" | "amq.topic";
function isDefault(name: string, type: string): name is DefaultExchangeNames {
    return isDefaultType(type) && Exchange.DEFAULT_EXCHANGES[type] === name;
}

function isNameless(name: string | undefined) {
    return name === "";
}

interface ExchangeEvents {
    ready: [];
    drain: [];
    close: [err: unknown];
    connected: [];
}

export class Exchange extends EventEmitter<ExchangeEvents> {
    static DEFAULT_EXCHANGES = {
        direct: "amq.direct",
        fanout: "amq.fanout",
        topic: "amq.topic"
    }

    static DEFAULT_EXCHANGE_OPTIONS = {
        durable: true,
        internal: false,
        autoDelete: false,
        alternateExchange: undefined,
        noReply: false
    } satisfies ExchangeOptions;

    static DEFAULT_PUBLISH_OPTIONS = {
        contentType: "application/json",
        mandatory: false,
        persistent: false,
        expiration: undefined,
        userId: undefined,
        CC: undefined,
        BCC: undefined
    } satisfies PublishOptions;

    name: string;
    type: string;
    options: ExchangeOptions & typeof Exchange.DEFAULT_EXCHANGE_OPTIONS;

    #ready = false;
    #connection: amqp.Connection | undefined;
    #channel: amqp.Channel | undefined;
    #publishing = 0;
    #replyQueue: Queue | null;
    #pendingReplies: Record<string, (data: unknown) => void> = {};

    constructor(name: string | undefined, type: string, options: ExchangeOptions = {}) {
        super();
        if (!type) {
            throw new Error("missing exchange type");
        }

        let _name = name;
        if (!isNameless(_name)) {
            _name = name || (isDefaultType(type) ? Exchange.DEFAULT_EXCHANGES[type] : "");
            if (!_name) {
                throw new Error("missing exchange name");
            }
        }

        this.name = _name;
        this.type = type;
        this.options = extend({}, Exchange.DEFAULT_EXCHANGE_OPTIONS, options);
        this.#replyQueue = this.options.noReply ? null : queue({ exclusive: true });
    }

    connect(con: amqp.Connection) {
        this.#connection = con;
        this.#connection.createChannel(this.onChannel.bind(this));
        if (this.#replyQueue) {
            this.#replyQueue.on("close", this.bail.bind(this));
            this.#replyQueue.consume(this.onReply.bind(this), { noAck: true });
        }
        return this;
    }

    queue = this.#createQueue;
    #createQueue(options?: QueueOptions) {
        const newQueue = queue(options);
        newQueue.on("close", this.bail.bind(this));
        newQueue.once("ready", () => {
            // the default exchange has implicit bindings to all queues
            if (!isNameless(this.name)) {
                const keys = (options?.keys || [options?.key]).filter(Boolean) as string[];
                this.#bindKeys(newQueue, keys)
                    .then(() => {
                        newQueue.emit("bound");
                    })
                    .catch(this.bail.bind(this));
            }
        });

        if (this.#connection) {
            newQueue.connect(this.#connection);
        } else {
            this.once("ready", () => {
                newQueue.connect(this.#connection!);
            });
        }

        return newQueue;
    }

    async #bindKeys(newQueue: Queue, keys: string[]) {
        const bindKey = (key: string) => new Promise<amqp.Replies.Empty>((resolve, reject) => {
            this.#channel!.bindQueue(
                newQueue.name!,
                this.name,
                key,
                {},
                (err, ok) => {
                    if (err) return reject(err);
                    resolve(ok);
                }
            );
        });
        return Promise.all(keys.map(bindKey));
    }

    publish(message: unknown, options: PublishOptions = {}) {
        this.#publishing++;
        if (this.#ready) sendMessage.call(this);
        else this.once("ready", sendMessage.bind(this));
        return this;

        function sendMessage(this: Exchange) {
            // TODO: better blacklisting/whitelisting of properties
            const opts = extend({}, Exchange.DEFAULT_PUBLISH_OPTIONS, options);
            const msg = this.encodeMessage(message, opts.contentType);
            if (opts.reply) {
                opts.replyTo = this.#replyQueue!.name;
                opts.correlationId = v4();
                this.#pendingReplies[opts.correlationId] = opts.reply;
                delete opts.reply;
            }

            const drained = this.#channel!.publish(
                this.name,
                opts.key!,
                Buffer.isBuffer(msg) ? msg : Buffer.from(msg),
                opts
            );
            if (drained) this.onDrain();
        }
    }

    encodeMessage(message: unknown, contentType: string | undefined) {
        if (contentType === "application/json") return JSON.stringify(message);
        return message;
    }

    onReply(data: unknown, ack: AckCallback, nack: NackCallback, msg: amqp.Message) {
        this.#pendingReplies[msg.properties.correlationId]?.(data);
    }

    bail(err: unknown) {
        this.#connection = undefined;
        this.#channel = undefined;
        this.emit("close", err);
    }

    onDrain() {
        setImmediate(() => {
            this.#publishing--;
            if (this.#publishing === 0) {
                this.emit("drain");
            }
        })
    }

    onChannel(err: unknown, chan: amqp.Channel) {
        if (err) return this.bail(err);
        this.#channel = chan;
        this.#channel.on("close", this.bail.bind(this, new Error("Channel closed")));
        this.#channel.on("drain", this.onDrain.bind(this));
        this.emit("connected");
        if (isDefault(this.name, this.type) || isNameless(this.name)) {
            return this.onExchange(undefined, { exchange: this.name });
        }
        this.#channel.assertExchange(
            this.name,
            this.type,
            this.options,
            this.onExchange.bind(this)
        );
    }

    onExchange(err: unknown, info: amqp.Replies.AssertExchange) {
        if (err) return this.bail(err);

        const isReady = () => {
            this.#ready = true;
            this.emit("ready");
        }

        if (this.#replyQueue) {
            this.#replyQueue.connect(this.#connection!);
            this.#replyQueue.once("ready", isReady);
        } else {
            isReady();
        }
    }
}

export default (name: string | undefined, type: string, options: ExchangeOptions = {}) => new Exchange(name, type, options);