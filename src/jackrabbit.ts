import * as amqp from "amqplib/callback_api.js";
import { EventEmitter } from "events";

import exchange, { type ExchangeOptions } from "./exchange.js"

interface JackrabbitOptions {

}

interface JackrabbitEvents {
    connected: [];
    error: [err: unknown];
    close: [];
}

export class Jackrabbit extends EventEmitter<JackrabbitEvents> {
    #connection: amqp.Connection | undefined;
    #channel: amqp.Channel | undefined;

    constructor() {
        super();
    }

    set connection(conn: amqp.Connection) {
        this.#connection = conn;
        this.#connection.on("close", this.#bail.bind(this));
        this.emit("connected");
    }

    getInternals() {
        return {
            amqp,
            connection: this.#connection
        }
    }

    close(callback?: (err: unknown) => void) {
        try {
            // I don't think amqplib should be throwing here, as this is an async function
            // TODO: figure out how to test whether or not amqplib will throw
            // (eg, how do they determine if closing is an illegal operation?)
            this.#connection?.close((err) => {
                callback?.(err);
                this.emit("close");
            });
        } catch (e) {
            console.log(e);
            callback?.(e);
        }
    }

    default = this.#createDefaultExchange.bind(this);
    #createDefaultExchange(options: ExchangeOptions = {}) {
        return this.createExchange("direct")("", options);
    }

    direct = this.createExchange("direct");
    topic = this.createExchange("topic");
    fanout = this.createExchange("fanout");

    createExchange(type: string) {
        return (name?: string, options?: ExchangeOptions) => {
            const newExchange = exchange(name, type, options);
            if (this.#connection) {
                newExchange.connect(this.#connection);
            } else {
                this.once("connected", () => {
                    newExchange.connect(this.#connection!);
                })
            }
            return newExchange;
        }
    }

    #bail(err: unknown) {
        this.#connection = undefined;
        this.#channel = undefined;
        this.emit("error", err);
    }
}

export default (url: string, options?: JackrabbitOptions) => {
    if (!url) throw new Error("url required for jackrabbit connection");

    const jackrabbit = new Jackrabbit();

    amqp.connect(url, options || {}, (err, conn) => {
        if (err) return jackrabbit.emit("error", err);
        jackrabbit.connection = conn;
    });

    return jackrabbit;
}