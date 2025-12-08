import jackrabbit from "../../src/jackrabbit.ts";

var rabbit = jackrabbit(process.env.RABBIT_URL);
var exchange = rabbit.fanout();

exchange.publish("this is a log");
exchange.on("drain", process.exit);
