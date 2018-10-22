import * as test from "blue-tape";
import { PassThrough } from "stream";
import * as util from "util";
import { streamWait } from "./stream-wait";

test("stream-wait", async (t) => {
    const stream = new PassThrough({ objectMode: true });

    const write = util.promisify(stream.write.bind(stream));
    const end = util.promisify(stream.end.bind(stream));

    const wait = streamWait<string>(stream, (chunk) => chunk === "bb");

    await write("aa");
    await write("bb");

    t.equal(await wait, "bb");

    await end();
});
