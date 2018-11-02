import { PassThrough, Readable } from "stream";

export async function streamWait<T = any>(
    stream: Readable,
    waitFor?: (chunk: T) => boolean,
) {
    const waitStream = new PassThrough({
        // highWaterMark: 0,
        objectMode: true,
    });

    const resultPromise = new Promise<T | void>(
        resolve => {
            if (waitFor) waitStream.on("data", (chunk: T) => {
                if (!waitFor(chunk)) return;
                resolve(chunk);
            });
            waitStream.on("end", () => {
                resolve(undefined);
            });
        },
    );

    const closePromise = new Promise(
        (resolve, reject) => waitStream.
            on("close", resolve).
            on("error", reject),
    );

    stream.pipe(waitStream);
    waitStream.resume();

    const result = await resultPromise;

    stream.unpipe(waitStream);
    waitStream.destroy();

    await closePromise;

    return result;
}
