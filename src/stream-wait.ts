import { Readable } from "stream";

/**
 * Read stream until the waitFor function returns true, then
 * pause the stream.
 * @param stream stream to read
 * @param waitFor return true to resolve
 */
export async function streamWait<T = any>(
    stream: Readable,
    waitFor: (chunk: T) => boolean,
) {
    return new Promise<T>(resolve => {
        stream.addListener("data", onData);
        stream.resume();
        function onData(chunk: T) {
            if (waitFor(chunk)) {
                stream.pause();
                stream.removeListener("data", onData);
                resolve(chunk);
            }
        }
    });
}
