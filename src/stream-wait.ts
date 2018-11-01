import { PassThrough, Readable } from "stream";

export function streamWait<T = any>(
    stream: Readable,
    waitFor?: (chunk: T) => boolean,
) {
    return new Promise<T>((resolve, reject) => {
        let resolveValue: T | undefined;
        const onClose = () => {
            pipeStream.removeListener("close", onClose);
            stream.removeListener("close", onClose);

            resolve(resolveValue);
        };

        const pipeStream = new PassThrough({
            // highWaterMark: 0,
            objectMode: true,
        });
        pipeStream.addListener("error", reject);
        pipeStream.addListener("unpipe", () => pipeStream.destroy());
        if (waitFor) {
            const onData = (chunk: T) => {
                if (!waitFor(chunk)) return;
                pipeStream.removeListener("data", onData);
                resolveValue = chunk;
                stream.unpipe(pipeStream);
            };
            pipeStream.addListener("data", onData);

        }

        pipeStream.addListener("close", onClose);
        stream.addListener("close", onClose);

        stream.pipe(pipeStream);
    });
}
