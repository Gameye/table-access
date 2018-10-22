import { PassThrough, Readable } from "stream";

export function streamWait<T = any>(
    stream: Readable,
    waitFor?: (chunk: T) => boolean,
) {
    return new Promise((resolve, reject) => {
        let resolveValue: T | undefined;
        const onClose = () => {
            pipeStream.removeListener("close", onClose);
            stream.removeListener("close", onClose);

            resolve(resolveValue);
        };

        const pipeStream = new PassThrough({
            objectMode: true,
            highWaterMark: 0,
        });
        pipeStream.addListener("error", reject);
        pipeStream.addListener("unpipe", () => pipeStream.destroy());
        if (waitFor) {
            pipeStream.on("readable", () => {
                const chunk = pipeStream.read();
                if (chunk === null) return;
                if (!waitFor(chunk)) return;
                resolveValue = chunk;
                stream.unpipe(pipeStream);
            });
        }

        pipeStream.addListener("close", onClose);
        stream.addListener("close", onClose);

        stream.pipe(pipeStream);
    });
}
