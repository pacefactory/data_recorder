import {Readable} from "node:stream";
import tar from "tar-stream";
import {stream} from "undici";
import {MessageRecorder} from "./recording";

function getDbserverSnapshotsUrl(dbserverBase: string, cameraId: string, startTime: number, endTime: number) {
    return `${dbserverBase}/${cameraId}/snapshots/get-many-images-tar/by-time-range/${startTime}/${endTime}`;
}

/**
 * Image filenames in tar archives from dbserver are in the form <timestamp>.jpg
 */
function unixTimestampFromFilename(filename: string): number {
    return parseInt(filename.slice(0, -4));
}

/**
 * Given a message recorder that has recorded messages, download all snapshots in the time range
 * of the recorded messages. Yields a readable stream containing binary image data for each snapshot.
 */
export async function* downloadSnapshots(
    dbserverBase: string,
    messageRecorder: MessageRecorder,
): AsyncGenerator<{
    timestamp: number;
    topic: string;
    imageData: Readable;
}> {
    for (const [topic, cameraName] of Object.entries(messageRecorder.cameraNameByTopic)) {
        const messages = messageRecorder.messagesByTopic[topic]!;
        const startTimeMs = messages[0].frameTime.epochMs;
        const endTimeMs = messages[messages.length - 1].frameTime.epochMs;
        const url = getDbserverSnapshotsUrl(dbserverBase, cameraName, startTimeMs, endTimeMs);
        console.log(url);

        const extract = tar.extract();
        const fut = stream(
            url,
            {
                method: "GET",
                opaque: extract,
            },
            ({opaque: extract}) => extract,
        );

        const extractedEntries = [];
        for await (const tarEntry of extract) {
            const filename = tarEntry.header.name;
            extractedEntries.push({timestamp: unixTimestampFromFilename(filename), topic, imageData: tarEntry});
            tarEntry.resume();
        }

        await fut;
        yield* extractedEntries;
    }
}
