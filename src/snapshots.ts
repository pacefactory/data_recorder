import tar from "tar-stream";
import {stream} from "undici";
import {MessageRecorder} from "./recording";

function getDbserverSnapshotsUrl(dbserverBase: string, cameraId: string, startTime: number, endTime: number) {
    return new URL(`${cameraId}/snapshots/get-many-images-tar/by-time-range/${startTime}/${endTime}`, dbserverBase);
}

function getDbserverCameraInfoUrl(dbserverBase: string, cameraId: string, startTime: number) {
    return new URL(`${cameraId}/camerainfo/get-active-metadata/by-time-target/${startTime}`, dbserverBase);
}

function getDbserverConfigInfoUrl(dbserverBase: string, cameraId: string, startTime: number) {
    return new URL(`${cameraId}/configinfo/get-active-metadata/by-time-target/${startTime}`, dbserverBase);
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
    imageData: Buffer;
}> {
    for (const [topic, cameraName] of Object.entries(messageRecorder.cameraNameByTopic)) {
        const messages = messageRecorder.messagesByTopic[topic]!;
        const startTimeMs = messages[0].frameTime.epochMs;
        const endTimeMs = messages[messages.length - 1].frameTime.epochMs;
        const url = getDbserverSnapshotsUrl(dbserverBase, cameraName, startTimeMs, endTimeMs);

        const extract = tar.extract();
        const fut = stream(
            url,
            {
                method: "GET",
                opaque: extract,
            },
            ({opaque: extract}) => extract,
        );

        for await (const tarEntry of extract) {
            const filename = tarEntry.header.name;
            const chunks: Buffer[] = [];
            for await (const chunk of tarEntry) {
                chunks.push(chunk);
            }
            const buf = Buffer.concat(chunks);
            yield {timestamp: unixTimestampFromFilename(filename), topic, imageData: buf};
            tarEntry.resume();
        }

        await fut;
    }
}

/**
 * Fetch and return the active camera info and config info for the given message recorder, in string format (without parsing), by camera ID.
 */
export async function getCameraInfoAndConfigInfo(
    dbserverBase: string,
    messageRecorder: MessageRecorder,
): Promise<Map<string, {cameraInfo: string; configInfo: string}>> {
    const cameraMetadataByCameraId = new Map();
    const visitedCameras = new Set();
    for (const [topic, cameraName] of Object.entries(messageRecorder.cameraNameByTopic)) {
        if (visitedCameras.has(cameraName)) {
            continue;
        }
        visitedCameras.add(cameraName);

        const messages = messageRecorder.messagesByTopic[topic]!;
        const startTimeMs = messages[0].frameTime.epochMs;
        const [cameraInfo, configInfo] = await Promise.all([
            fetch(getDbserverCameraInfoUrl(dbserverBase, cameraName, startTimeMs)).then((resp) => resp.text()),
            fetch(getDbserverConfigInfoUrl(dbserverBase, cameraName, startTimeMs)).then((resp) => resp.text()),
        ]);
        cameraMetadataByCameraId.set(cameraName, {cameraInfo, configInfo});
    }

    return cameraMetadataByCameraId;
}
