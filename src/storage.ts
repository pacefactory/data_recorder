import {MessageRecorder} from "./recording";
import {DataStore, IDataStore} from "../proto/DataStore";
import {downloadSnapshots} from "./snapshots";
import {toNumber} from "./utils";

export class MessageStore {
    dataStore: DataStore;

    constructor(dataStore: DataStore) {
        this.dataStore = dataStore;
    }

    static async fromRecorder(recorder: MessageRecorder, dbserver_base?: string): Promise<MessageStore> {
        const messagesByTopic: {[topic: string]: DataStore.IFrameDataList} = {};
        for (const [topic, msgArray] of Object.entries(recorder.messagesByTopic)) {
            messagesByTopic[topic] = {frameData: msgArray};
        }

        const snapshotsByCamera: {[cameraName: string]: DataStore.ISnapshotList} = {};
        if (dbserver_base !== undefined) {
            for await (const {timestamp, topic, imageData} of downloadSnapshots(dbserver_base, recorder)) {
                const cameraName = recorder.cameraNameByTopic[topic];
                const snapshotData = {epochMs: timestamp, image: imageData.read()};
                if (cameraName in snapshotsByCamera) {
                    snapshotsByCamera[cameraName].snapshot!.push(snapshotData);
                } else {
                    snapshotsByCamera[cameraName] = {snapshot: [snapshotData]};
                }
            }
        }

        const dataStore: IDataStore = {messagesByTopic, snapshotsByCamera};

        const verifyResult = DataStore.verify(dataStore);
        if (verifyResult !== null) {
            console.error("error: MessageStore.fromRecorder was called with bad data");
        }

        return new MessageStore(DataStore.fromObject(dataStore));
    }

    toProtobufEncoded(): Uint8Array {
        const encoded = DataStore.encode(this.dataStore).finish();
        return encoded;
    }

    static fromProtobufEncoded(encoded: Uint8Array): MessageStore {
        const dataStore = DataStore.decode(encoded);
        return new MessageStore(dataStore);
    }

    trim(startTimestampMs: number, endTimestampMs: number) {
        // Trim frame messages
        for (const [topic, message] of Object.entries(this.dataStore.messagesByTopic)) {
            const filteredMessage = message.frameData!.filter((fd) => {
                const epochMs = toNumber(fd.frameTime!.epochMs!);
                return epochMs >= startTimestampMs && epochMs <= endTimestampMs;
            });
            this.dataStore.messagesByTopic[topic].frameData = filteredMessage;
        }

        // Trim snapshots, if necessary
        for (const [camera, snapshots] of Object.entries(this.dataStore.snapshotsByCamera)) {
            const filteredSnapshots = snapshots.snapshot!.filter((sn) => {
                const epochMs = toNumber(sn.epochMs!);
                return epochMs >= startTimestampMs && epochMs <= endTimestampMs;
            });
            this.dataStore.snapshotsByCamera[camera].snapshot = filteredSnapshots;
        }
    }
}
