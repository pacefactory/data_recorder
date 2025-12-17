import {MessageRecorder} from "./recording";
import {DataStore, IDataStore} from "../proto/DataStore";

export class MessageStore {
    dataStore: DataStore;

    constructor(dataStore: DataStore) {
        this.dataStore = dataStore;
    }

    static fromRecorder(recorder: MessageRecorder): MessageStore {
        const messagesByTopic: {[topic: string]: DataStore.IMqttMessageList} = {};

        // Ensure messages are sorted by timestamp
        for (const [topic, msgArray] of Object.entries(recorder.messagesByTopic)) {
            msgArray.sort(({frameTime: {epochMs: ems1}}, {frameTime: {epochMs: ems2}}) => ems1 - ems2);
            messagesByTopic[topic] = {mqttMessages: msgArray};
        }

        const dataStore: IDataStore = {messagesByTopic};

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
}
