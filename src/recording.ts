import mqtt from "mqtt";
import {setTimeout} from "timers/promises";
import {FrameTime, StoredMessage} from "./types";

export class MessageRecorder {
    messagesByTopic: {[topic: string]: StoredMessage[]};

    constructor() {
        this.messagesByTopic = {};
    }

    /**
     * Messages are stored as the raw bytestrings that are received, but we do care about their
     * timestamp info, so we need to extract that here.
     * Returns an object combining the message bytestring and extracted time info, or null
     * if that time info does not exist.
     */
    static bufferToStoredMessage(topic: string, message: Buffer): StoredMessage | null {
        const {frame_index, epoch_ms} = JSON.parse(message.toString());
        if (frame_index === undefined || epoch_ms === undefined) {
            return null;
        }
        const frameTime: FrameTime = {
            frameIndex: frame_index,
            epochMs: epoch_ms,
        };
        return {frameTime, messageBuffer: message};
    }

    recordMessage(topic: string, message: Buffer) {
        let messageArray = this.messagesByTopic[topic];
        if (messageArray === undefined) {
            messageArray = [];
            this.messagesByTopic[topic] = messageArray;
        }

        const storedMessage = MessageRecorder.bufferToStoredMessage(topic, message);
        if (storedMessage === null) {
            console.error(`Could not parse message on topic ${topic}: `, message.toString());
            return;
        }
        messageArray.push(storedMessage);
    }
}

function getCameraFrameTopic(cameraName: string): string {
    return `realtime/${cameraName}/frame`;
}

export async function recordData(
    mqttClient: mqtt.MqttClient,
    cameraNames: string[],
    durationMs: number,
): Promise<MessageRecorder> {
    const messageRecorder = new MessageRecorder();
    const topics = cameraNames.map(getCameraFrameTopic);
    await mqttClient.subscribeAsync(topics);
    mqttClient.on("message", (topic, message) => messageRecorder.recordMessage(topic, message));

    // Pause this thread while the MQTT clent receives messages
    await setTimeout(durationMs);

    mqttClient.removeAllListeners();
    await mqttClient.unsubscribeAsync(topics);
    return messageRecorder;
}
