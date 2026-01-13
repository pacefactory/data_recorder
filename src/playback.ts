import mqtt from "mqtt";
import {MessageStore} from "./storage";
import {StoredMessage, StoredMessageOnTopic} from "./types";
import {setTimeout} from "node:timers/promises";

export class MessageSequence {
    messages: StoredMessageOnTopic[];

    constructor(messages: StoredMessageOnTopic[]) {
        this.messages = messages;
    }

    static fromMessageStore(messageStore: MessageStore): MessageSequence {
        const messages = [];
        for (const [topic, {frameData}] of Object.entries(messageStore.dataStore.messagesByTopic)) {
            for (const msg of frameData!) {
                messages.push({topic, storedMessage: msg as StoredMessage});
            }
        }

        messages.sort(
            (
                {
                    storedMessage: {
                        frameTime: {epochMs: ems1},
                    },
                },
                {
                    storedMessage: {
                        frameTime: {epochMs: ems2},
                    },
                },
            ) => ems1 - ems2,
        );

        return new MessageSequence(messages);
    }

    async mqttPlayback(mqttClient: mqtt.MqttClient) {
        for (const {topic, storedMessage} of this.messages) {
            await mqttClient.publishAsync(topic, storedMessage.messageBuffer, {qos: 2});
            await setTimeout(20);
        }
    }
}
