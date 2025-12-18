import mqtt from "mqtt";
import {MessageStore} from "./storage";
import {StoredMessage, StoredMessageOnTopic} from "./types";

export class MessageSequence {
    messages: StoredMessageOnTopic[];

    constructor(messages: StoredMessageOnTopic[]) {
        this.messages = messages;
    }

    static fromMessageStore(messageStore: MessageStore): MessageSequence {
        const messages = [];
        for (const [topic, {mqttMessages}] of Object.entries(messageStore.dataStore.messagesByTopic)) {
            for (const msg of mqttMessages!) {
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
            await mqttClient.publishAsync(topic, storedMessage.messageBuffer);
        }
    }
}
