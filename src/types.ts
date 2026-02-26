import {IFrameData} from "../proto/DataStore";

export interface FrameTime {
    frameIndex: number;
    epochMs: number;
}

export interface StoredMessage {
    frameTime: FrameTime;
    messageBuffer: Buffer;
}

function protobufNumberToNumber(n: number | Long): number {
    if (typeof n === "number") {
        return n;
    } else {
        return n.toNumber();
    }
}

export function frameDataToStoredMessage(fd: IFrameData): StoredMessage {
    return {
        frameTime: {
            frameIndex: protobufNumberToNumber(fd.frameTime!.frameIndex!),
            epochMs: protobufNumberToNumber(fd.frameTime!.epochMs!),
        },
        messageBuffer: Buffer.from(fd.messageBuffer!),
    };
}

export interface StoredMessageOnTopic {
    topic: string;
    storedMessage: StoredMessage;
}
