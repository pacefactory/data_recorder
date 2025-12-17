export interface FrameTime {
    frameIndex: number;
    epochMs: number;
}

export interface StoredMessage {
    frameTime: FrameTime;
    messageBuffer: Buffer;
}
