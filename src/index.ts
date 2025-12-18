import mqtt from "mqtt";
import {parseArgs} from "util";
import {recordData} from "./recording";
import {MessageStore} from "./storage";
import {open} from "fs/promises";
import {MessageSequence} from "./playback";

async function runCLIRecord(args: string[]) {
    const parsedArgs = parseArgs({
        args,
        options: {
            duration: {
                type: "string",
                short: "d",
            },
            camera: {
                type: "string",
                short: "c",
                multiple: true,
            },
            mqtt_url: {
                type: "string",
                short: "u",
            },
            out: {
                type: "string",
                short: "o",
            },
        },
    });

    if (
        parsedArgs.values.duration === undefined ||
        parsedArgs.values.camera === undefined ||
        parsedArgs.values.mqtt_url === undefined ||
        parsedArgs.values.out === undefined
    ) {
        console.error("Required options: duration, camera, mqtt_url, out");
        process.exit(1);
    }
    const duration = parseInt(parsedArgs.values.duration);
    const cameras = parsedArgs.values.camera;

    const mqttClient = await mqtt.connectAsync(parsedArgs.values.mqtt_url);
    const messageRecorder = await recordData(mqttClient, cameras, duration);
    const messageStore = MessageStore.fromRecorder(messageRecorder);
    const buffer = messageStore.toProtobufEncoded();

    mqttClient.endAsync();

    const file = await open(parsedArgs.values.out, "w");
    await file.write(buffer);
    await file.close();
}

async function runCLIPlayback(args: string[]) {
    const parsedArgs = parseArgs({
        args,
        options: {
            in: {
                type: "string",
                short: "i",
            },
            mqtt_url: {
                type: "string",
                short: "u",
            },
        },
    });

    if (parsedArgs.values.in === undefined || parsedArgs.values.mqtt_url === undefined) {
        console.error("Required options: in, mqtt_url");
        process.exit(1);
    }

    const file = await open(parsedArgs.values.in, "r");
    const {buffer} = await file.readFile();
    await file.close();

    const messageStore = MessageStore.fromProtobufEncoded(Buffer.from(buffer));
    const messageSequence = MessageSequence.fromMessageStore(messageStore);

    const mqttClient = await mqtt.connectAsync(parsedArgs.values.mqtt_url);

    await messageSequence.mqttPlayback(mqttClient);

    mqttClient.endAsync();
}

async function runCLI() {
    const [, , mode, ...args] = process.argv;
    if (mode === "record") {
        await runCLIRecord(args);
    } else if (mode === "playback") {
        await runCLIPlayback(args);
    } else {
        console.error("Must be run with mode `record` or `playback`.");
    }
}

if (require.main === module) {
    runCLI();
}
