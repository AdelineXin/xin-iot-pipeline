import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";

const REGION = process.env.AWS_REGION || "eu-north-1";

const ddbClient = new DynamoDBClient({ region: REGION });
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);
const sqsClient = new SQSClient({ region: REGION });

// 从环境变量读取资源，避免硬编码！(高度符合生产标准 👈)
const TABLE_NAME = process.env.TABLE_NAME;
const SQS_QUEUE_URL = process.env.SQS_QUEUE_URL;

export const handler = async (event) => {
    // 处理从 IoT Core 直接传入的数据（通常会被包装在 records 里，或者如果是直接触发则直接是 event）
    const telemetryData = event.Records ? JSON.parse(event.Records[0].body) : event;
    console.log("🚗 [IaC 云端收到遥测数据]：", JSON.stringify(telemetryData, null, 2));

    const { vehicle_id, timestamp, location, speed_kmh, status } = telemetryData;

    if (!vehicle_id || !timestamp) {
        console.error("❌ 数据校验失败：缺少主键");
        return { statusCode: 400, body: "Invalid data" };
    }

    try {
        // 1. 写入 DynamoDB
        console.log(`💾 正在写入 DynamoDB...`);
        await ddbDocClient.send(new PutCommand({
            TableName: TABLE_NAME,
            Item: {
                vehicle_id: vehicle_id,
                timestamp: Number(timestamp),
                latitude: location?.lat,
                longitude: location?.lon,
                speed_kmh: Number(speed_kmh),
                status: status
            }
        }));

        // 2. 异常报警推送到 SQS
        if (speed_kmh > 120 || status === "collision") {
            console.warn(`🚨 警告：车辆状态异常！准备触发报警...`);
            const alertPayload = {
                alert_type: status === "collision" ? "CRASH_DETECTION" : "SPEEDING_ALERT",
                vehicle_id: vehicle_id,
                timestamp: timestamp,
                speed_kmh: speed_kmh,
                location: location,
                emitted_at: new Date().toISOString()
            };

            await sqsClient.send(new SendMessageCommand({
                QueueUrl: SQS_QUEUE_URL,
                MessageBody: JSON.stringify(alertPayload)
            }));
            console.log("✅ SQS 报警发送成功！");
        }

        return { statusCode: 200, body: "Processed successfully" };
    } catch (error) {
        console.error("❌ 发生异常:", error);
        return { statusCode: 500, body: error.message };
    }
};
