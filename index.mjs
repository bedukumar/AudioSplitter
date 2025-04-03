import { S3 } from "@aws-sdk/client-s3";
import path from "path";
import fs from "fs";
import os from "os";
import ffmpeg from "fluent-ffmpeg";
import { fileURLToPath } from "url";

const s3 = new S3({
  region: "ap-south-1",  //  Change this to match your bucket region
});

// Get __dirname equivalent in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configure FFmpeg path (pre-built layer in Lambda)
const FFMPEG_PATH = "/opt/bin/ffmpeg"; // Update with Lambda layer path
ffmpeg.setFfmpegPath(FFMPEG_PATH);

const BUCKET_A = "mixradio-obj-bucket";
const BUCKET_B = "mixradio-chunks-bucket";
const CHUNK_DURATION = 20; // seconds

export const handler = async (event) => {
  try {
    // Get file details from S3 event
    const record = event.Records[0];
    const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));
    const fileName = path.basename(key, ".m4a");

    console.log(`Processing file: ${key}`);

    // Download file from S3 Bucket A
    const tempFilePath = path.join(os.tmpdir(), `${fileName}.m4a`);
    const { Body } = await s3.getObject({ Bucket: BUCKET_A, Key: key });
    fs.writeFileSync(tempFilePath, await Body.transformToByteArray());

    // Generate output pattern for chunks
    const outputPattern = path.join(os.tmpdir(), `${fileName}_%03d.m4a`);

    // Run FFmpeg to split the file
    await new Promise((resolve, reject) => {
      ffmpeg(tempFilePath)
        .output(outputPattern)
        .outputOptions([
          "-f segment",
          `-segment_time ${CHUNK_DURATION}`,
          "-c copy",
          "-reset_timestamps 1",
        ])
        .on("error", (err) => reject(`FFmpeg Error: ${err.message}`))
        .on("end", resolve)
        .run();
    });

    // Upload chunks to S3 Bucket B
    const chunkFiles = fs.readdirSync(os.tmpdir()).filter((f) => f.startsWith(`${fileName}_`));
    let chunkDetails = [];

    for (const chunkFile of chunkFiles) {
      const chunkPath = path.join(os.tmpdir(), chunkFile);
      const chunkKey = `chunks/${chunkFile}`;

      await s3.putObject({
        Bucket: BUCKET_B,
        Key: chunkKey,
        Body: fs.createReadStream(chunkPath),
        ContentType: "audio/m4a",
      });

      chunkDetails.push({ chunkKey, size: fs.statSync(chunkPath).size });
      console.log(`Uploaded: ${chunkKey}`);
    }

    console.log("Chunk details:", JSON.stringify(chunkDetails, null, 2));

    return { status: "success", chunkDetails };
  } catch (error) {
    console.error("Processing failed:", error);
    return { status: "error", error };
  }
};
