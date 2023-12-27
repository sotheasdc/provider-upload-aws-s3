"use strict";
const fp = require("lodash/fp");
const clientS3 = require("@aws-sdk/client-s3");
const s3RequestPresigner = require("@aws-sdk/s3-request-presigner");
const libStorage = require("@aws-sdk/lib-storage");
const ENDPOINT_PATTERN = /^(.+\.)?s3[.-]([a-z0-9-]+)\./;
function isUrlFromBucket(fileUrl, bucketName, baseUrl = "") {
  const url = new URL(fileUrl);
  if (baseUrl) {
    return false;
  }
  const { bucket } = getBucketFromAwsUrl(fileUrl);
  if (bucket) {
    return bucket === bucketName;
  }
  return url.host.startsWith(`${bucketName}.`) || url.pathname.includes(`/${bucketName}/`);
}
function getBucketFromAwsUrl(fileUrl) {
  const url = new URL(fileUrl);
  if (url.protocol === "s3:") {
    const bucket = url.host;
    if (!bucket) {
      return { err: `Invalid S3 url: no bucket: ${url}` };
    }
    return { bucket };
  }
  if (!url.host) {
    return { err: `Invalid S3 url: no hostname: ${url}` };
  }
  const matches = url.host.match(ENDPOINT_PATTERN);
  if (!matches) {
    return { err: `Invalid S3 url: hostname does not appear to be a valid S3 endpoint: ${url}` };
  }
  const prefix = matches[1];
  if (!prefix) {
    if (url.pathname === "/") {
      return { bucket: null };
    }
    const index2 = url.pathname.indexOf("/", 1);
    if (index2 === -1) {
      return { bucket: url.pathname.substring(1) };
    }
    if (index2 === url.pathname.length - 1) {
      return { bucket: url.pathname.substring(1, index2) };
    }
    return { bucket: url.pathname.substring(1, index2) };
  }
  return { bucket: prefix.substring(0, prefix.length - 1) };
}
const extractCredentials = (options) => {
  if (options.accessKeyId && options.secretAccessKey) {
    return {
      accessKeyId: options.accessKeyId,
      secretAccessKey: options.secretAccessKey
    };
  }
  if (options.s3Options?.accessKeyId && options.s3Options.secretAccessKey) {
    process.emitWarning(
      "Credentials passed directly to s3Options is deprecated and will be removed in a future release. Please wrap them inside a credentials object."
    );
    return {
      accessKeyId: options.s3Options.accessKeyId,
      secretAccessKey: options.s3Options.secretAccessKey
    };
  }
  if (options.s3Options?.credentials) {
    return {
      accessKeyId: options.s3Options.credentials.accessKeyId,
      secretAccessKey: options.s3Options.credentials.secretAccessKey
    };
  }
  return null;
};
const assertUrlProtocol = (url) => {
  return /^\w*:\/\//.test(url);
};
const getConfig = ({ baseUrl, rootPath, s3Options, ...legacyS3Options }) => {
  if (Object.keys(legacyS3Options).length > 0) {
    process.emitWarning(
      "S3 configuration options passed at root level of the plugin's providerOptions is deprecated and will be removed in a future release. Please wrap them inside the 's3Options:{}' property."
    );
  }
  const credentials = extractCredentials({ s3Options, ...legacyS3Options });
  const config = {
    ...s3Options,
    ...legacyS3Options,
    ...credentials ? { credentials } : {}
  };
  config.params.ACL = fp.getOr(clientS3.ObjectCannedACL.public_read, ["params", "ACL"], config);
  return config;
};
const index = {
  init({ baseUrl, rootPath, s3Options, ...legacyS3Options }) {
    const config = getConfig({ baseUrl, rootPath, s3Options, ...legacyS3Options });
    const s3Client = new clientS3.S3Client(config);
    const filePrefix = rootPath ? `${rootPath.replace(/\/+$/, "")}/` : "";
    const getFileKey = (file) => {
      const path = file.path ? `${file.path}/` : "";
      return `${filePrefix}${path}${file.hash}${file.ext}`;
    };
    // const uploadss = async (file, customParams = {}) => {
    //   const fileKey = getFileKey(file);
    //   const uploadObj = new libStorage.Upload({
    //     client: s3Client,
    //     params: {
    //       Bucket: config.params.Bucket,
    //       Key: fileKey,
    //       Body: file.stream || Buffer.from(file.buffer, "binary"),
    //       ACL: config.params.ACL,
    //       ContentType: file.mime,
    //       ...customParams
    //     }
    //   });
    //   const upload2 = await uploadObj.done();
    //   if (assertUrlProtocol(upload2.Location)) {
    //     file.url = baseUrl ? `${baseUrl}/${fileKey}` : upload2.Location;
    //   } else {
    //     file.url = `https://${upload2.Location}`;
    //   }
    // };
    const upload = async (file, customParams = {}) => {
      const fileKey = getFileKey(file);
      const { Bucket, ACL } = config.params;

      const params = {
        Bucket,
        Key: fileKey,
        ACL,
        ContentType: file.mime,
        ...customParams,
      };

      const buffer = file.stream ? await streamToBuffer(file.stream) : file.buffer;
      const partSize = 5 * 1024 * 1024; // 5 MB parts
      const numParts = Math.ceil(buffer.length / partSize);
      const uploadId = await initiateMultipartUpload(s3Client, params);

      try {
        const uploadPromises = [];

        for (let i = 0; i < numParts; i++) {
          const start = i * partSize;
          const end = Math.min(start + partSize, buffer.length);
          const partBuffer = buffer.slice(start, end);

          const uploadPartPromise = s3Client.send(
            new UploadPartCommand({
              Bucket,
              Key: fileKey,
              UploadId: uploadId,
              Body: partBuffer,
              PartNumber: i + 1,
            })
          );

          uploadPromises.push(uploadPartPromise);
        }

        const uploadPartResponses = await Promise.all(uploadPromises);

        const completedUploadResponse = await completeMultipartUpload(
          s3Client,
          Bucket,
          fileKey,
          uploadId,
          uploadPartResponses
        );

        const fileUrl = assertUrlProtocol(completedUploadResponse.Location)
          ? baseUrl
            ? `${baseUrl}/${fileKey}`
            : completedUploadResponse.Location
          : `https://${completedUploadResponse.Location}`;

        file.url = fileUrl;
      } catch (error) {
        console.error("Error uploading large file:", error);

        if (uploadId) {
          await abortMultipartUpload(s3Client, Bucket, fileKey, uploadId);
        }
      }
    };

    const initiateMultipartUpload = async (s3Client, params) => {
      const { UploadId } = await s3Client.send(new CreateMultipartUploadCommand(params));
      return UploadId;
    };

    const completeMultipartUpload = async (s3Client, Bucket, Key, UploadId, uploadPartResponses) => {
      return s3Client.send(
        new CompleteMultipartUploadCommand({
          Bucket,
          Key,
          UploadId,
          MultipartUpload: {
            Parts: uploadPartResponses.map(({ ETag }, i) => ({
              ETag,
              PartNumber: i + 1,
            })),
          },
        })
      );
    };

    const abortMultipartUpload = async (s3Client, Bucket, Key, UploadId) => {
      await s3Client.send(
        new AbortMultipartUploadCommand({
          Bucket,
          Key,
          UploadId,
        })
      );
    };

    const streamToBuffer = async (stream) => {
      const chunks = [];
      for await (const chunk of stream) {
        chunks.push(chunk);
      }
      return Buffer.concat(chunks);
    };

    
    return {
      isPrivate() {
        return config.params.ACL === "private";
      },
      async getSignedUrl(file, customParams) {
        if (!isUrlFromBucket(file.url, config.params.Bucket, baseUrl)) {
          return { url: file.url };
        }
        const fileKey = getFileKey(file);
        const url = await s3RequestPresigner.getSignedUrl(
          s3Client,
          new clientS3.GetObjectCommand({
            Bucket: config.params.Bucket,
            Key: fileKey,
            ...customParams
          }),
          {
            expiresIn: fp.getOr(15 * 60, ["params", "signedUrlExpires"], config)
          }
        );
        return { url };
      },
      uploadStream(file, customParams = {}) {
        return upload(file, customParams);
      },
      upload(file, customParams = {}) {
        return upload(file, customParams);
      },
      delete(file, customParams = {}) {
        const command = new clientS3.DeleteObjectCommand({
          Bucket: config.params.Bucket,
          Key: getFileKey(file),
          ...customParams
        });
        return s3Client.send(command);
      }
    };
  }
};
module.exports = index;
//# sourceMappingURL=index.js.map
