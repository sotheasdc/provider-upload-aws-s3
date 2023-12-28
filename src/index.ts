import type { ReadStream } from 'node:fs';
import { getOr } from 'lodash/fp';
import {
  S3Client,
  GetObjectCommand,
  DeleteObjectCommand,
  DeleteObjectCommandOutput,
  PutObjectCommandInput,
  CompleteMultipartUploadCommandOutput,
  AbortMultipartUploadCommandOutput,
  S3ClientConfig,
  ObjectCannedACL,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
} from '@aws-sdk/client-s3';
import type { AwsCredentialIdentity } from '@aws-sdk/types';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { Upload } from '@aws-sdk/lib-storage';
import { extractCredentials, isUrlFromBucket } from './utils';

export interface File {
  name: string;
  alternativeText?: string;
  caption?: string;
  width?: number;
  height?: number;
  formats?: Record<string, unknown>;
  hash: string;
  ext?: string;
  mime: string;
  size: number;
  url: string;
  previewUrl?: string;
  path?: string;
  provider?: string;
  provider_metadata?: Record<string, unknown>;
  stream?: ReadStream;
  buffer?: Buffer;
}

export type UploadCommandOutput = (
  | CompleteMultipartUploadCommandOutput
  | AbortMultipartUploadCommandOutput
) & {
  Location: string;
};

export interface AWSParams {
  Bucket: string; // making it required
  ACL?: ObjectCannedACL;
  signedUrlExpires?: number;
}

export interface DefaultOptions extends S3ClientConfig {
  // TODO Remove this in V5
  accessKeyId?: AwsCredentialIdentity['accessKeyId'];
  secretAccessKey?: AwsCredentialIdentity['secretAccessKey'];
  // Keep this for V5
  credentials?: AwsCredentialIdentity;
  params?: AWSParams;
  [k: string]: any;
}

export type InitOptions = (DefaultOptions | { s3Options: DefaultOptions }) & {
  baseUrl?: string;
  rootPath?: string;
  [k: string]: any;
};

const assertUrlProtocol = (url: string) => {
  // Regex to test protocol like "http://", "https://"
  return /^\w*:\/\//.test(url);
};

const getConfig = ({ baseUrl, rootPath, s3Options, ...legacyS3Options }: InitOptions) => {
  if (Object.keys(legacyS3Options).length > 0) {
    process.emitWarning(
      "S3 configuration options passed at root level of the plugin's providerOptions is deprecated and will be removed in a future release. Please wrap them inside the 's3Options:{}' property."
    );
  }
  const credentials = extractCredentials({ s3Options, ...legacyS3Options });
  const config = {
    ...s3Options,
    ...legacyS3Options,
    ...(credentials ? { credentials } : {}),
  };

  config.params.ACL = getOr(ObjectCannedACL.public_read, ['params', 'ACL'], config);

  return config;
};

export default {
  init({ baseUrl, rootPath, s3Options, ...legacyS3Options }: InitOptions) {
    // TODO V5 change config structure to avoid having to do this
    const config = getConfig({ baseUrl, rootPath, s3Options, ...legacyS3Options });
    const s3Client = new S3Client(config);
    const filePrefix = rootPath ? `${rootPath.replace(/\/+$/, '')}/` : '';

    const getFileKey = (file: File) => {
      const path = file.path ? `${file.path}/` : '';
      return `${filePrefix}${path}${file.hash}${file.ext}`;
    };

    const upload = async (file: File, customParams: Partial<PutObjectCommandInput> = {}) => {
      const fileKey = getFileKey(file);

      try {
        if (file.size > 5 * 1024 * 1024) {
          // Use multipart upload for files larger than 5 MB
          await uploadLargeFile(s3Client, config.params.Bucket, fileKey, file);
        } else {
          // Use single-part upload for smaller files
          await uploadSinglePart(s3Client, config.params.Bucket, fileKey, file, customParams);
        }
      } catch (error) {
        console.error("Error uploading file:", error);
      }
    };

    const uploadSinglePart = async (
      s3Client: S3Client,
      bucket: string,
      key: string,
      file: File,
      customParams: Partial<PutObjectCommandInput> = {}
    ) => {
      const upload = new Upload({
        client: s3Client,
        params: {
          Bucket: bucket,
          Key: key,
          Body: file.stream || Buffer.from(file.buffer as any, 'binary'),
          ACL: config.params.ACL,
          ContentType: file.mime,
          ...customParams,
        },
      });

      const uploadResult = await upload.done() as UploadCommandOutput;
      updateFileUrl(file, uploadResult.Location);
    };

    const uploadLargeFile = async (
      s3Client: S3Client,
      bucket: string,
      key: string,
      file: File
    ): Promise<void> => {
      const uploadId = await initiateMultipartUpload(s3Client, bucket, key);
    
      try {
        await uploadFileParts(s3Client, bucket, key, file, uploadId);
        const completedUploadResponse = await completeMultipartUpload(
          s3Client,
          bucket,
          key,
          uploadId
        );
        updateFileUrl(file, completedUploadResponse.Location);
      } catch (error) {
        console.error("Error uploading large file:", error);
        await abortMultipartUpload(s3Client, bucket, key, uploadId);
        throw error; // Re-throw the error after handling
      }
    };

    const initiateMultipartUpload = async (s3Client: S3Client, bucket: string, key: string) => {
      const { UploadId } = await s3Client.send(
        new CreateMultipartUploadCommand({
          Bucket: bucket,
          Key: key,
        })
      );
      return UploadId;
    };

    const uploadFileParts = async (
      s3Client: S3Client,
      bucket: string,
      key: string,
      file: File,
      uploadId: string
    ) => {
      const partSize = 5 * 1024 * 1024; // 5 MB parts
      const numParts = Math.ceil(file.size / partSize);
      const uploadPromises = [];

      for (let i = 0; i < numParts; i++) {
        const start = i * partSize;
        const end = Math.min(start + partSize, file.size);
        const partBuffer = file.stream
          ? await readStreamChunk(file.stream, end - start)
          : file.buffer.slice(start, end);

        uploadPromises.push(
          s3Client.send(
            new UploadPartCommand({
              Bucket: bucket,
              Key: key,
              UploadId: uploadId,
              Body: partBuffer,
              PartNumber: i + 1,
            })
          )
        );
      }

      await Promise.all(uploadPromises);
    };

    const completeMultipartUpload = async (
      s3Client: S3Client,
      bucket: string,
      key: string,
      uploadId: string
    ): Promise<{ Location: string }> => {
      try {
        const { Location } = await s3Client.send(
          new CompleteMultipartUploadCommand({
            Bucket: bucket,
            Key: key,
            UploadId: uploadId,
          })
        );
        return { Location };
      } catch (error) {
        console.error("Error completing multipart upload:", error);
        throw error; // Re-throw the error after logging
      }
    };

    const abortMultipartUpload = async (
      s3Client: S3Client,
      bucket: string,
      key: string,
      uploadId: string
    ) => {
      await s3Client.send(
        new AbortMultipartUploadCommand({
          Bucket: bucket,
          Key: key,
          UploadId: uploadId,
        })
      );
    };

    const readStreamChunk = async (stream: ReadStream, size: number) => {
      const chunks = [];
      let bytesRead = 0;

      return new Promise<Buffer>((resolve, reject) => {
        stream.on('data', (chunk) => {
          bytesRead += chunk.length;
          chunks.push(chunk);

          if (bytesRead >= size) {
            const buffer = Buffer.concat(chunks);
            resolve(buffer.slice(0, size));
            stream.destroy(); // Close the stream after reading the required size
          }
        });

        stream.on('end', () => {
          if (bytesRead < size) {
            reject(new Error('Stream ended before reaching the required size.'));
          }
        });

        stream.on('error', reject);
      });
    };

    const updateFileUrl = (file: File, location: string) => {
      if (assertUrlProtocol(location)) {
        file.url = baseUrl ? `${baseUrl}/${getFileKey(file)}` : location;
      } else {
        file.url = `https://${location}`;
      }
    };


    return {
      isPrivate() {
        return config.params.ACL === 'private';
      },

      async getSignedUrl(file: File, customParams: any): Promise<{ url: string }> {
        // Do not sign the url if it does not come from the same bucket.
        if (!isUrlFromBucket(file.url, config.params.Bucket, baseUrl)) {
          return { url: file.url };
        }
        const fileKey = getFileKey(file);

        const url = await getSignedUrl(
          s3Client,
          new GetObjectCommand({
            Bucket: config.params.Bucket,
            Key: fileKey,
            ...customParams,
          }),
          {
            expiresIn: getOr(15 * 60, ['params', 'signedUrlExpires'], config),
          }
        );

        return { url };
      },
      uploadStream(file: File, customParams = {}) {
        return upload(file, customParams);
      },
      upload(file: File, customParams = {}) {
        return upload(file, customParams);
      },
      delete(file: File, customParams = {}): Promise<DeleteObjectCommandOutput> {
        const command = new DeleteObjectCommand({
          Bucket: config.params.Bucket,
          Key: getFileKey(file),
          ...customParams,
        });
        return s3Client.send(command);
      },
    };
  },
};