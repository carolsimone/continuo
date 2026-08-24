import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';

function requiredEnv(name: string): string {
  const v = process.env[name];
  if (v === undefined || v.trim() === '') {
    throw new Error(`${name} is required: the S3 client has no credential fallback`);
  }
  return v;
}

// Fails closed on missing credentials: every supported deployment path
// (docker-compose, Helm chart) injects explicit S3 credentials, so an unset
// variable is a wiring error that must surface at server startup, not as a
// failed S3 call mid-request.
export function assertS3Config(): void {
  requiredEnv('AWS_ACCESS_KEY_ID');
  requiredEnv('AWS_SECRET_ACCESS_KEY');
}

let client: S3Client | undefined;

function s3(): S3Client {
  if (!client) {
    client = new S3Client({
      region: process.env.AWS_DEFAULT_REGION || 'us-east-1',
      endpoint: process.env.S3_ENDPOINT_URL,
      forcePathStyle: true, // Required for MinIO / path-style S3 endpoints
      credentials: {
        accessKeyId: requiredEnv('AWS_ACCESS_KEY_ID'),
        secretAccessKey: requiredEnv('AWS_SECRET_ACCESS_KEY'),
      },
    });
  }
  return client;
}

const BUCKET = process.env.S3_BUCKET || 'continuo';

export async function getLogObject(key: string): Promise<string> {
  const resp = await s3().send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
  return resp.Body!.transformToString();
}
