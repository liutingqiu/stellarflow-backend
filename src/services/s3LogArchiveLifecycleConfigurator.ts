import {
  GetBucketLifecycleConfigurationCommand,
  PutBucketLifecycleConfigurationCommand,
  S3Client,
  type LifecycleRule,
} from "@aws-sdk/client-s3";

export interface S3LogArchiveLifecycleOptions {
  bucketName: string;
  prefix?: string;
  expirationDays?: number;
  objectLockRetentionDays?: number;
}

export interface S3LifecycleCommandClient {
  send(command: unknown): Promise<unknown>;
}

const MANAGED_RULE_ID = "stellarflow-log-archive-lifecycle";

function assertDays(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
}

/** Configures archive retention without discarding unrelated bucket rules. */
export class S3LogArchiveLifecycleConfigurator {
  constructor(
    private readonly client: S3LifecycleCommandClient = new S3Client({}),
  ) {}

  buildRule(options: S3LogArchiveLifecycleOptions): LifecycleRule {
    const bucketName = options.bucketName.trim();
    if (!bucketName) throw new Error("bucketName is required");

    const expirationDays = options.expirationDays ?? 365;
    assertDays(expirationDays, "expirationDays");

    if (options.objectLockRetentionDays !== undefined) {
      assertDays(options.objectLockRetentionDays, "objectLockRetentionDays");
      if (expirationDays < options.objectLockRetentionDays) {
        throw new Error(
          "expirationDays cannot be shorter than Object Lock retention",
        );
      }
    }

    return {
      ID: MANAGED_RULE_ID,
      Status: "Enabled",
      Filter: { Prefix: options.prefix ?? "" },
      Transitions: [
        { Days: 30, StorageClass: "STANDARD_IA" },
        { Days: 90, StorageClass: "GLACIER" },
      ],
      Expiration: { Days: expirationDays },
      AbortIncompleteMultipartUpload: { DaysAfterInitiation: 7 },
    };
  }

  async configure(options: S3LogArchiveLifecycleOptions): Promise<LifecycleRule> {
    const bucketName = options.bucketName.trim();
    const normalizedOptions = { ...options, bucketName };
    const rule = this.buildRule(normalizedOptions);
    const existingRules = await this.getExistingRules(bucketName);
    const rules = [
      ...existingRules.filter((existing) => existing.ID !== MANAGED_RULE_ID),
      rule,
    ];

    await this.client.send(
      new PutBucketLifecycleConfigurationCommand({
        Bucket: bucketName,
        LifecycleConfiguration: { Rules: rules },
      }),
    );
    return rule;
  }

  private async getExistingRules(bucketName: string): Promise<LifecycleRule[]> {
    try {
      const response = (await this.client.send(
        new GetBucketLifecycleConfigurationCommand({ Bucket: bucketName }),
      )) as { Rules?: LifecycleRule[] };
      return response.Rules ?? [];
    } catch (error) {
      const name =
        typeof error === "object" && error !== null && "name" in error
          ? String(error.name)
          : "";
      if (name === "NoSuchLifecycleConfiguration") return [];
      throw error;
    }
  }
}