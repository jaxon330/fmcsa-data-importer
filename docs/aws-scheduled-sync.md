# AWS Scheduled FMCSA Sync

These are deployment notes only. Do not create AWS resources from this repository without an explicit deployment task.

## High-Level Steps

1. Create an S3 bucket for raw FMCSA files.
   - AWS profile: `fmcsa-importer`
   - AWS region: `us-east-2`
   - Bucket: `fmcsa-importer-dataset-dev`
   - Prefix: `fmcsa/dataset`
2. Store importer secrets in AWS Secrets Manager.
   - Database URL secret: `fmcsa-importer-dev/database-url`
   - Socrata app token secret: `fmcsa-importer-dev/socrata-app-token`
3. Build the Docker image.
4. Push the image to ECR.
   - Repository URI: `010257029704.dkr.ecr.us-east-2.amazonaws.com/fmcsa-importer-dev`
   - Current amd64 image tags: `amd64-90d1bc6`, `latest-amd64`
5. Create an ECS task definition for the `fmcsa-data-importer` container.
   - Task definition is created.
   - Family: `fmcsa-importer-dev`
   - Current ARN/revision: `arn:aws:ecs:us-east-2:010257029704:task-definition/fmcsa-importer-dev:4`
   - Container name: `fmcsa-data-importer`
   - Image: `010257029704.dkr.ecr.us-east-2.amazonaws.com/fmcsa-importer-dev:amd64-90d1bc6`
   - Execution role: `arn:aws:iam::010257029704:role/fmcsa-importer-ecs-execution-dev`
   - Task role: `arn:aws:iam::010257029704:role/fmcsa-importer-ecs-task-dev`
   - Log group: `/ecs/fmcsa-importer-dev`
   - Revision 1 failed manual ECS testing because the image was built for arm64 and ECS Fargate was configured for X86_64, causing `exec format error`.
   - Revision 2 uses the amd64 image tag and passed the manual ECS readiness test with exit code 0.
   - Revision 3 corrected S3 environment variable names to match the current code.
   - Revision 4 adds production-safe retry handling for transient FMCSA/Socrata download failures.
   - Manual dry-run task: `arn:aws:ecs:us-east-2:010257029704:task/fmcsa-importer-dev/c71bbcc78c4042169e81e9159e946422`
   - Manual dry-run command: `npm run sync:fmcsa -- --dry-run --source diff --datasets carrier`
   - Manual dry-run result: failed with exit code 1 after retrying transient HTTP 503 responses from FMCSA/Socrata.
   - Manual dry-run config check: logs showed `storage: s3`, so the S3 task configuration is correct.
   - EventBridge schedule is created but disabled.
6. Configure environment variables:
   - `AWS_PROFILE=fmcsa-importer`
   - `AWS_REGION=us-east-2`
   - `FMCSA_STORAGE_TYPE=s3`
   - `FMCSA_S3_BUCKET_NAME=fmcsa-importer-dataset-dev`
   - `FMCSA_S3_PREFIX=fmcsa/dataset`
   - `LOG_LEVEL=INFO`
7. Attach an IAM role with:
   - IAM roles are created.
   - Execution role: `fmcsa-importer-ecs-execution-dev`
   - Task role: `fmcsa-importer-ecs-task-dev`
   - S3 read/write permission for the raw FMCSA bucket and prefix.
   - Secret read permission for `fmcsa-importer-dev/database-url` and `fmcsa-importer-dev/socrata-app-token`.
8. Create an EventBridge Scheduler rule:
   - Schedule name: `fmcsa-importer-daily-diff-dev`
   - Schedule ARN: `arn:aws:scheduler:us-east-2:010257029704:schedule/default/fmcsa-importer-daily-diff-dev`
   - State: `DISABLED`
   - Schedule: 6:00 AM America/Chicago daily.
   - Target cluster: `arn:aws:ecs:us-east-2:010257029704:cluster/fmcsa-importer-dev`
   - Target task definition: `arn:aws:ecs:us-east-2:010257029704:task-definition/fmcsa-importer-dev:4`
   - Scheduler role: `arn:aws:iam::010257029704:role/fmcsa-importer-scheduler-dev`
   - Network: `subnet-0fb837392966b8f56`, `subnet-06771f8d9ba66631f`, `sg-05ff742d7518b9226`, public IP enabled.
   - Command: `npm run sync:fmcsa -- --source diff --datasets carrier,active-insurance,insurance-history`
9. Send ECS task logs to CloudWatch.
10. Optionally create a retry schedule at 9:00 AM America/Chicago using the same command.

## Notes

- Daily diff sync is the normal scheduled job.
- All-history sync should be manual or monthly, not daily.
- Raw S3 keys use `{FMCSA_S3_PREFIX}/{source}/{filename}`. Date folders are not used because the FMCSA date is already in each filename.
- Downloads retry transient failures such as HTTP 429, 500, 502, 503, 504, timeouts, and connection resets with bounded exponential backoff.
- Real secret values are stored in AWS Secrets Manager and must never be committed to the repository.
- Update this document after every successful AWS implementation step so deployment state stays aligned with real infrastructure.
