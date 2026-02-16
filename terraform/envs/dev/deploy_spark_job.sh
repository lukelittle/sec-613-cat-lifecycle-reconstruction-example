#!/bin/bash
# Deploy Spark job to EMR Serverless

set -e

echo "Deploying Spark Lifecycle Job to EMR Serverless..."

# Get Terraform outputs
EMR_APP_ID=$(terraform output -raw emr_application_id)
EMR_ROLE_ARN=$(terraform output -raw emr_job_role_arn)
S3_BUCKET=$(terraform output -raw s3_bucket_name)
MSK_BOOTSTRAP=$(terraform output -raw msk_bootstrap_servers)

echo "EMR Application ID: $EMR_APP_ID"
echo "S3 Bucket: $S3_BUCKET"

# Upload Spark job to S3 (already done by Terraform, but can update)
echo "Uploading Spark job..."
aws s3 cp ../../../spark/lifecycle_job/lifecycle_streaming.py \
  s3://$S3_BUCKET/spark-jobs/lifecycle_streaming.py

# Start job run
echo "Starting EMR Serverless job..."
JOB_RUN_ID=$(aws emr-serverless start-job-run \
  --application-id $EMR_APP_ID \
  --execution-role-arn $EMR_ROLE_ARN \
  --name "cat-lifecycle-streaming-$(date +%Y%m%d-%H%M%S)" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'$S3_BUCKET'/spark-jobs/lifecycle_streaming.py",
      "sparkSubmitParameters": "--conf spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,software.amazon.msk:aws-msk-iam-auth:2.0.3 --conf spark.sql.streaming.checkpointLocation=s3://'$S3_BUCKET'/checkpoints/lifecycle-job --conf spark.executor.memory=4g --conf spark.driver.memory=2g --conf spark.sql.shuffle.partitions=6"
    }
  }' \
  --configuration-overrides '{
    "monitoringConfiguration": {
      "cloudWatchLoggingConfiguration": {
        "enabled": true,
        "logGroupName": "/aws/emr-serverless/'$EMR_APP_ID'",
        "logStreamNamePrefix": "lifecycle-job"
      }
    }
  }' \
  --query 'jobRunId' \
  --output text)

echo "Job started with ID: $JOB_RUN_ID"
echo ""
echo "Monitor job status:"
echo "  aws emr-serverless get-job-run --application-id $EMR_APP_ID --job-run-id $JOB_RUN_ID"
echo ""
echo "View logs:"
echo "  aws logs tail /aws/emr-serverless/applications/$EMR_APP_ID --follow"
echo ""
echo "Job will start processing events from Kafka topic: cat.events.v1"
echo "Bootstrap servers: $MSK_BOOTSTRAP"
