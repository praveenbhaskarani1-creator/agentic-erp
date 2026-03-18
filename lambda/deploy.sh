#!/bin/bash
# ─────────────────────────────────────────────────────────────
# lambda/deploy.sh
# Packages and deploys the process_upload Lambda function.
#
# Run once from the project root:
#   bash lambda/deploy.sh
#
# Prerequisites:
#   - AWS CLI configured (aws configure)
#   - Python 3.11 available
#   - Lambda execution role already exists (see ROLE_ARN below)
# ─────────────────────────────────────────────────────────────

set -e

AWS_REGION="us-east-1"
ACCOUNT_ID="241030170015"
FUNCTION_NAME="agentic-erp-process-upload"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/agentic-erp-lambda-role"
BUCKET="agentic-erp-artifacts-${ACCOUNT_ID}"
RUNTIME="python3.11"
HANDLER="process_upload.handler"

# RDS connection — Lambda reads from these env vars
DB_HOST="agentic-erp-pgvector.cwb0sigyk8na.us-east-1.rds.amazonaws.com"
DB_PORT="5432"
DB_NAME="agentdb"
DB_USER="pgadmin"
# DB_PASSWORD is injected from Secrets Manager (see --environment below)

echo ""
echo "═══════════════════════════════════════════════"
echo "  Deploying Lambda: ${FUNCTION_NAME}"
echo "═══════════════════════════════════════════════"
echo ""

# ── Step 1: Package dependencies ──────────────────────────────
echo "  [1/5] Installing dependencies into package/"
rm -rf lambda/package
mkdir -p lambda/package
pip install -r lambda/requirements.txt \
    --target lambda/package \
    --platform manylinux2014_x86_64 \
    --implementation cp \
    --python-version 3.11 \
    --only-binary=:all: \
    --quiet

# ── Step 2: Zip everything ─────────────────────────────────────
echo "  [2/5] Creating deployment zip"
cp lambda/process_upload.py lambda/package/
cd lambda/package
zip -r ../function.zip . -q
cd ../..
echo "  Zip size: $(du -sh lambda/function.zip | cut -f1)"

# ── Step 3: Upload zip to S3 ──────────────────────────────────
echo "  [3/5] Uploading zip to S3"
aws s3 cp lambda/function.zip \
    s3://${BUCKET}/lambda/${FUNCTION_NAME}.zip \
    --region ${AWS_REGION}

# ── Step 4: Create or update Lambda ───────────────────────────
echo "  [4/5] Creating / updating Lambda function"

FUNCTION_EXISTS=$(aws lambda get-function \
    --function-name ${FUNCTION_NAME} \
    --region ${AWS_REGION} \
    --query "Configuration.FunctionName" \
    --output text 2>/dev/null || echo "NOT_FOUND")

ENV_VARS="Variables={DB_HOST=${DB_HOST},DB_PORT=${DB_PORT},DB_NAME=${DB_NAME},DB_USER=${DB_USER},DB_PASSWORD=$(aws secretsmanager get-secret-value --secret-id agentic-erp/rds-pgvector --region ${AWS_REGION} --query SecretString --output text | python3 -c 'import sys,json; print(json.load(sys.stdin).get(\"password\",\"\"))'),S3_ARTIFACTS_BUCKET=${BUCKET}}"

if [ "${FUNCTION_EXISTS}" == "NOT_FOUND" ]; then
    echo "  Creating new Lambda function..."
    aws lambda create-function \
        --function-name  ${FUNCTION_NAME} \
        --runtime        ${RUNTIME} \
        --role           ${ROLE_ARN} \
        --handler        ${HANDLER} \
        --code           S3Bucket=${BUCKET},S3Key=lambda/${FUNCTION_NAME}.zip \
        --timeout        120 \
        --memory-size    512 \
        --environment    "${ENV_VARS}" \
        --region         ${AWS_REGION}
else
    echo "  Updating existing Lambda function..."
    aws lambda update-function-code \
        --function-name ${FUNCTION_NAME} \
        --s3-bucket     ${BUCKET} \
        --s3-key        lambda/${FUNCTION_NAME}.zip \
        --region        ${AWS_REGION} > /dev/null

    aws lambda update-function-configuration \
        --function-name ${FUNCTION_NAME} \
        --timeout       120 \
        --memory-size   512 \
        --environment   "${ENV_VARS}" \
        --region        ${AWS_REGION} > /dev/null
fi

# ── Step 5: Add S3 trigger ────────────────────────────────────
echo "  [5/5] Configuring S3 trigger (uploads/pending/)"

# Allow S3 to invoke Lambda
aws lambda add-permission \
    --function-name ${FUNCTION_NAME} \
    --statement-id  s3-trigger-permission \
    --action        lambda:InvokeFunction \
    --principal     s3.amazonaws.com \
    --source-arn    arn:aws:s3:::${BUCKET} \
    --region        ${AWS_REGION} 2>/dev/null || echo "  (Permission already exists)"

LAMBDA_ARN=$(aws lambda get-function \
    --function-name ${FUNCTION_NAME} \
    --region        ${AWS_REGION} \
    --query         "Configuration.FunctionArn" \
    --output text)

# Set S3 bucket notification
aws s3api put-bucket-notification-configuration \
    --bucket ${BUCKET} \
    --notification-configuration "{
        \"LambdaFunctionConfigurations\": [
            {
                \"LambdaFunctionArn\": \"${LAMBDA_ARN}\",
                \"Events\": [\"s3:ObjectCreated:*\"],
                \"Filter\": {
                    \"Key\": {
                        \"FilterRules\": [
                            {\"Name\": \"prefix\", \"Value\": \"uploads/pending/\"}
                        ]
                    }
                }
            }
        ]
    }"

echo ""
echo "═══════════════════════════════════════════════"
echo "  ✅ Lambda deployed successfully!"
echo "═══════════════════════════════════════════════"
echo ""
echo "  Function : ${FUNCTION_NAME}"
echo "  Trigger  : s3://${BUCKET}/uploads/pending/*"
echo "  Results  : s3://${BUCKET}/uploads/results/*"
echo ""
echo "  IAM role needed permissions:"
echo "    - s3:GetObject   on ${BUCKET}/uploads/pending/*"
echo "    - s3:PutObject   on ${BUCKET}/uploads/results/*"
echo "    - rds-db:connect on agentdb"
echo "    - logs:CreateLogGroup / PutLogEvents"
echo ""
