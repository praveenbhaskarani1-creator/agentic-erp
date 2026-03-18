#!/bin/bash
# ─────────────────────────────────────────────────────────────
# deploy.sh
# Complete ECS Fargate deployment for Agentic Time Entry Validation
# Run from: C:\Users\PraveenBhaskarani\Documents\agentic-erp\
#
# Usage:
#   bash scripts/deploy.sh
#
# Requirements:
#   - AWS CLI configured (praveenAWS credentials)
#   - Docker Desktop running
#   - Run from project root directory
# ─────────────────────────────────────────────────────────────

set -e  # stop on any error

# ── Config ────────────────────────────────────────────────────
AWS_ACCOUNT_ID="241030170015"
AWS_REGION="us-east-1"
ECR_REPO="agentic-erp-agent"
IMAGE_TAG="latest"
CLUSTER_NAME="agentic-erp-cluster"
SERVICE_NAME="agentic-erp-service"
TASK_FAMILY="agentic-erp-task"
CONTAINER_NAME="agentic-erp-container"
LOG_GROUP="/ecs/agentic-erp"
SECURITY_GROUP="sg-06b966f739be28ab0"

ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}"

echo ""
echo "═══════════════════════════════════════════════"
echo "  Agentic Time Entry Validation — Deploy"
echo "═══════════════════════════════════════════════"
echo ""

# ── Step 1: Login to ECR ──────────────────────────────────────
echo "Step 1/7 — Logging into ECR..."
aws ecr get-login-password --region ${AWS_REGION} | \
    docker login --username AWS --password-stdin \
    ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
echo "  ✅ ECR login successful"

# ── Step 2: Build Docker image ────────────────────────────────
echo ""
echo "Step 2/7 — Building Docker image..."
docker build -t ${ECR_REPO}:${IMAGE_TAG} .
echo "  ✅ Image built: ${ECR_REPO}:${IMAGE_TAG}"

# ── Step 3: Tag and push to ECR ───────────────────────────────
echo ""
echo "Step 3/7 — Pushing to ECR..."
docker tag ${ECR_REPO}:${IMAGE_TAG} ${ECR_URI}:${IMAGE_TAG}
docker push ${ECR_URI}:${IMAGE_TAG}
echo "  ✅ Pushed to: ${ECR_URI}:${IMAGE_TAG}"

# ── Step 4: Create CloudWatch log group ───────────────────────
echo ""
echo "Step 4/7 — Creating CloudWatch log group..."
aws logs create-log-group \
    --log-group-name ${LOG_GROUP} \
    --region ${AWS_REGION} 2>/dev/null || echo "  ℹ️  Log group already exists"
echo "  ✅ Log group: ${LOG_GROUP}"

# ── Step 5: Register ECS task definition ─────────────────────
echo ""
echo "Step 5/7 — Registering ECS task definition..."
TASK_ARN=$(aws ecs register-task-definition \
    --cli-input-json file://ecs-task-definition.json \
    --region ${AWS_REGION} \
    --query "taskDefinition.taskDefinitionArn" \
    --output text)
echo "  ✅ Task definition registered: ${TASK_ARN}"

# ── Step 6: Get subnet ID ─────────────────────────────────────
echo ""
echo "Step 6/7 — Getting subnet configuration..."
SUBNET_ID=$(aws ec2 describe-subnets \
    --filters "Name=tag:Name,Values=*public*" \
    --region ${AWS_REGION} \
    --query "Subnets[0].SubnetId" \
    --output text)

if [ "${SUBNET_ID}" == "None" ] || [ -z "${SUBNET_ID}" ]; then
    echo "  ⚠️  No public subnet found by tag — fetching first available"
    SUBNET_ID=$(aws ec2 describe-subnets \
        --region ${AWS_REGION} \
        --query "Subnets[0].SubnetId" \
        --output text)
fi
echo "  ✅ Subnet: ${SUBNET_ID}"

# ── Step 7: Create or Update ECS Service ─────────────────────
echo ""
echo "Step 7/7 — Deploying ECS service..."

# Check if service already exists
SERVICE_STATUS=$(aws ecs describe-services \
    --cluster ${CLUSTER_NAME} \
    --services ${SERVICE_NAME} \
    --region ${AWS_REGION} \
    --query "services[0].status" \
    --output text 2>/dev/null || echo "MISSING")

if [ "${SERVICE_STATUS}" == "ACTIVE" ]; then
    # Update existing service
    echo "  ℹ️  Service exists — updating..."
    aws ecs update-service \
        --cluster ${CLUSTER_NAME} \
        --service ${SERVICE_NAME} \
        --task-definition ${TASK_FAMILY} \
        --force-new-deployment \
        --region ${AWS_REGION} \
        --query "service.serviceName" \
        --output text
    echo "  ✅ Service updated — deploying new version"
else
    # Create new service
    echo "  ℹ️  Creating new service..."
    aws ecs create-service \
        --cluster ${CLUSTER_NAME} \
        --service-name ${SERVICE_NAME} \
        --task-definition ${TASK_FAMILY} \
        --desired-count 1 \
        --launch-type FARGATE \
        --network-configuration "awsvpcConfiguration={
            subnets=[${SUBNET_ID}],
            securityGroups=[${SECURITY_GROUP}],
            assignPublicIp=ENABLED
        }" \
        --region ${AWS_REGION} \
        --query "service.serviceName" \
        --output text
    echo "  ✅ Service created"
fi

# ── Done ──────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════"
echo "  ✅ Deployment complete!"
echo "═══════════════════════════════════════════════"
echo ""
echo "Next steps:"
echo "  1. Wait ~2 minutes for task to start"
echo "  2. Run: bash scripts/get_api_url.sh"
echo "  3. Test: curl http://<task-ip>:8000/health"
echo ""
