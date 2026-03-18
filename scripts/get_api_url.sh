#!/bin/bash
# ─────────────────────────────────────────────────────────────
# get_api_url.sh
# Gets the public IP of your running ECS Fargate task
#
# Run after deploy.sh completes:
#   bash scripts/get_api_url.sh
# ─────────────────────────────────────────────────────────────

AWS_REGION="us-east-1"
CLUSTER_NAME="agentic-erp-cluster"
SERVICE_NAME="agentic-erp-service"

echo ""
echo "Getting ECS task public IP..."

# Get task ARN
TASK_ARN=$(aws ecs list-tasks \
    --cluster ${CLUSTER_NAME} \
    --service-name ${SERVICE_NAME} \
    --region ${AWS_REGION} \
    --query "taskArns[0]" \
    --output text)

if [ "${TASK_ARN}" == "None" ] || [ -z "${TASK_ARN}" ]; then
    echo "  ⚠️  No running tasks found yet — wait 1-2 minutes and try again"
    exit 1
fi

echo "  Task ARN: ${TASK_ARN}"

# Get network interface ID
ENI_ID=$(aws ecs describe-tasks \
    --cluster ${CLUSTER_NAME} \
    --tasks ${TASK_ARN} \
    --region ${AWS_REGION} \
    --query "tasks[0].attachments[0].details[?name=='networkInterfaceId'].value" \
    --output text)

# Get public IP from network interface
PUBLIC_IP=$(aws ec2 describe-network-interfaces \
    --network-interface-ids ${ENI_ID} \
    --region ${AWS_REGION} \
    --query "NetworkInterfaces[0].Association.PublicIp" \
    --output text)

if [ "${PUBLIC_IP}" == "None" ] || [ -z "${PUBLIC_IP}" ]; then
    echo "  ⚠️  No public IP yet — task may still be starting"
    echo "  Wait 1 more minute and try again"
    exit 1
fi

echo ""
echo "═══════════════════════════════════════════════"
echo "  ✅ Your API is live!"
echo "═══════════════════════════════════════════════"
echo ""
echo "  Base URL:  http://${PUBLIC_IP}:8000"
echo ""
echo "  Endpoints:"
echo "  Health:    http://${PUBLIC_IP}:8000/health"
echo "  Docs:      http://${PUBLIC_IP}:8000/docs"
echo "  Ask:       http://${PUBLIC_IP}:8000/ask"
echo "  Export:    http://${PUBLIC_IP}:8000/export/excel"
echo "  Queries:   http://${PUBLIC_IP}:8000/queries"
echo ""
echo "  Test command:"
echo "  curl http://${PUBLIC_IP}:8000/health"
echo ""
echo "  Ask command:"
echo "  curl -X POST http://${PUBLIC_IP}:8000/ask \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"question\": \"show me entries with blank memos\"}'"
echo ""
