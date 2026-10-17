#!/bin/sh
# Creates the FIFO queues used by the service (idempotent).
#   wager-transactions.fifo      input: provider operations (redrive -> DLQ after 5 receives)
#   wager-transactions-dlq.fifo  dead letters (poison messages and exhausted retries)
#   wager-events.fifo            output: events published from the transactional outbox
set -eu
E="--endpoint-url http://localstack:4566 --region us-east-1"
ACCOUNT=000000000000
DLQ_ARN="arn:aws:sqs:us-east-1:${ACCOUNT}:wager-transactions-dlq.fifo"

aws $E sqs create-queue --queue-name wager-transactions-dlq.fifo \
  --attributes FifoQueue=true,MessageRetentionPeriod=1209600 >/dev/null

# Broker-side access control: only the provider gateway role may send, only the
# wallet service role may receive/delete. LocalStack community does NOT enforce
# IAM policies; on AWS this policy (plus IAM) is what protects the queue.
cat > /tmp/main-attrs.json <<JSON
{"FifoQueue":"true","VisibilityTimeout":"20",
 "RedrivePolicy":"{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"5\"}",
 "Policy":"{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"ProviderGatewaySend\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::${ACCOUNT}:role/provider-gateway\"},\"Action\":\"sqs:SendMessage\",\"Resource\":\"arn:aws:sqs:us-east-1:${ACCOUNT}:wager-transactions.fifo\"},{\"Sid\":\"WalletServiceConsume\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::${ACCOUNT}:role/wallet-service\"},\"Action\":[\"sqs:ReceiveMessage\",\"sqs:DeleteMessage\",\"sqs:ChangeMessageVisibility\",\"sqs:GetQueueAttributes\"],\"Resource\":\"arn:aws:sqs:us-east-1:${ACCOUNT}:wager-transactions.fifo\"}]}"}
JSON
aws $E sqs create-queue --queue-name wager-transactions.fifo --attributes file:///tmp/main-attrs.json >/dev/null

aws $E sqs create-queue --queue-name wager-events.fifo --attributes FifoQueue=true >/dev/null
echo "queues ready"
