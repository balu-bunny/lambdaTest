# AWS Lambda Project with EventBridge Orchestration

This project demonstrates an AWS serverless application with parent-child Lambda functions, EventBridge orchestration, S3 storage, DynamoDB tracking, and API Gateway integration.

## Architecture

- Parent Lambda (Orchestrator): Receives events from EventBridge and manages the workflow
- Child Lambda (Task Processor): Performs the actual task processing
- EventBridge: Triggers the parent Lambda based on custom events
- S3: Stores task results
- DynamoDB: Tracks task status and metadata
- API Gateway: Provides HTTP endpoints for interaction

## Prerequisites

1. AWS CLI installed and configured
2. AWS SAM CLI installed
3. Python 3.9 or later
4. boto3 library (`pip install boto3`)

## Project Structure

```
aws-lambda-project/
├── src/
│   └── functions/
│       ├── parent_lambda/
│       │   └── handler.py
│       └── child_lambda/
│           └── handler.py
└── infrastructure/
    └── cloudformation/
        └── template.yaml
```

## Deployment

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Build and deploy using SAM:
```bash
sam build
sam deploy --guided
```

3. Follow the prompts to configure your deployment.

## Testing

1. You can trigger the workflow via the API Gateway endpoint provided in the CloudFormation outputs.

2. Example API request:
```bash
curl -X POST https://your-api-endpoint/dev/task \
  -H "Content-Type: application/json" \
  -d '{"taskId": "task-123"}'
```

3. Check DynamoDB for task status and S3 for task results.

## Environment Variables

The following environment variables are automatically set by CloudFormation:
- DYNAMODB_TABLE: Name of the DynamoDB tracking table
- S3_BUCKET: Name of the S3 storage bucket
- CHILD_LAMBDA_ARN: ARN of the child Lambda function

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
