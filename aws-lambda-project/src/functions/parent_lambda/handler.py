import json
import boto3
import os

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')
table = dynamodb.Table(os.environ['DYNAMODB_TABLE'])

def lambda_handler(event, context):
    try:
        # Extract data from EventBridge event
        event_detail = event.get('detail', {})
        
        # Store event in DynamoDB for tracking
        table.put_item(Item={
            'eventId': event['id'],
            'timestamp': event['time'],
            'status': 'PROCESSING',
            'detail': event_detail
        })
        
        # Invoke child Lambda
        response = lambda_client.invoke(
            FunctionName=os.environ['CHILD_LAMBDA_ARN'],
            InvocationType='RequestResponse',
            Payload=json.dumps(event_detail)
        )
        
        # Process child Lambda response
        child_response = json.loads(response['Payload'].read())
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processing completed successfully',
                'childResponse': child_response
            })
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error processing request',
                'error': str(e)
            })
        }
