import json
import boto3
import os

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['DYNAMODB_TABLE'])

def lambda_handler(event, context):
    try:
        # Process the task
        task_id = event.get('taskId', 'default-task')
        
        # Store result in S3
        result = {'taskId': task_id, 'status': 'COMPLETED'}
        s3.put_object(
            Bucket=os.environ['S3_BUCKET'],
            Key=f'results/{task_id}.json',
            Body=json.dumps(result)
        )
        
        # Update task status in DynamoDB
        table.update_item(
            Key={'eventId': task_id},
            UpdateExpression='SET #status = :status',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={':status': 'COMPLETED'}
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Task processed successfully',
                'taskId': task_id
            })
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error processing task',
                'error': str(e)
            })
        }
