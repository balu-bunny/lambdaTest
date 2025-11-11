import json
import logging
import traceback
from functools import wraps

# Set up a shared logger (writes to CloudWatch automatically)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_exception_handler(func):
    """
    Decorator for handling exceptions and logging in AWS Lambda.
    Usage:
        @lambda_exception_handler
        def lambda_handler(event, context):
            ...
    """
    @wraps(func)
    def wrapper(event, context):
        logger.info(f"Lambda {func.__name__} started")
        logger.info(f"Event: {json.dumps(event)}")

        try:
            # Execute the main Lambda function
            result = func(event, context)
            logger.info(f"Lambda {func.__name__} completed successfully")

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Success",
                    "result": result
                })
            }

        except ValueError as ve:
            logger.error(f"ValueError: {str(ve)}", exc_info=True)
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "error": "Invalid input",
                    "details": str(ve)
                })
            }

        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {str(e)}")
            logger.debug(traceback.format_exc())
            return {
                "statusCode": 500,
                "body": json.dumps({
                    "error": "Internal Server Error",
                    "details": str(e)
                })
            }

    return wrapper
