import json
import boto3
import os


# Initialize Step Functions client
sf_client = boto3.client("stepfunctions")

# Read ARNs from environment
CREATED_SF_ARN = os.environ["CREATED_SF_ARN"]
DELETED_SF_ARN = os.environ["DELETED_SF_ARN"]

def handler(event, context):
    for record in event["Records"]:
        try:
            # EventBridge puts the full event as a JSON string inside the SQS message body
            body = json.loads(record["body"])
            print("Received event:", json.dumps(body, indent=2))

            #event_name = body["detail"]["eventName"]
            event_name = body.get("detail-type", "")
            print(event_name)
            sf_input = json.dumps(body)  # Full event passed into Step Function

            if event_name.startswith("Object Created"):
                print(f"Routing to CREATED Step Function: {CREATED_SF_ARN}")
                sf_client.start_execution(
                    stateMachineArn=CREATED_SF_ARN,
                    input=sf_input
                )

            elif event_name.startswith("Object Deleted"):
                print(f"Routing to DELETED Step Function: {DELETED_SF_ARN}")
                sf_client.start_execution(
                    stateMachineArn=DELETED_SF_ARN,
                    input=sf_input
                )
            else:
                print(f"Unknown event type: {event_name}")
        
        except Exception as e:
            print(f"Error processing record: {e}")
