from aws_cdk import (
    Stack,
    Duration,
    aws_s3 as s3,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_dynamodb as ddb,
    aws_s3_notifications as s3n,
    aws_sqs as sqs,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda_event_sources as lambda_event_sources,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    RemovalPolicy
)
from constructs import Construct
from .config import *

class FileMetadataTrackerStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # Reference an existing bucket
        bucket = s3.Bucket.from_bucket_name(
            self, "ExistingBucket",
            bucket_name=S3_BUCKET_TO_TRACK,
        )

        s3_event_queue = sqs.Queue(self, "s3EventQueue", 
                                   visibility_timeout=Duration.seconds(60))
        
        event_rule = events.Rule(
            self, "S3EventBridgeRule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created", "Object Deleted"],
                detail={
                    "bucket": {
                        "name": [bucket.bucket_name]
                    }
                }
            )
        )

        event_rule.add_target(targets.SqsQueue(s3_event_queue))




        # Router Lambda to spin up step functions

        router_lambda_role = iam.Role(
        self, f"{construct_id}_RouterLambdaRole",
        assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        managed_policies=[
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
        ])
        # Add permission to start Step Functions
        router_lambda_role.add_to_policy(iam.PolicyStatement(
            actions=["states:StartExecution"],
            resources=["*"]  # TODO: replace with specific ARNs in prod
        ))

        router_lambda = _lambda.Function(
            self, "S3EventRouterLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="event_router.handler",
            code=_lambda.Code.from_asset("Router_lambda"),
            timeout=Duration.seconds(60),
            environment={
                "CREATED_SF_ARN": "<TO_BE_FILLED>",
                "DELETED_SF_ARN": "<TO_BE_FILLED>"
            },
            role=router_lambda_role
        )

        router_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(s3_event_queue))

        # Lambda Execution Role
        MetaData_lambda_role = iam.Role(
            self, f"{construct_id}_MetaData_LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        #Layer for pyarrow
    
        # pyarrow_layer = _lambda.LayerVersion(
        #     self, "PyarrowLayer",
        #     code=_lambda.Code.from_asset("layers/pyarrow_layer"),
        #     compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
        #     description="Layer with pyarrow and dependencies",
        # )


        # DynamoDB Tables
        latest_table = ddb.Table(
            self, LATEST_TABLE_NAME,
            partition_key={"name": "filepath", "type": ddb.AttributeType.STRING},
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            # table_name=PROCESSED_TABLE_NAME
        )

        skipped_table = ddb.Table(
            self, SKIPPED_TABLE_NAME,
            partition_key={"name": "filepath", "type": ddb.AttributeType.STRING},
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            # table_name=SKIPPED_TABLE_NAME
        )

        failed_table = ddb.Table(
            self, FAILED_TABLE_NAME,
            partition_key={"name": "filepath", "type": ddb.AttributeType.STRING},
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            # table_name=FAILED_TABLE_NAME    
        )

        history_table = ddb.Table(
            self, HISTORY_TABLE_NAME,
            partition_key={"name": "filepath", "type": ddb.AttributeType.STRING},
            sort_key={"name": "timestamp", "type": ddb.AttributeType.STRING},
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            # table_name = HISTORY_TABLE_NAME
            #removal_policy=RemovalPolicy.DESTROY  # Only use in dev!
        )

        # Deleted Table
        deleted_table = ddb.Table(
            self, DELETED_TABLE_NAME,
            partition_key={"name": "filepath", "type": ddb.AttributeType.STRING},
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            # table_name=PROCESSED_TABLE_NAME
        )



        # Grant required permissions
        bucket.grant_read(MetaData_lambda_role)
        latest_table.grant_read_write_data(MetaData_lambda_role)
        skipped_table.grant_write_data(MetaData_lambda_role)
        failed_table.grant_write_data(MetaData_lambda_role)
        history_table.grant_write_data(MetaData_lambda_role)

        pyarrow_layer = _lambda.LayerVersion.from_layer_version_arn(
            self, "PyarrowLayer",
            layer_version_arn="arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:1"
        )


        # Lambda Function
        MetaData_lambda_fn = _lambda.Function(
            self, FILE_META_DATA_PROCESSOR_LAMBDA,
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="metadata_extractor.metadata_handler",
            code=_lambda.Code.from_asset("MetaData_lambda"),
            layers=[pyarrow_layer],
            environment={
                ENV_LATEST_TABLE: latest_table.table_name,
                ENV_SKIPPED_TABLE: skipped_table.table_name,
                ENV_FAILED_TABLE: failed_table.table_name,
                ENV_HISTORY_TABLE: history_table.table_name
            },
            role=MetaData_lambda_role
        )

        deletion_lambdarole = iam.Role(
            self, f"{construct_id}_DeletionLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        latest_table.grant_read_write_data(deletion_lambdarole)
        deleted_table.grant_read_write_data(deletion_lambdarole)

        deletion_lambda_fn = _lambda.Function(
            self, "DeletionTrackerLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="deletion_tracker.handler",
            code=_lambda.Code.from_asset("Deletion_lambda"),
            environment={
                ENV_LATEST_TABLE: latest_table.table_name,
                ENV_DELETED_TABLE: deleted_table.table_name
            },
            role=deletion_lambdarole
        )


        # Step functions
        # UploadTracker Step Function
        upload_tracker_definition = tasks.LambdaInvoke(
            self, "InvokeMetaDataProcessor",
            lambda_function=MetaData_lambda_fn,
            output_path="$.Payload"
        )

        upload_tracker_sm = sfn.StateMachine(
            self, "UploadTrackerStateMachine",
            definition=upload_tracker_definition,
            timeout=Duration.minutes(5)
        )

        upload_tracker_sm.grant_start_execution(router_lambda_role)
        router_lambda.add_environment("CREATED_SF_ARN", upload_tracker_sm.state_machine_arn)

    

        # DeletionTracker Step Function
        deletion_tracker_definition = tasks.LambdaInvoke(
            self, "InvokeDeletionProcessor",
            lambda_function=deletion_lambda_fn,
            output_path="$.Payload"
        )

        deletion_tracker_sm = sfn.StateMachine(
            self, "DeletionTrackerStateMachine",
            definition=deletion_tracker_definition,
            timeout=Duration.minutes(5)
        )

        deletion_tracker_sm.grant_start_execution(router_lambda_role)
        router_lambda.add_environment("DELETED_SF_ARN", deletion_tracker_sm.state_machine_arn)

                



        # Since Event bridge and SQS are being setup - this is no longer needed
        # bucket.add_event_notification(
        #     s3.EventType.OBJECT_CREATED,
        #     s3n.LambdaDestination(lambda_fn)
        # )
