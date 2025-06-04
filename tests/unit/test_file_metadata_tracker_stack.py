import aws_cdk as core
import aws_cdk.assertions as assertions

from file_metadata_tracker.file_metadata_tracker_stack import FileMetadataTrackerStack

# example tests. To run these tests, uncomment this file along with the example
# resource in file_metadata_tracker/file_metadata_tracker_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = FileMetadataTrackerStack(app, "file-metadata-tracker")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
