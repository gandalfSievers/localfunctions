"""Simple Python Lambda handler that echoes back the event."""

import json


def handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Hello from Python!",
            "input": event,
        }),
    }
