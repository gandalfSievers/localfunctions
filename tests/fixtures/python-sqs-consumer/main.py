"""SQS consumer Lambda handler for container reuse integration tests.

Processes SQS messages and returns the record count. Used to verify that a
single container can process multiple sequential SQS batches without getting
stuck in the Busy state.
"""

import json
import os
import urllib.request


def handler(event, context):
    records = event.get("Records", [])
    bodies = [r.get("body", "") for r in records]

    result = {
        "statusCode": 200,
        "body": json.dumps({
            "message": "SQS messages processed",
            "record_count": len(records),
            "bodies": bodies,
        }),
    }

    # If any message body is "self-invoke", trigger a self-invocation
    # to test that the invoke API works while an SQS consumer is running.
    for body in bodies:
        if body == "self-invoke":
            invoke_url = os.environ.get("SELF_INVOKE_URL")
            if invoke_url:
                req = urllib.request.Request(
                    invoke_url,
                    data=json.dumps({"source": "self-invoke"}).encode(),
                    method="POST",
                )
                try:
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        resp_body = resp.read().decode()
                        result["body"] = json.dumps({
                            "message": "SQS + self-invoke processed",
                            "self_invoke_result": json.loads(resp_body),
                            "record_count": len(records),
                        })
                except Exception as e:
                    result["body"] = json.dumps({
                        "message": "SQS processed, self-invoke failed",
                        "error": str(e),
                        "record_count": len(records),
                    })

    return result
