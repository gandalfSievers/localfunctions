"""Python Lambda handler that sleeps longer than its timeout."""

import time


def handler(event, context):
    time.sleep(30)
    return {"message": "should never reach here"}
