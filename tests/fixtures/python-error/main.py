"""Python Lambda handler that raises an error."""


def handler(event, context):
    raise ValueError("Intentional test error from Python")
