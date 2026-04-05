"""
SDK compatibility tests using Python boto3.

Verifies that a standard boto3 Lambda client can invoke functions on
localfunctions using only the endpoint_url override — no custom HTTP logic.

Requires:
  - localfunctions running with test functions configured (see README.md)
  - pip install boto3 pytest
"""

import json
import os

import boto3
import pytest

ENDPOINT = os.environ.get("LOCALFUNCTIONS_ENDPOINT", "http://localhost:9600")


@pytest.fixture(scope="module")
def lambda_client():
    """Create a boto3 Lambda client pointing at localfunctions."""
    return boto3.client(
        "lambda",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


# ---------------------------------------------------------------------------
# Successful invocations
# ---------------------------------------------------------------------------


class TestSuccessfulInvocations:
    """Verify that successful Lambda invocations return correct responses."""

    def test_invoke_python_function(self, lambda_client):
        """Invoke the python-hello function and verify the response payload."""
        payload = {"key": "value", "number": 42}
        response = lambda_client.invoke(
            FunctionName="python-hello",
            Payload=json.dumps(payload),
        )

        assert response["StatusCode"] == 200
        assert "FunctionError" not in response

        result = json.loads(response["Payload"].read())
        assert result["statusCode"] == 200

        body = json.loads(result["body"])
        assert body["message"] == "Hello from Python!"
        assert body["input"] == payload

    def test_invoke_nodejs_function(self, lambda_client):
        """Invoke the nodejs-hello function and verify the response payload."""
        payload = {"greeting": "hello", "items": [1, 2, 3]}
        response = lambda_client.invoke(
            FunctionName="nodejs-hello",
            Payload=json.dumps(payload),
        )

        assert response["StatusCode"] == 200
        assert "FunctionError" not in response

        result = json.loads(response["Payload"].read())
        assert result["statusCode"] == 200

        body = json.loads(result["body"])
        assert body["message"] == "Hello from Node.js!"
        assert body["input"] == payload

    def test_invoke_with_empty_payload(self, lambda_client):
        """Invoke with an empty JSON object — SDK should handle it fine."""
        response = lambda_client.invoke(
            FunctionName="python-hello",
            Payload=json.dumps({}),
        )

        assert response["StatusCode"] == 200
        result = json.loads(response["Payload"].read())
        body = json.loads(result["body"])
        assert body["input"] == {}

    def test_response_metadata_present(self, lambda_client):
        """Verify that the response contains expected metadata fields."""
        response = lambda_client.invoke(
            FunctionName="python-hello",
            Payload=json.dumps({"test": True}),
        )

        assert response["StatusCode"] == 200
        # boto3 should parse the executed version header
        assert "ExecutedVersion" in response


# ---------------------------------------------------------------------------
# Error responses
# ---------------------------------------------------------------------------


class TestErrorResponses:
    """Verify that function errors are reported correctly through the SDK."""

    def test_python_function_error(self, lambda_client):
        """Invoke a function that raises an exception — SDK should surface it."""
        response = lambda_client.invoke(
            FunctionName="python-error",
            Payload=json.dumps({"trigger": "error"}),
        )

        # Lambda returns 200 with FunctionError header for handled/unhandled errors
        assert response["StatusCode"] == 200
        assert response.get("FunctionError") == "Unhandled"

        error_payload = json.loads(response["Payload"].read())
        assert "errorMessage" in error_payload
        assert "errorType" in error_payload

    def test_nodejs_function_error(self, lambda_client):
        """Invoke a Node.js function that throws — SDK should surface it."""
        response = lambda_client.invoke(
            FunctionName="nodejs-error",
            Payload=json.dumps({"trigger": "error"}),
        )

        assert response["StatusCode"] == 200
        assert response.get("FunctionError") == "Unhandled"

        error_payload = json.loads(response["Payload"].read())
        assert "errorMessage" in error_payload
        assert "errorType" in error_payload

    def test_invoke_nonexistent_function(self, lambda_client):
        """Invoke a function that does not exist — expect ResourceNotFoundException."""
        with pytest.raises(lambda_client.exceptions.ResourceNotFoundException):
            lambda_client.invoke(
                FunctionName="nonexistent-function",
                Payload=json.dumps({}),
            )


# ---------------------------------------------------------------------------
# DryRun invocation
# ---------------------------------------------------------------------------


class TestDryRun:
    """Verify DryRun invocation type works through the SDK."""

    def test_dryrun_invocation(self, lambda_client):
        """DryRun should return 204 without executing the function."""
        response = lambda_client.invoke(
            FunctionName="python-hello",
            InvocationType="DryRun",
            Payload=json.dumps({}),
        )

        assert response["StatusCode"] == 204
