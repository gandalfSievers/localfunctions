/**
 * SDK compatibility tests using Node.js @aws-sdk/client-lambda (v3).
 *
 * Verifies that a standard AWS SDK Lambda client can invoke functions on
 * localfunctions using only the endpoint override — no custom HTTP logic.
 *
 * Requires:
 *   - localfunctions running with test functions configured (see README.md)
 *   - npm install
 *
 * Run: node --test test_aws_sdk.mjs
 */

import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  LambdaClient,
  InvokeCommand,
  ResourceNotFoundException,
} from "@aws-sdk/client-lambda";

const ENDPOINT =
  process.env.LOCALFUNCTIONS_ENDPOINT || "http://localhost:9600";

const client = new LambdaClient({
  endpoint: ENDPOINT,
  region: "us-east-1",
  credentials: {
    accessKeyId: "testing",
    secretAccessKey: "testing",
  },
});

function decodePayload(payload) {
  return JSON.parse(new TextDecoder().decode(payload));
}

// ---------------------------------------------------------------------------
// Successful invocations
// ---------------------------------------------------------------------------

describe("Successful invocations", () => {
  it("should invoke a Python function and get correct response", async () => {
    const payload = { key: "value", number: 42 };
    const command = new InvokeCommand({
      FunctionName: "python-hello",
      Payload: new TextEncoder().encode(JSON.stringify(payload)),
    });

    const response = await client.send(command);

    assert.equal(response.StatusCode, 200);
    assert.equal(response.FunctionError, undefined);

    const result = decodePayload(response.Payload);
    assert.equal(result.statusCode, 200);

    const body = JSON.parse(result.body);
    assert.equal(body.message, "Hello from Python!");
    assert.deepEqual(body.input, payload);
  });

  it("should invoke a Node.js function and get correct response", async () => {
    const payload = { greeting: "hello", items: [1, 2, 3] };
    const command = new InvokeCommand({
      FunctionName: "nodejs-hello",
      Payload: new TextEncoder().encode(JSON.stringify(payload)),
    });

    const response = await client.send(command);

    assert.equal(response.StatusCode, 200);
    assert.equal(response.FunctionError, undefined);

    const result = decodePayload(response.Payload);
    assert.equal(result.statusCode, 200);

    const body = JSON.parse(result.body);
    assert.equal(body.message, "Hello from Node.js!");
    assert.deepEqual(body.input, payload);
  });

  it("should handle empty payload", async () => {
    const command = new InvokeCommand({
      FunctionName: "python-hello",
      Payload: new TextEncoder().encode(JSON.stringify({})),
    });

    const response = await client.send(command);

    assert.equal(response.StatusCode, 200);

    const result = decodePayload(response.Payload);
    const body = JSON.parse(result.body);
    assert.deepEqual(body.input, {});
  });

  it("should include ExecutedVersion in response", async () => {
    const command = new InvokeCommand({
      FunctionName: "python-hello",
      Payload: new TextEncoder().encode(JSON.stringify({ test: true })),
    });

    const response = await client.send(command);

    assert.equal(response.StatusCode, 200);
    assert.ok(
      response.ExecutedVersion !== undefined,
      "ExecutedVersion should be present"
    );
  });
});

// ---------------------------------------------------------------------------
// Error responses
// ---------------------------------------------------------------------------

describe("Error responses", () => {
  it("should surface Python function errors", async () => {
    const command = new InvokeCommand({
      FunctionName: "python-error",
      Payload: new TextEncoder().encode(JSON.stringify({ trigger: "error" })),
    });

    const response = await client.send(command);

    // Lambda returns 200 with FunctionError for unhandled errors
    assert.equal(response.StatusCode, 200);
    assert.equal(response.FunctionError, "Unhandled");

    const errorPayload = decodePayload(response.Payload);
    assert.ok(errorPayload.errorMessage, "errorMessage should be present");
    assert.ok(errorPayload.errorType, "errorType should be present");
  });

  it("should surface Node.js function errors", async () => {
    const command = new InvokeCommand({
      FunctionName: "nodejs-error",
      Payload: new TextEncoder().encode(JSON.stringify({ trigger: "error" })),
    });

    const response = await client.send(command);

    assert.equal(response.StatusCode, 200);
    assert.equal(response.FunctionError, "Unhandled");

    const errorPayload = decodePayload(response.Payload);
    assert.ok(errorPayload.errorMessage, "errorMessage should be present");
    assert.ok(errorPayload.errorType, "errorType should be present");
  });

  it("should throw ResourceNotFoundException for nonexistent function", async () => {
    const command = new InvokeCommand({
      FunctionName: "nonexistent-function",
      Payload: new TextEncoder().encode(JSON.stringify({})),
    });

    await assert.rejects(async () => await client.send(command), (err) => {
      assert.ok(
        err instanceof ResourceNotFoundException,
        `Expected ResourceNotFoundException, got ${err.constructor.name}`
      );
      return true;
    });
  });
});

// ---------------------------------------------------------------------------
// DryRun invocation
// ---------------------------------------------------------------------------

describe("DryRun invocation", () => {
  it("should return 204 without executing the function", async () => {
    const command = new InvokeCommand({
      FunctionName: "python-hello",
      InvocationType: "DryRun",
      Payload: new TextEncoder().encode(JSON.stringify({})),
    });

    const response = await client.send(command);

    assert.equal(response.StatusCode, 204);
  });
});
