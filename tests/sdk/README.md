# SDK Compatibility Tests

These tests verify that standard AWS SDKs can invoke Lambda functions through
localfunctions using only an endpoint URL override — no custom HTTP logic.

## Prerequisites

1. **localfunctions running** with test functions configured (see below)
2. **Docker** running with Lambda runtime images pre-pulled
3. **Python 3.9+** with boto3 installed
4. **Node.js 18+** with `@aws-sdk/client-lambda` installed

## Setup

### 1. Configure test functions

Create or update `functions.json` in the project root:

```json
{
  "functions": {
    "python-hello": {
      "runtime": "python3.12",
      "handler": "main.handler",
      "code_path": "./tests/fixtures/python-hello",
      "timeout": 30,
      "memory_size": 128
    },
    "python-error": {
      "runtime": "python3.12",
      "handler": "main.handler",
      "code_path": "./tests/fixtures/python-error",
      "timeout": 30,
      "memory_size": 128
    },
    "nodejs-hello": {
      "runtime": "nodejs20.x",
      "handler": "index.handler",
      "code_path": "./tests/fixtures/nodejs-hello",
      "timeout": 30,
      "memory_size": 128
    },
    "nodejs-error": {
      "runtime": "nodejs20.x",
      "handler": "index.handler",
      "code_path": "./tests/fixtures/nodejs-error",
      "timeout": 30,
      "memory_size": 128
    }
  },
  "runtime_images": {}
}
```

### 2. Pull Lambda runtime images

```bash
docker pull public.ecr.aws/lambda/python:3.12
docker pull public.ecr.aws/lambda/nodejs:20
```

### 3. Start the test network and server

```bash
docker compose -f docker-compose.test.yml up -d
cargo run
```

The server starts on `http://localhost:9600` by default.

### 4. Install test dependencies

**Python:**
```bash
cd tests/sdk
pip install -r requirements.txt
```

**Node.js:**
```bash
cd tests/sdk
npm install
```

### 5. Run the tests

**Python (boto3):**
```bash
cd tests/sdk
python -m pytest test_boto3.py -v
```

**Node.js (aws-sdk v3):**
```bash
cd tests/sdk
npm test
```

**Both:**
```bash
cd tests/sdk
./run_all.sh
```

## Environment Variables

| Variable                 | Default                 | Description                    |
|--------------------------|-------------------------|--------------------------------|
| `LOCALFUNCTIONS_ENDPOINT`| `http://localhost:9600` | localfunctions Invoke API URL  |

## What these tests verify

- Standard SDK invocations work with only an endpoint URL override
- Successful function invocations return correct payloads
- Error responses match AWS Lambda error format
- Both Python and Node.js runtimes produce correct results
- No custom HTTP logic is needed — only `endpoint_url` (boto3) or `endpoint` (aws-sdk v3)
