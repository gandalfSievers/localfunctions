# localfunctions

A local AWS Lambda emulator written in Rust. Run Lambda functions locally using real AWS Lambda runtime containers, supporting all major languages.

## Quick start

### Prerequisites

- [Rust 1.83+](https://rustup.rs/) (for building from source)
- [Docker](https://docs.docker.com/get-docker/) (required for running Lambda containers)

### Setup

1. Copy the example config and create your first function:

```sh
cp functions.json.example functions.json
cp .env.example .env
mkdir -p functions/my-python-func
```

2. Create a handler in `functions/my-python-func/main.py`:

```python
def handler(event, context):
    return {"statusCode": 200, "body": f"Hello, {event.get('name', 'World')}!"}
```

3. Pull the required runtime image:

```sh
docker pull public.ecr.aws/lambda/python:3.12
```

4. Start localfunctions:

```sh
# From source
cargo run --release

# Or via Docker
docker compose up -d
```

5. Invoke your function:

```sh
curl -X POST http://localhost:9600/2015-03-31/functions/my-python-func/invocations \
  -d '{"name": "Lambda"}'
```

## Configuration

### functions.json

Defines the functions to serve and which runtime images to use.

```json
{
  "functions": {
    "my-python-func": {
      "runtime": "python3.12",
      "handler": "main.handler",
      "code_path": "./functions/my-python-func",
      "timeout": 30,
      "memory_size": 128,
      "environment": {
        "TABLE_NAME": "my-table"
      }
    },
    "my-node-func": {
      "runtime": "nodejs20.x",
      "handler": "index.handler",
      "code_path": "./functions/my-node-func",
      "timeout": 10
    }
  },
  "runtime_images": {
    "python3.12": "public.ecr.aws/lambda/python:3.12",
    "nodejs20.x": "public.ecr.aws/lambda/nodejs:20",
    "provided.al2023": "public.ecr.aws/lambda/provided:al2023"
  }
}
```

#### Function properties

| Property | Required | Default | Description |
|---|---|---|---|
| `runtime` | Yes* | - | Runtime identifier (e.g. `python3.12`, `nodejs20.x`, `java21`, `dotnet8`, `ruby3.3`, `provided.al2023`, `custom`) |
| `handler` | Yes* | - | Entry point (e.g. `main.handler`, `index.handler`) |
| `code_path` | Yes* | - | Path to function code directory |
| `timeout` | No | `30` | Invocation timeout in seconds (1-900) |
| `memory_size` | No | `128` | Memory in MB |
| `ephemeral_storage_mb` | No | `512` | /tmp size in MB (512-10240) |
| `environment` | No | `{}` | Environment variables passed to the function |
| `image` | No | - | Custom Docker image (for `custom` runtime) |
| `image_uri` | No | - | Complete OCI image URI (bypasses runtime images) |
| `architecture` | No | `x86_64` | `x86_64` or `arm64` |
| `layers` | No | `[]` | Local directory paths mounted as /opt layers |
| `function_url_enabled` | No | `false` | Enable HTTP endpoint at `/{function_name}/` |
| `reserved_concurrent_executions` | No | - | Per-function concurrency limit (1-1000) |
| `max_retry_attempts` | No | `2` | Async invocation retry count (0-2) |
| `on_success` | No | - | Destination function for successful async invocations |
| `on_failure` | No | - | Destination function after all retries exhausted |

*Not required when using `image_uri`.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `LOCAL_LAMBDA_HOST` | `0.0.0.0` | Bind address |
| `LOCAL_LAMBDA_PORT` | `9600` | Invoke API port |
| `LOCAL_LAMBDA_RUNTIME_PORT` | `9601` | Runtime API port (containers connect here) |
| `LOCAL_LAMBDA_REGION` | `us-east-1` | AWS region for ARN generation |
| `LOCAL_LAMBDA_ACCOUNT_ID` | `000000000000` | AWS account ID for ARN generation |
| `LOCAL_LAMBDA_FUNCTIONS_FILE` | `./functions.json` | Path to function definitions |
| `LOCAL_LAMBDA_LOG_LEVEL` | `info` | Log level (`trace`, `debug`, `info`, `warn`, `error`) |
| `LOCAL_LAMBDA_LOG_FORMAT` | `json` | Log format (`json` or `text`) |
| `LOCAL_LAMBDA_SHUTDOWN_TIMEOUT` | `30` | Graceful shutdown timeout (seconds) |
| `LOCAL_LAMBDA_CONTAINER_IDLE_TIMEOUT` | `300` | Idle container removal timeout (seconds) |
| `LOCAL_LAMBDA_MAX_CONTAINERS` | `20` | Maximum concurrent containers |
| `LOCAL_LAMBDA_DOCKER_NETWORK` | `localfunctions` | Docker network name |
| `LOCAL_LAMBDA_PULL_IMAGES` | `false` | Pull missing images on startup |
| `LOCAL_LAMBDA_INIT_TIMEOUT` | `10` | Container bootstrap timeout (seconds) |
| `LOCAL_LAMBDA_CONTAINER_ACQUIRE_TIMEOUT` | `10` | Wait for container slot (seconds) |
| `LOCAL_LAMBDA_MAX_BODY_SIZE` | `6291456` | Sync invocation payload limit (bytes, 6 MB) |
| `LOCAL_LAMBDA_MAX_ASYNC_BODY_SIZE` | `262144` | Async invocation payload limit (bytes, 256 KB) |
| `LOCAL_LAMBDA_FORWARD_AWS_CREDENTIALS` | `true` | Forward `AWS_*` env vars to containers |
| `LOCAL_LAMBDA_MOUNT_AWS_CREDENTIALS` | `false` | Mount `~/.aws` directory into containers |
| `LOCAL_LAMBDA_HOT_RELOAD` | `true` | Watch code paths and recycle containers on changes |
| `LOCAL_LAMBDA_HOT_RELOAD_DEBOUNCE_MS` | `500` | File change debounce interval |
| `LOCAL_LAMBDA_DOMAIN` | - | Custom domain for virtual host routing (e.g. `lambda.local`). Enables `{function}.{domain}` addressing via `Host` header |

Copy `.env.example` to `.env` to set these locally.

## API

### Invoke a function

```sh
# Synchronous (RequestResponse)
curl -X POST http://localhost:9600/2015-03-31/functions/{FunctionName}/invocations \
  -d '{"key": "value"}'

# Asynchronous (fire-and-forget)
curl -X POST http://localhost:9600/2015-03-31/functions/{FunctionName}/invocations \
  -H "X-Amz-Invocation-Type: Event" \
  -d '{"key": "value"}'

# Dry run (validate only)
curl -X POST http://localhost:9600/2015-03-31/functions/{FunctionName}/invocations \
  -H "X-Amz-Invocation-Type: DryRun" \
  -d '{}'

# Streaming response
curl -X POST http://localhost:9600/2021-11-15/functions/{FunctionName}/response-streaming-invocations \
  -d '{"key": "value"}'
```

### List functions

```sh
curl http://localhost:9600/2015-03-31/functions
```

### Get function details

```sh
curl http://localhost:9600/2015-03-31/functions/{FunctionName}
```

### Health check

```sh
curl http://localhost:9600/health
```

### Metrics

```sh
curl http://localhost:9600/metrics
```

### Function URLs

When `function_url_enabled` is set, the function is accessible at:

```sh
curl http://localhost:9600/{FunctionName}/
```

### Using the AWS SDK

Point the SDK endpoint to localfunctions:

```python
import boto3

client = boto3.client(
    "lambda",
    endpoint_url="http://localhost:9600",
    region_name="us-east-1",
    aws_access_key_id="testing",
    aws_secret_access_key="testing",
)

response = client.invoke(
    FunctionName="my-python-func",
    Payload=b'{"name": "Lambda"}',
)
print(response["Payload"].read().decode())
```

```javascript
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";

const client = new LambdaClient({
  endpoint: "http://localhost:9600",
  region: "us-east-1",
  credentials: { accessKeyId: "testing", secretAccessKey: "testing" },
});

const response = await client.send(
  new InvokeCommand({
    FunctionName: "my-python-func",
    Payload: JSON.stringify({ name: "Lambda" }),
  })
);
```

## Supported runtimes

| Runtime | Example identifier |
|---|---|
| Python | `python3.11`, `python3.12` |
| Node.js | `nodejs18.x`, `nodejs20.x` |
| Java | `java11`, `java17`, `java21` |
| .NET | `dotnet6`, `dotnet7`, `dotnet8` |
| Ruby | `ruby3.2`, `ruby3.3` |
| Custom (AL2023) | `provided.al2023` |
| Custom image | `custom` (with `image` field) |
| Full OCI image | any (with `image_uri` field) |

Map runtime identifiers to Docker images in the `runtime_images` section of `functions.json`.

## Using with other local AWS services

localfunctions works well alongside other local AWS service emulators. Pass service endpoints to your functions via environment variables:

```json
{
  "functions": {
    "my-func": {
      "runtime": "python3.12",
      "handler": "main.handler",
      "code_path": "./functions/my-func",
      "environment": {
        "AWS_ENDPOINT_URL_S3": "http://host.docker.internal:9090",
        "AWS_ENDPOINT_URL_SECRETS_MANAGER": "http://host.docker.internal:9091"
      }
    }
  }
}
```

Use `host.docker.internal` to reach services running on the Docker host from inside Lambda containers.

### TLS termination with Traefik

localfunctions does not handle TLS itself. To serve functions over HTTPS (e.g. to fully emulate `lambda.<region>.amazonaws.com`), put [Traefik](https://doc.traefik.io/traefik/) in front on the same Docker network:

```yaml
services:
  traefik:
    image: traefik:v3
    command:
      - --entrypoints.https.address=:443
      - --providers.docker=true
    ports:
      - "443:443"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - ./certs:/certs:ro
    networks:
      - localservices

  localfunctions:
    # ... existing localfunctions service ...
    labels:
      - "traefik.http.routers.lambda.rule=HostRegexp(`{name:.+}.lambda.${LOCAL_LAMBDA_REGION:-us-east-1}.amazonaws.com`)"
      - "traefik.http.routers.lambda.tls=true"
      - "traefik.http.routers.lambda.tls.certresolver=default"
      - "traefik.http.services.lambda.loadbalancer.server.port=9600"
    networks:
      - localservices

networks:
  localservices:
    name: localservices
```

Pair this with a local DNS resolver (e.g. dnsmasq) that points `*.amazonaws.com` at Traefik, and your AWS SDKs can connect over TLS just like in production.

## Docker

### Run with Docker Compose

```sh
docker compose up -d
```

The `docker-compose.yml` mounts the Docker socket (required for managing Lambda containers), `functions.json`, and the `functions/` directory.

### Build images

```sh
# Debian-based image
make docker-build-debian

# Alpine-based image (smaller)
make docker-build-alpine

# Multi-architecture (amd64 + arm64)
make docker-build-multi
```

## Development

```sh
# Build
make build

# Run locally
make run

# Run tests
make test              # unit tests
make test-integration  # integration tests (requires Docker)
make test-all          # both

# Lint
make lint              # fmt + clippy
```

## Architecture

localfunctions runs two HTTP servers:

- **Invoke API** (port 9600) - External-facing. Receives invocation requests from clients and AWS SDKs.
- **Runtime API** (port 9601) - Internal. Implements the [Lambda Runtime API](https://docs.aws.amazon.com/lambda/latest/dg/runtimes-api.html) that Lambda containers use to fetch work and return results.

When a function is invoked:

1. The Invoke API validates the request and queues it on the function's channel.
2. A container is acquired (reusing a warm container or cold-starting a new one).
3. The container long-polls the Runtime API at `/2018-06-01/runtime/invocation/next`.
4. The container processes the event and posts the result back to the Runtime API.
5. The result is returned to the original caller.

Idle containers are kept warm for 5 minutes (configurable) and reused across invocations. Hot reload watches function code directories and recycles containers when files change.

## License

See [LICENSE](LICENSE).
