# protoc-gen-asyncapi

A [buf](https://buf.build) / `protoc` plugin that turns annotated Protobuf **services** into
**[AsyncAPI 3.0 / 3.1](https://www.asyncapi.com/docs/reference/specification/v3.1.0)** documents, for any messaging
protocol it knows about. The output is ready to open in [AsyncAPI Studio](https://studio.asyncapi.com) or any other
AsyncAPI tool.

The repository is one shared core plus one package per protocol:

| Part | Annotations | What it covers |
| --- | --- | --- |
| **Core** | `asyncapi/v3/annotations.proto` (`buf.build/austin-zhu/asyncapi`) | The whole AsyncAPI 3.0/3.1 object model: info, servers, security schemes and OAuth flows, channels, parameters, operations, replies, messages, headers, examples, tags, external docs, bindings and `x-` extensions. JSON Schemas for every payload, from the proto definitions, comments, [protovalidate](https://github.com/bufbuild/protovalidate) rules and `google.api.field_behavior`. |
| **NATS** | `nats/asyncapi/v1/annotations.proto` (`buf.build/austin-zhu/nats-asyncapi`) | Core NATS publish/subscribe, request/reply, queue groups, [NATS micro](https://github.com/nats-io/nats.go/tree/main/micro) services, JetStream streams and consumers, KV buckets and object stores. |
| **Temporal** | `temporal/asyncapi/v1/options.proto` (`buf.build/austin-zhu/temporal-asyncapi`) | [Temporal](https://temporal.io) Workflows, Activities, Signals, Queries and Updates, with task queues, timeouts, retry policies and continue-as-new. |

Three binaries are built from it:

- **`protoc-gen-asyncapi`** documents every protocol. One document may mix NATS and Temporal services.
- **`protoc-gen-nats-asyncapi`** and **`protoc-gen-temporal-asyncapi`** document only their own protocol and ignore
  the other's annotations.

The plugin generates documentation only. It generates no code and never contacts a server.

## Quick start

### 1. Install

```sh
go install github.com/AustinZhu/protoc-gen-asyncapi/cmd/protoc-gen-asyncapi@latest
# or only one protocol:
go install github.com/AustinZhu/protoc-gen-asyncapi/cmd/protoc-gen-temporal-asyncapi@latest
go install github.com/AustinZhu/protoc-gen-asyncapi/cmd/protoc-gen-nats-asyncapi@latest
```

Prebuilt binaries are attached to each [GitHub release](https://github.com/AustinZhu/protoc-gen-asyncapi/releases),
named `<binary>_<version>_<os>_<arch>.tar.gz`.

### 2. Depend on the annotations

The annotations are published to the Buf Schema Registry. Every release is labeled with its tag. Add the core module
and the protocols you use to your `buf.yaml`:

```yaml
version: v2
deps:
  - buf.build/austin-zhu/asyncapi            # asyncapi.v3: document, service, operation, message and field options
  - buf.build/austin-zhu/temporal-asyncapi   # temporal.asyncapi.v1
  - buf.build/austin-zhu/nats-asyncapi       # nats.asyncapi.v1
  # pin a release with :v0.3.0
```

Then run `buf dep update`. Without buf, copy the `.proto` files under [`proto/`](proto) into your proto tree; each
release archive includes them too. The Go bindings live under
`github.com/AustinZhu/protoc-gen-asyncapi/pb/...`.

### 3. Annotate

A Temporal service:

```proto
import "asyncapi/v3/annotations.proto";
import "google/protobuf/empty.proto";
import "temporal/asyncapi/v1/options.proto";

option (asyncapi.v3.document) = {
  info: {title: "Acme Shop Workflows", version: "1.0.0"}
  servers: [{name: "production", host: "acme.tmprl.cloud:7233"}]
};
option (temporal.asyncapi.v1.document) = {
  servers: [{name: "production", namespace: "acme-shop.a1b2c"}]
};

service Orders {
  option (temporal.asyncapi.v1.service) = {task_queue: "orders"};

  // Processes a customer order from payment through fulfillment.
  rpc ProcessOrder(ProcessOrderInput) returns (ProcessOrderResult) {
    option (temporal.asyncapi.v1.operation).workflow = {continue_as_new: true, execution_timeout: "720h"};
  }

  // Cancels the order if it has not shipped yet.
  rpc CancelOrder(CancelOrderInput) returns (google.protobuf.Empty) {
    option (temporal.asyncapi.v1.operation).signal = {};
  }
}
```

A NATS service:

```proto
import "asyncapi/v3/annotations.proto";
import "nats/asyncapi/v1/annotations.proto";

service OrderService {
  option (nats.asyncapi.v1.service) = {subject_prefix: "orders", queue_group: "orders-api"};

  // Places an order.
  rpc PlaceOrder(PlaceOrderRequest) returns (PlaceOrderResponse) {
    option (nats.asyncapi.v1.operation) = {pattern: PATTERN_REQUEST_REPLY, subject: "place"};
    option (asyncapi.v3.operation) = {tags: [{name: "checkout"}]};
  }
}
```

[`examples/`](examples) has a complete setup for both protocols and the documents it generates:
[NATS](examples/asyncapi/acme/orders/v1/orders.asyncapi.yaml) and
[Temporal](examples/asyncapi/acme/shop/v1/orders.asyncapi.yaml).

### 4. Generate

```yaml
# buf.gen.yaml
version: v2
plugins:
  - local: protoc-gen-asyncapi
    out: docs/asyncapi
    opt:
      - version=1.0.0
```

```sh
protoc -I proto --asyncapi_out=docs/asyncapi proto/acme/shop/v1/orders.proto
```

Each `.proto` file that declares something to document produces `<path>.asyncapi.yaml` next to its source path.
Files that declare nothing produce nothing. With `merge=true`, all files go into one document, `asyncapi.yaml` by
default; with buf, also set `strategy: all` so the plugin sees every file in one run.

## Options

Pass these as `opt:` entries in buf, or as comma-separated `key=value` pairs in `--asyncapi_opt`. `--help` lists
them for each binary.

| Option | Default | Meaning |
| --- | --- | --- |
| `format` | `yaml` | `yaml`, `json`, or `jsonschema`: a standalone JSON Schema (draft 2020-12) of the payloads, `<path>.schema.json`. |
| `asyncapi_version` | `3.1.0` | `3.1.0` or `3.0.0`. |
| `merge` | `false` | Write one document for all files instead of one per file. |
| `merge_file_name` | `asyncapi` | Base name of the merged document. |
| `perspective` | `server` | `server` (or `worker`): the document describes the application handling the operations, which `receive`. `client`: operations `send`, as seen by callers. |
| `payload` | `jsonschema` | `jsonschema`, or `protobuf` to embed the `.proto` source as the payload schema. |
| `services` | all | Fully-qualified service glob, such as `acme.orders.**`. Repeatable. |
| `title` | service or proto package | `info.title`. |
| `version` | `0.0.0` | `info.version`. |
| `description` | the file's leading comments | `info.description`. |
| `id` | none | Document `id`. |
| `content_type` | per protocol | Default content type of messages. |
| `json_names` | `true` | lowerCamelCase JSON field names; `false` uses `.proto` names. |
| `enum_values` | `names` | `names`, `numbers` or `both`. |
| `enums_as_ints` | `false` | Shorthand for `enum_values=numbers`. |
| `proto_types` | `false` | Annotate every property with its `x-protobuf-type`. |
| `trim_unused_schemas` | `false` | Emit only schemas reachable from an operation. By default every message and enum of the documented files is included. |
| `protovalidate` | `true` | Translate `buf.validate` constraints when `buf/validate/validate.proto` is among the inputs. |
| `server_url` | none | Temporal only: frontend address (`host:port`, or a URL). Adds a `temporal` server. |
| `namespace` | none | Temporal only: namespace of the document's Temporal servers that don't set one, as `x-temporal-namespace`. |
| `include_all` | `false` | NATS only: also document services without NATS annotations. |

The same settings, and everything else about the document (servers, security schemes, tags, external docs and so
on), can also be declared with `(asyncapi.v3.document)` in the proto itself, so each file can carry its own. When
both are given, the plugin option wins.

## The core annotations

`asyncapi/v3/annotations.proto` mirrors the AsyncAPI object model, so a protocol package only has to describe what is
specific to its protocol. Its options apply to any protocol:

| Option | On | Sets |
| --- | --- | --- |
| `(asyncapi.v3.document)` | file | `id`, `info` (contact, license, tags, external docs), `default_content_type`, `servers` (with variables, security, bindings and extensions), `security_schemes` (every type, OAuth flows), tags, external docs and document extensions. |
| `(asyncapi.v3.service)` | service | Tags, external docs, security and servers shared by every operation; `skip`. |
| `(asyncapi.v3.operation)` | rpc | Operation id, title, summary, description, tags, external docs, security, reply address, bindings and extensions; the channel's id, title, parameters, servers, bindings and extensions; `skip`. |
| `(asyncapi.v3.message)` | message | Name, title, summary, content type, headers, examples, correlation id, tags, bindings and extensions. |
| `(asyncapi.v3.field)` | field | Examples, `required`, `format`, `correlation_id`, `hidden`. |

Bindings take a protocol name and a JSON value. The plugin accepts only the protocols the chosen AsyncAPI version
defines for that object, or `x-` keys; 3.1 adds `ros2`. Every generated document is checked against the rules of the
specification before it is written, and generation fails with `file:line:column` for every problem at once.

## Temporal

| Temporal | Channel address | Operation | Reply |
| --- | --- | --- | --- |
| Workflow | `workflow/<Name>` | start the workflow | `workflow/<Name>/{workflowId}/result` |
| Continue-as-new | same channel as the Workflow | `<id>.continueAsNew` | none |
| Activity | `activity/<Name>` | schedule the activity | `activity/<Name>/result` |
| Signal | `workflow/{workflowId}/signal/<Name>` | fire and forget | none |
| Query | `workflow/{workflowId}/query/<Name>` | request | `…/query/<Name>/result` |
| Update | `workflow/{workflowId}/update/<Name>` | request | `…/update/<Name>/result` |

`(temporal.asyncapi.v1.operation)` sets exactly one kind:

| Kind | Fields | Shape |
| --- | --- | --- |
| `workflow` | `name`, `task_queue`, `continue_as_new`, `execution_timeout`, `run_timeout`, `task_timeout`, `retry_policy` | Result is the response; `Empty` for none. |
| `activity` | `name`, `task_queue`, `schedule_to_close_timeout`, `start_to_close_timeout`, `schedule_to_start_timeout`, `heartbeat_timeout`, `retry_policy` | Result is the response; `Empty` for none. |
| `signal` | `name`, `workflows` | Must return `google.protobuf.Empty`. |
| `query` | `name`, `workflows` | Must return a result. |
| `update` | `name`, `workflows` | Result is the response; `Empty` for none. |

- Names default to the rpc name. `(temporal.asyncapi.v1.service).task_queue` is the default task queue.
- A service's Signals, Queries and Updates belong to its Workflow when it has exactly one. Otherwise, list their rpc
  names in `workflows`.
- Durations are Go-style, such as `"30s"` or `"1h30m"`.
- `(temporal.asyncapi.v1.document).servers` gives a server its Temporal namespace.
- Messages carry Temporal's payload metadata as headers (`encoding`, `messageType`); channels and operations carry an
  `x-temporal` binding with the task queue, timeouts, retry policy and handler links.
- Operations are tagged with their kind and `workflow:<Name>`, grouping each Workflow with its handlers.

The plugin rejects streaming rpcs, a Signal with a result, a Query without one, unknown `workflows` references,
duplicate `(kind, name)` pairs across all files, and malformed durations.

## NATS

`nats/asyncapi/v1/annotations.proto` covers subjects with `{parameters}` and wildcards, publish/subscribe,
request/reply, queue groups, NATS micro services (with their `$SRV` endpoints), JetStream streams, consumers and
publishing, KV buckets, object stores, and NATS server details (account, JetStream domain and API prefix, TLS,
authentication). The annotation file documents every field.

## How Protobuf maps onto JSON Schema

Schemas describe the canonical **Protobuf JSON** encoding.

| Proto | Schema |
| --- | --- |
| `double`, `float` | `number` |
| 32-bit integers | `integer`, with `minimum: 0` when unsigned |
| 64-bit integers | `integer` or numeric `string`, as Protobuf JSON writes them as strings |
| `bool` / `string` / `bytes` | `boolean` / `string` / base64 `string` |
| enum | value names, numbers or both (`enum_values`), with value comments in the description |
| message | `$ref` to `components.schemas` (recursive types work) |
| `repeated T` / `map<K, V>` | `array` / `object` with `additionalProperties` |
| `oneof` | at most one member, or exactly one with `(buf.validate.oneof).required` |
| well-known types | their JSON forms: `Timestamp` is `date-time`, wrappers are nullable, `Struct` is an object, and so on |
| `[deprecated = true]` | `deprecated: true` |
| `google.api.field_behavior` | `REQUIRED` → `required`, `OUTPUT_ONLY` → `readOnly`, `INPUT_ONLY` → `writeOnly` |

When `buf/validate/validate.proto` is among the inputs, protovalidate constraints become `minLength`, `pattern`,
`format`, `minimum`, `minItems`, `required` and the like. Constraints JSON Schema can't express, such as CEL, are
skipped rather than approximated.

## Adding a protocol

A protocol implements `core.Protocol` (`internal/core/builder.go`): it claims the services it understands and adds
channels, messages and operations through `core.Builder`. The core handles everything else: documents, servers,
security, tags, schemas, bindings, validation and output. See `internal/temporal` and `internal/nats`, then register
the protocol in `internal/cli/plugins.go` and give it an annotations module under `proto/`.

## Development

`make` runs everything CI runs:

```sh
make test       # unit and golden tests; every golden document is validated against the AsyncAPI JSON Schemas
make golden     # accept changed golden files under internal/*/testdata/golden
make lint       # go vet, gofmt, buf lint and buf format
make generate   # regenerate the Go bindings under pb/
make examples   # regenerate examples/asyncapi
make validate   # check every golden and example document with the official @asyncapi/parser (needs Node.js)
make check      # fail if generated files are out of date
```

### Releasing

Push a `v*` tag, or run the **release** workflow manually with a version. The workflow runs, stopping at the first
failure:

1. Tests and spec validation, as in CI.
2. For each of `proto/asyncapi`, `proto/nats` and `proto/temporal`: `buf lint`, `buf breaking` against the previous
   release tag (breaking changes are allowed on a major bump, or a minor bump while on v0), and a check that
   `buf.yaml` names the module. It also checks that the `BUF_TOKEN` secret is set.
3. GoReleaser publishes the GitHub release and the three binaries.
4. The three annotation modules are pushed to the BSR, labeled with the tag. The example modules are never published,
   and nothing is published from pull requests, forks or branch pushes.

Before a release, the owner must create each module on the BSR (the workflow doesn't pass `--create`) and give the
`BUF_TOKEN` bot user write access to those three modules only. The token is passed to `buf push` through the
environment and is never logged or written to disk.
