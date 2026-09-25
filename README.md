# protoc-gen-temporal-asyncapi

A [buf](https://buf.build) / `protoc` plugin that turns [Temporal](https://temporal.io) Workflows, Activities,
Signals, Queries and Updates declared as annotated Protobuf **services** into
**[AsyncAPI 3.0 / 3.1](https://www.asyncapi.com/docs/reference/specification/v3.1.0)** documents. The output is
ready to open in [AsyncAPI Studio](https://studio.asyncapi.com) or any other AsyncAPI tool.

Each rpc is one Temporal operation: its request is the payload and its response is the result. The plugin produces:

- one **channel** per operation, plus a result channel when there is a result, with its message bound;
- one **operation** per primitive. Queries, Updates, and Workflow/Activity results use AsyncAPI's `reply`;
- **JSON Schemas** for every payload, derived from the proto definitions and their comments, and optionally
  enriched with [protovalidate](https://github.com/bufbuild/protovalidate) constraints;
- an `x-temporal` **binding** carrying the statically declared task queue, timeouts, retry policy, and continue-as-new
  support;
- **tags** that group each Workflow with its Signal, Query and Update handlers.

The plugin generates documentation only. It does not generate code and never contacts a Temporal cluster. Its
sibling, [protoc-gen-nats-asyncapi](https://github.com/AustinZhu/protoc-gen-nats-asyncapi), does the same for NATS.

## Quick start

### 1. Install the plugin

```sh
go install github.com/AustinZhu/protoc-gen-temporal-asyncapi/cmd/protoc-gen-temporal-asyncapi@latest
```

Prebuilt binaries are also attached to each [GitHub release](https://github.com/AustinZhu/protoc-gen-temporal-asyncapi/releases),
named `protoc-gen-temporal-asyncapi_<version>_<os>_<arch>.tar.gz`.

### 2. Depend on the options

The options are published to the Buf Schema Registry as
**[`buf.build/austin-zhu/temporal-asyncapi`](https://buf.build/austin-zhu/temporal-asyncapi)**.
Each release is labeled with its tag. Add it as a dependency in your `buf.yaml`:

```yaml
version: v2
deps:
  - buf.build/austin-zhu/temporal-asyncapi        # latest
  # - buf.build/austin-zhu/temporal-asyncapi:v0.2.0  # or pin a release label
```

Then run `buf dep update` and `import "temporal/asyncapi/v1/options.proto";`.

Without buf, copy [`proto/temporal/asyncapi/v1/options.proto`](proto/temporal/asyncapi/v1/options.proto) into your
proto tree at `temporal/asyncapi/v1/options.proto`. Each release archive also includes it. The Go bindings are at
`github.com/AustinZhu/protoc-gen-temporal-asyncapi/proto/temporal/asyncapi/v1`.

### 3. Annotate your services

```proto
import "google/protobuf/empty.proto";
import "temporal/asyncapi/v1/options.proto";

service Orders {
  option (temporal.asyncapi.v1.service) = {task_queue: "orders"};

  // Processes a customer order from payment through fulfillment.
  rpc ProcessOrder(ProcessOrderInput) returns (ProcessOrderResult) {
    option (temporal.asyncapi.v1.operation).workflow = {
      continue_as_new: true
      execution_timeout: "720h"
    };
  }

  // Cancels the order if it has not shipped yet.
  rpc CancelOrder(CancelOrderInput) returns (google.protobuf.Empty) {
    option (temporal.asyncapi.v1.operation).signal = {};
  }

  // Returns the order's current status.
  rpc GetStatus(google.protobuf.Empty) returns (OrderStatusResult) {
    option (temporal.asyncapi.v1.operation).query = {};
  }
}
```

- Names default to the rpc name, so `ProcessOrder` is the Workflow type. Set `name` when the registered name differs.
- `google.protobuf.Empty` means "no payload" as a request and "no result" as a response.
- A service's Signals, Queries and Updates belong to its Workflow when it has exactly one. Otherwise, list their rpc
  names in `workflows`.
- Comments on rpcs, messages, fields and enum values become descriptions in the generated document.

Temporal payloads are usually named after their operation and often use `google.protobuf.Empty`, which buf's
`STANDARD` lint category flags. [`buf.yaml`](buf.yaml) shows the `ignore_only` entries the example uses.

### 4. Generate

With buf (`buf.gen.yaml`):

```yaml
version: v2
plugins:
  - local: protoc-gen-temporal-asyncapi
    out: docs/asyncapi
    opt:
      - version=1.0.0
      - server_url=temporal.acme.internal:7233
      - namespace=orders
```

With protoc:

```sh
protoc -I proto --temporal-asyncapi_out=docs/asyncapi --temporal-asyncapi_opt=version=1.0.0 \
  proto/acme/orders/v1/orders.proto
```

Each `.proto` file that declares operations produces `<path>.asyncapi.yaml` next to its source path, for example
`docs/asyncapi/acme/orders/v1/orders.asyncapi.yaml`. Files that declare none produce nothing. With `merge=true`, all
files go into one document instead, `asyncapi.yaml` by default; with buf, also set `strategy: all` so the plugin sees
every file in one run.

[`examples/`](examples) contains a complete setup and its generated
[`orders.asyncapi.yaml`](examples/asyncapi/acme/orders/v1/orders.asyncapi.yaml).

## Annotation reference

`(temporal.asyncapi.v1.service)`, on a service:

| Field | Meaning |
| --- | --- |
| `task_queue` | Default task queue of the service's Workflows and Activities. |

When a service has this option, every one of its rpcs must declare an operation. Without it, unannotated rpcs are
ignored.

`(temporal.asyncapi.v1.operation)`, on an rpc, sets exactly one kind:

| Kind | Fields | Shape |
| --- | --- | --- |
| `workflow` | `name`, `task_queue`, `continue_as_new`, `execution_timeout`, `run_timeout`, `task_timeout`, `retry_policy` | Result is the response; `Empty` for none. |
| `activity` | `name`, `task_queue`, `schedule_to_close_timeout`, `start_to_close_timeout`, `schedule_to_start_timeout`, `heartbeat_timeout`, `retry_policy` | Result is the response; `Empty` for none. |
| `signal` | `name`, `workflows` | Must return `google.protobuf.Empty`. |
| `query` | `name`, `workflows` | Must return a result. |
| `update` | `name`, `workflows` | Result is the response; `Empty` for none. |

- Timeouts and retry intervals are Go-style durations such as `"30s"` or `"1h30m"`.
- `retry_policy` has `initial_interval`, `backoff_coefficient`, `maximum_interval`, `maximum_attempts` and
  `non_retryable_error_types`.
- `workflows` lists rpc names of Workflows in the same service.

Because each kind is its own message, protoc itself rejects fields that don't apply, such as a heartbeat timeout on a
Workflow. The plugin then fails generation, reporting `file:line:column` for every problem at once, when:

- an operation sets no kind, or an annotated service has an unannotated rpc;
- an rpc streams;
- a Signal returns something other than `Empty`, or a Query returns `Empty`;
- `workflows` names something that isn't a Workflow rpc of the same service;
- two rpcs declare the same `(kind, name)`, in any of the files being generated;
- a duration is malformed.

## Options

Pass these as `opt:` entries in buf, or as comma-separated `key=value` pairs in `--temporal-asyncapi_opt`.
`protoc-gen-temporal-asyncapi --help` lists them too.

| Option | Default | Meaning |
| --- | --- | --- |
| `format` | `yaml` | `yaml` or `json`. |
| `merge` | `false` | Write one document for all files instead of one per file. |
| `merge_file_name` | `asyncapi` | Base name of the merged document. |
| `asyncapi_version` | `3.1.0` | `3.1.0` or `3.0.0`. The generated documents are otherwise identical. |
| `perspective` | `client` | `client`: operations `send`, as seen by callers. `worker`: operations `receive`, as seen by the Worker hosting them. |
| `title` | proto package | `info.title` |
| `version` | `0.0.0` | `info.version` |
| `description` | generated overview | `info.description` |
| `id` | `urn:temporal:<package>` | Document `id`. |
| `server_url` | none | Temporal frontend address (`host:port`, or a URL). Emits a `temporal` server. |
| `namespace` | none | Temporal namespace, recorded as `x-temporal-namespace` on the server. |
| `json_names` | `true` | lowerCamelCase JSON field names; `false` uses `.proto` names. Match this to your data converter. |
| `trim_unused_schemas` | `false` | Emit only schemas reachable from an operation. By default every message and enum in the documented files is included. |
| `protovalidate` | `true` | Translate `buf.validate` constraints when `buf/validate/validate.proto` is among the inputs. |

## How Temporal maps onto AsyncAPI

| Temporal | Channel address | Operation | Reply |
| --- | --- | --- | --- |
| Workflow | `workflow/<Name>` | `send`: start the workflow | `workflow/<Name>/{workflowId}/result` |
| Continue-as-new | same channel as the Workflow | `send`: `<id>.continueAsNew` | none |
| Activity | `activity/<Name>` | `send`: schedule the activity | `activity/<Name>/result` |
| Signal | `workflow/{workflowId}/signal/<Name>` | `send`: fire and forget | none |
| Query | `workflow/{workflowId}/query/<Name>` | `send` | `…/query/<Name>/result` |
| Update | `workflow/{workflowId}/update/<Name>` | `send` | `…/update/<Name>/result` |

These are the `client` perspective actions. With `perspective=worker`, every operation except continue-as-new is
`receive`.

- **Keys.** Channel and operation ids are `<kind>.<Name>`, such as `workflow.ProcessOrder` and `signal.CancelOrder`.
- **`{workflowId}`.** This is a channel parameter: Signals, Queries, Updates and Workflow results all target one Workflow
  Execution.
- **Messages.**
  - Each message sets `contentType: application/json`.
  - Its `headers` describe Temporal's payload metadata: `encoding: json/protobuf` and `messageType: <full proto name>`.
  - Its `payload` references the schema under `components.schemas`.
- **Bindings.** These live in `components.channelBindings` and `components.operationBindings` and are referenced by
  `$ref`.
  - AsyncAPI has no official Temporal binding and only accepts `x-` extensions for unknown protocols, hence
    `x-temporal`.
  - Channel bindings carry `kind`, `name` and `taskQueue`.
  - Operation bindings also carry `continueAsNew`, `workflows`, `timeouts` and `retryPolicy`.
- **Tags.**
  - Every operation gets a kind tag: `Workflows`, `Activities`, `Signals`, `Queries` or `Updates`, each linked to the
    Temporal docs.
  - A Workflow and its Signal, Query and Update handlers share a `workflow:<Name>` tag.
  - When more than one package is documented, each operation also gets a proto-package tag.

## How Protobuf maps onto JSON Schema

Schemas describe the **canonical Protobuf JSON** encoding, which is what Temporal's default `json/protobuf` payload
converter produces. They use JSON Schema draft-07 keywords, the dialect AsyncAPI's default schema format builds on.

| Proto | Schema |
| --- | --- |
| `double`, `float` | `number` |
| `int32`, `sint32`, `sfixed32` | `integer` |
| `uint32`, `fixed32` | `integer`, `minimum: 0` |
| `int64`, `sint64`, `sfixed64`, `uint64`, `fixed64` | `integer` or numeric `string`. Protobuf JSON writes 64-bit integers as strings to avoid JavaScript precision loss. |
| `bool` / `string` | `boolean` / `string` |
| `bytes` | `string`, `contentEncoding: base64` |
| enum | `string` limited to the value names, defined once under `components.schemas`, with value comments listed in its description |
| message | `$ref: '#/components/schemas/<package.Message>'` (recursive types work) |
| `repeated T` | `array` of `T` |
| `map<K, V>` | `object` with `additionalProperties: V`. Non-string keys get a `propertyNames` pattern. |
| `oneof` | Members are ordinary properties, plus a `oneOf` that allows at most one of them. With `(buf.validate.oneof).required`, exactly one is required. |
| proto3 `optional` | Same as the plain field. Presence tracking doesn't change the JSON shape. |
| proto2 / editions `required` | Listed in `required`. |
| `[deprecated = true]` | `deprecated: true` |
| `Timestamp` / `Duration` | `string` with `format: date-time` / a `^-?\d+(\.\d+)?s$` pattern |
| wrappers (`StringValue`, …) | The wrapped scalar, nullable. |
| `Struct` / `Value` / `ListValue` | `object` / any value / `array` |
| `Empty` | `object`, `additionalProperties: false` |
| `Any` | `object` with a required `@type` string |
| `FieldMask` | `string` |

### protovalidate

When `buf/validate/validate.proto` is among the plugin's inputs, which it is whenever your protos import it, the plugin
translates common constraints. It reads them dynamically, so the plugin binary has no protovalidate dependency.

| Constraint | JSON Schema |
| --- | --- |
| `string.min_len` / `max_len` / `len` | `minLength` / `maxLength` |
| `string.pattern` | `pattern` |
| `string.email`, `uuid`, `uri`, `uri_ref`, `hostname`, `ipv4`, `ipv6` | `format` |
| `string.const` / `in`, numeric `const` / `in` | `const` / `enum` |
| numeric `gt` / `gte` / `lt` / `lte` | `exclusiveMinimum` / `minimum` / `exclusiveMaximum` / `maximum` |
| `repeated.min_items` / `max_items` / `unique` / `items` | `minItems` / `maxItems` / `uniqueItems` / rules applied to `items` |
| `map.min_pairs` / `max_pairs` / `values` | `minProperties` / `maxProperties` / rules applied to the values |
| `required: true` | Listed in the parent's `required`. |

Constraints JSON Schema can't express are skipped rather than approximated. These include CEL expressions and
"outside the range" bounds.

## Why AsyncAPI and not OpenAPI?

Temporal isn't a REST API. There are no resources, verbs or status codes. What callers interact with is a set of
named destinations: a workflow type, an activity type, or a signal on a running execution. Each one accepts a typed
message and, for some primitives, answers with another. That is the shape AsyncAPI was built for: channels with bound
message schemas, and operations that send or receive on them.

AsyncAPI 3's `reply` object is what makes the request/reply primitives representable. A Query or Update is a
synchronous request with a typed answer, and a Workflow or Activity result is the eventual reply to a start. OpenAPI
could only fake these as HTTP endpoints that don't exist.

## Development

`make` runs everything CI runs. The individual targets:

```sh
make test       # unit and golden tests; every golden document is validated against the AsyncAPI 3.0.0/3.1.0 JSON Schemas
make golden     # accept changed golden files under internal/generator/testdata/golden
make lint       # go vet, gofmt, buf lint and buf format
make generate   # regenerate the Go bindings after editing options.proto
make examples   # regenerate examples/asyncapi
make validate   # also check every golden document with the official @asyncapi/parser (needs Node.js)
make check      # fail if generated files are out of date
```

Test protos live in `internal/generator/testdata/protos`, and each golden case writes its output under
`internal/generator/testdata/golden/<case>/`. The example in `examples/proto` is a golden case too.

### Releasing

Cut a release in either of two ways:

- push a `v*` tag;
- run the **release** workflow manually from the default branch with a version. The workflow creates the tag.

The workflow then runs these steps in order. If any step fails, the later ones don't run:

1. Tests and spec validation, the same checks as CI.
2. BSR checks: `buf lint` on the `proto` module, `buf breaking` against the previous release tag, and a check that
   `buf.yaml` names the module and that the `BUF_TOKEN` secret is set. Breaking changes are allowed only on a major
   version bump, or on a minor bump while the major version is 0.
3. GoReleaser publishes the GitHub release and binaries. For a manual run, the tag is created just before this.
4. Only the `proto` module (`temporal/asyncapi/v1/options.proto`) is pushed to `buf.build/austin-zhu/temporal-asyncapi`,
   labeled with the tag. The example module is never published, and nothing is published from pull requests, forks or
   branch pushes.

Before the first BSR release, the owner must do two things:

1. Create the `buf.build/austin-zhu/temporal-asyncapi` module on the BSR. The workflow deliberately doesn't
   pass `--create`, so the token never needs permission to create modules.
2. Add a `BUF_TOKEN` Actions secret containing a token for a bot user that has write access to that module only.

The token is passed to `buf push` through the environment and is never logged or written to disk.
