# protoc-gen-temporal-asyncapi

A [buf](https://buf.build) / `protoc` plugin that turns Protobuf messages used as
[Temporal](https://temporal.io) payloads into an **[AsyncAPI 3](https://www.asyncapi.com/docs/reference/specification/v3.1.0)**
document describing your Workflows, Activities, Signals, Queries and Updates. The output is ready to open in
[AsyncAPI Studio](https://studio.asyncapi.com) or any other AsyncAPI tool.

You annotate each input message with one option. The plugin produces:

- one **channel** per operation, plus a result channel when there is a result, with its message bound;
- one **operation** per primitive. Queries, Updates, and Workflow/Activity results use AsyncAPI's `reply`;
- **JSON Schemas** for every payload, derived from the proto definitions and their comments, and optionally
  enriched with [protovalidate](https://github.com/bufbuild/protovalidate) constraints;
- an `x-temporal` **binding** carrying the statically declared task queue, timeouts, retry policy, and continue-as-new
  support;
- **tags** that group each Workflow with its Signal, Query and Update handlers.

The plugin generates documentation only. It does not generate code and never contacts a Temporal cluster.

## Quick start

### 1. Install the plugin

```sh
go install github.com/AustinZhu/protoc-gen-temporal-asyncapi@latest
```

Prebuilt binaries are also attached to each [GitHub release](https://github.com/AustinZhu/protoc-gen-temporal-asyncapi/releases),
named `protoc-gen-temporal-asyncapi_<version>_<os>_<arch>.tar.gz`.

### 2. Add the options file

The options are published to the Buf Schema Registry as
**[`buf.build/austin-zhu/protoc-gen-temporal-asyncapi`](https://buf.build/austin-zhu/protoc-gen-temporal-asyncapi)**.
Each release is labeled with its tag. Add it as a dependency in your `buf.yaml`:

```yaml
version: v2
deps:
  - buf.build/austin-zhu/protoc-gen-temporal-asyncapi        # latest
  # - buf.build/austin-zhu/protoc-gen-temporal-asyncapi:v0.2.0  # or pin a release label
```

Then run `buf dep update` and `import "temporal/v1/options.proto";`.

Without buf, copy [`proto/temporal/v1/options.proto`](proto/temporal/v1/options.proto) into your proto tree at
`temporal/v1/options.proto`. Each release archive also includes it. If you also want to use it from Go, the generated
bindings are at `github.com/AustinZhu/protoc-gen-temporal-asyncapi/gen/temporalv1`.

### 3. Annotate your input messages

```proto
import "temporal/v1/options.proto";

// Processes a customer order from payment through fulfillment.
message ProcessOrderInput {
  option (temporal.v1.operation) = {
    kind: OPERATION_KIND_WORKFLOW
    result: "ProcessOrderResult"       // same package; may be nested: "Outer.Inner"
    task_queue: "orders"
    supports_continue_as_new: true
    timeouts: {execution: "720h"}
  };
  string order_id = 1;
}

message ProcessOrderResult { OrderStatus status = 1; }

// Cancels the order if it has not shipped yet.
message CancelOrderSignal {
  option (temporal.v1.operation) = {
    kind: OPERATION_KIND_SIGNAL
    name: "CancelOrder"
    workflows: "ProcessOrder"          // groups the handler with its workflow
  };
  string reason = 1;
}
```

`name` is optional. By default it is the message name with an `Input`, `Request`, `Args` or `Params` suffix removed,
so `ProcessOrderInput` becomes the `ProcessOrder` workflow. Comments on messages, fields and enum values become
descriptions in the generated document.

### 4. Generate

With buf (`buf.gen.yaml`):

```yaml
version: v2
plugins:
  - local: protoc-gen-temporal-asyncapi
    out: docs
    strategy: all            # one document for all inputs; required when protos span several directories
    opt:
      - title=Acme Orders
      - version=1.0.0
      - server-url=temporal.acme.internal:7233
      - namespace=orders
```

With protoc:

```sh
protoc -I proto -I third_party \
  --temporal-asyncapi_out=docs \
  --temporal-asyncapi_opt=title=Acme\ Orders,version=1.0.0 \
  proto/acme/orders/v1/*.proto
```

[`example/`](example) contains a complete setup and its generated
[`asyncapi.yaml`](example/gen/asyncapi.yaml). To check a document against the spec, run
`npx @asyncapi/cli validate docs/asyncapi.yaml`.

## Annotation reference

| Field | Applies to | Meaning |
| --- | --- | --- |
| `kind` | all | **Required.** `OPERATION_KIND_WORKFLOW`, `_ACTIVITY`, `_SIGNAL`, `_QUERY` or `_UPDATE`. |
| `name` | all | Name registered with the worker. Defaults to the message name minus `Input`/`Request`/`Args`/`Params`. |
| `result` | all but Signals | Result message, resolved in the input's package. Leave it empty for a result-less Update or Activity. |
| `task_queue` | all | Default task queue, if statically known. |
| `supports_continue_as_new` | Workflows | Adds a continue-as-new operation and flags the binding. |
| `workflows` | Signals, Queries, Updates | Workflow types that handle this message. |
| `timeouts` | Workflows: `execution`, `run`, `task`. Activities: `schedule_to_close`, `start_to_close`, `schedule_to_start`, `heartbeat` | Go-style durations such as `"30s"` or `"1h30m"`. |
| `retry_policy` | Workflows, Activities | `initial_interval`, `backoff_coefficient`, `maximum_interval`, `maximum_attempts`, `non_retryable_error_types`. |

Generation fails, reporting `file:line:column` for every problem at once, when:

- `kind` is missing;
- `result` can't be resolved, or resolves to another package;
- two messages declare the same `(kind, name)`;
- a field is used on a kind it doesn't apply to;
- a duration is malformed.

## Parameters

Pass these as `opt:` entries in buf, or as comma-separated `key=value` pairs in `--temporal-asyncapi_opt`.
`protoc-gen-temporal-asyncapi --help` lists them too.

| Key | Default | Meaning |
| --- | --- | --- |
| `path` | `asyncapi.yaml` | Output file, relative to `out`. |
| `format` | from `path` | `yaml` or `json`. If unset, `.json` paths produce JSON. |
| `services` | files being generated | Package glob of files to scan, such as `acme.orders.**` or `acme.*.v1`. `*` matches one segment and `**` matches any number. Repeat the key, or separate globs with `\|`. Also matches imported files, so dependencies can be documented. |
| `title` | first package | `info.title` |
| `version` | `0.0.0` | `info.version` |
| `description` | generated overview | `info.description` |
| `id` | `urn:temporal:<first package>` | Document `id`. |
| `server-url` | none | Temporal frontend address (`host:port`, or a URL). Emits a `temporal` server. |
| `namespace` | none | Temporal namespace, recorded as `x-temporal-namespace` on the server. |
| `perspective` | `client` | `client`: operations `send`, as seen by callers. `worker`: operations `receive`, as seen by the Worker hosting them. |
| `trim-unused-schemas` | `false` | Emit only schemas reachable from an operation. By default every message and enum in the scanned files is included. |
| `with-protovalidate` | `true` | Translate `buf.validate` constraints when `buf/validate/validate.proto` is among the inputs. Without it, this setting has no effect. |
| `use-proto-names` | `false` | Use `.proto` field names instead of lowerCamelCase JSON names. Match this to your data converter. |

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
  - Operation bindings also carry `supportsContinueAsNew`, `workflows`, `timeouts` and `retryPolicy`.
- **Tags.**
  - Every operation gets a kind tag: `Workflows`, `Activities`, `Signals`, `Queries` or `Updates`, each linked to the
    Temporal docs.
  - A Workflow and every handler that lists it in `workflows` share a `workflow:<Name>` tag.
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

```sh
go test ./...                                   # unit + golden tests (no protoc/buf needed)
go test ./internal/plugin -update               # accept changed golden files under testdata/
(cd tools/asyncapi-validate && npm ci && node validate.mjs)   # spec-validate every golden document
buf generate                                    # regenerate gen/temporalv1 after editing options.proto
go install . && buf generate example/proto --template example/buf.gen.yaml   # regenerate the example
```

Golden cases live in `testdata/<case>/`. Each case holds:

- `.proto` inputs;
- an optional `params` file;
- either `expected.asyncapi.{yaml,json}` or `expected.error`.

### Releasing

Cut a release in either of two ways:

- push a `v*` tag;
- run the **release** workflow manually from the default branch with a version. The workflow creates the tag.

The workflow then runs these steps in order. If any step fails, the later ones don't run:

1. Tests and spec validation, the same checks as CI.
2. BSR checks: `buf lint` on the `proto` module, `buf breaking` against the previous release tag, and a check that
   `buf.yaml` names the module and that the `BUF_TOKEN` secret is set.
3. GoReleaser publishes the GitHub release and binaries. For a manual run, the tag is created just before this.
4. Only the `proto` module (`temporal/v1/options.proto`) is pushed to `buf.build/austin-zhu/protoc-gen-temporal-asyncapi`,
   labeled with the tag. The example module is never published, and nothing is published from pull requests, forks or
   branch pushes.

Before the first BSR release, the owner must do two things:

1. Create the `buf.build/austin-zhu/protoc-gen-temporal-asyncapi` module on the BSR. The workflow deliberately doesn't
   pass `--create`, so the token never needs permission to create modules.
2. Add a `BUF_TOKEN` Actions secret containing a token for a bot user that has write access to that module only.

The token is passed to `buf push` through the environment and is never logged or written to disk.
