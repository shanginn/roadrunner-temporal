[![Linux](https://github.com/temporalio/roadrunner-temporal/workflows/rrtemporal/badge.svg)](https://github.com/temporalio/roadrunner-temporal/actions)
[![Discourse](https://img.shields.io/static/v1?label=Discourse&message=Get%20Help&color=informational)](https://community.temporal.io)
<a href="https://discord.gg/TFeEmCs"><img src="https://img.shields.io/badge/discord-chat-magenta.svg"></a>

# Roadrunner Temporal
The repository contains a number of plugins which enable workflow and activity processing for PHP processes. The communication protocol,
supervisor, load-balancer is based on [RoadRunner PHP Application Server](https://roadrunner.dev).

## Installation
Temporal is an official plugin of RoadRunner and available out-of-the-box in >= [RoadRunner 2023.0](https://github.com/roadrunner-server/roadrunner).

Read more about application server installation [here](https://roadrunner.dev/docs/intro-install).

To install PHP-SDK:

```bash
$ composer require temporal/sdk
```

## Nexus handler bridge

The Temporal plugin polls Nexus tasks through the Go SDK and dispatches each
registered service operation to the PHP activity pool. Namespace, task queue,
Nexus endpoint, request and callback headers, links, payloads, operation
tokens, and handler failures are preserved across the bridge.

Handler-method cancellation is cooperative. RoadRunner records cancellation of
in-flight Nexus `Start` and `Cancel` contexts in a process-independent
registry. The PHP SDK polls that state over RoadRunner's local RPC connection
while the handler is running. A cancellation is sticky until the handler
method returns and the entry is then removed. This design is intentional: PHP
pool workers process one request at a time, so a second pool request would
queue behind the active handler with one worker or could land in the wrong
process with multiple workers.

The PHP `Start` and `Cancel` dispatches are detached from their Nexus request
contexts so that the request cancellation signal cannot abort or kill the
worker before PHP polls it. RoadRunner's activity-pool `allocate_timeout` still
bounds worker allocation, and `supervisor.exec_ttl` is the hard execution bound
when configured. Production deployments should configure
`supervisor.exec_ttl` longer than the handler's expected cooperative cleanup
interval; without a supervisor TTL, a handler that never returns can occupy a
PHP worker indefinitely, as with any other unsupervised RoadRunner pool job.

PHP handlers doing long work must inspect their operation context for method
cancellation at appropriate cooperative cancellation points. This is separate
from cancelling an asynchronous Nexus operation by its operation token; token
cancellation continues to invoke the registered operation cancel handler.

See [the acceptance test instructions](tests/acceptance/README.md) for the
reproducible RoadRunner build and end-to-end test commands.

## License
[MIT License](https://github.com/temporalio/roadrunner-temporal/blob/master/LICENSE)
