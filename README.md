# Real-time user event analytics with Redpanda + Redpanda Connect

A clickstream analytics pipeline that takes raw user events, enriches them, and aggregates the results. Everything runs as YAML config on Redpanda Connect.

## Quick start

```bash
git clone <repository-url>
cd redpanda-connect-analytics-demo
docker compose up -d
docker exec redpanda rpk cluster health
docker exec redpanda rpk topic list
docker exec redpanda rpk topic consume user-events-enriched --num 5
```

Redpanda Console: [http://localhost:8080](http://localhost:8080)

## What this is for

This project is meant as an example showing how to use Redpanda and Redpanda Connect, and how various other tools can be configured to work well with Redpanda.
The analytics demo is primarily a way to drive data through the tools, so don't feel the need to focus too much on exactly what the pipelines are doing if it isn't related to your use case. Instead just focus on the fact that there are three YAML files that define the entire pipeline. Redpanda stores and moves the events while Redpanda Connect does the processing.

## What the pipeline does

A product analytics platform might ingest millions of events per day. Before any of that is useful for dashboards or cohort analysis, you need to answer some questions about each event: What tier is this user? How engaged are they? What region are they in? Should this event be flagged? This pipeline answers those in real time across three stages.

The generator produces synthetic clickstream at 10 events/sec, and this data includes page views, purchases, signups, with device type, browser, country, and session IDs. It simulates what an SDK or collector would send in production. Events go into `user-events-raw`.

The processor reads the raw events and adds:
- A user tier (vip/premium/standard/free) derived from user ID
- An engagement score weighted by event type and tier
- A geographic region mapped from the country code
- A high-value flag for large purchases, signups, and VIP cart activity
- Processing metadata with latency tracking

The processor's output goes to the `user-events-enriched` topic.

The aggregator then batches those enriched events into 10-second windows and writes summaries to `user-analytics`.

### Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Data Generator │────>│  user-events-raw │────>│  Data Processor  │
│  (rpcn-gen)     │     │     (topic)      │     │  (rpcn-proc)     │
└─────────────────┘     └──────────────────┘     └────────┬─────────┘
                                                          │
                             ┌────────────────────────────┼────────────────────────────┐
                             │                            │                            │
                             v                            v                            v
                   ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
                   │user-events-enrich│     │  user-analytics  │     │ user-events-dlq  │
                   │     (topic)      │     │     (topic)      │     │    (topic)       │
                   └────────┬─────────┘     └──────────────────┘     └──────────────────┘
                            │
                            v
                   ┌──────────────────┐
                   │ Analytics Aggreg │
                   │  (rpcn-agg)      │
                   └──────────────────┘
```

The whole thing is three YAML files, a shell script for topic creation, and a docker-compose.yml.

---

## Running the demo

### Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- 4GB RAM
- Ports 8080, 19092, 18081, 18082, 19644 free
- `DD_API_KEY` is only needed for the optional [Datadog integration](#datadog-integration) below - the base demo doesn't use it and starts fine without setting it

### Start everything

```bash
docker compose up -d
```

This starts the Redpanda broker, Console UI, creates the topics, and launches all three pipelines.

### Check that it's working

```bash
docker compose ps
docker exec redpanda rpk cluster health
docker exec redpanda rpk topic list
```

### Redpanda Console

Open [http://localhost:8080](http://localhost:8080) to browse topics, messages, and consumer groups.

## Exploring the data

### Raw events

```bash
docker exec redpanda rpk topic consume user-events-raw --num 5

# Follow the stream
docker exec redpanda rpk topic consume user-events-raw
```

### Enriched events

```bash
docker exec redpanda rpk topic consume user-events-enriched --num 5
```

The processor adds these fields:
- `processed_at` - when it was processed
- `user_tier` - vip, premium, standard, or free
- `engagement_score` - calculated from event type, tier, and session duration
- `properties.region` - geographic region
- `is_high_value` - true for large purchases, signups, VIP cart adds
- `processing_metadata` - latency and pipeline version

### Analytics

```bash
docker exec redpanda rpk topic consume user-analytics --num 3
```

Aggregates are produced every 10 seconds.

### Send a test event

```bash
docker exec redpanda bash -c '
echo "{\"event_id\": \"test-001\", \"user_id\": \"user_1234\", \"event_type\": \"purchase\", \"timestamp\": '$(date +%s)'000, \"session_id\": \"session_test\", \"properties\": {\"page_path\": \"/checkout\", \"device_type\": \"desktop\", \"browser\": \"chrome\", \"country\": \"US\", \"duration_ms\": 5000, \"order_id\": \"order_999\", \"amount\": 199.99, \"currency\": \"USD\"}}" | rpk topic produce user-events-raw
'
```

## Monitoring

### Pipeline health

```bash
docker compose exec connect-generator wget -qO- http://localhost:4195/benthos/ready
docker compose exec connect-processor wget -qO- http://localhost:4195/benthos/ready
docker compose exec connect-analytics wget -qO- http://localhost:4195/benthos/ready
```

### Prometheus metrics

```bash
docker compose exec connect-generator wget -qO- http://localhost:4195/metrics
docker compose exec connect-processor wget -qO- http://localhost:4195/metrics
```

### Redpanda's two metrics endpoints

The Redpanda broker's admin API (port 9644) exposes Prometheus-format metrics on **two** separate paths, and it matters which one you point a monitoring tool at:

| Endpoint | Prefix | Cardinality | Use it for |
|---|---|---|---|
| `/public_metrics` | `redpanda_` | Low (curated, aggregated across shards) | Dashboards, alerting, external tools like Datadog |
| `/metrics` | `vectorized_` | High (thousands of series, per-shard) | Local debugging/development, deep troubleshooting |

Try both against the broker in this demo:

```bash
docker exec redpanda curl -s http://localhost:9644/public_metrics | grep -c '^# TYPE'   # ~80 metric families
docker exec redpanda curl -s http://localhost:9644/metrics | grep -c '^# TYPE'           # ~300+ metric families
```

`/metrics` is the older endpoint (`vectorized_` is a holdover from Redpanda's original company name) and predates `/public_metrics`. It's still useful when you need low-level, per-shard detail that `/public_metrics` deliberately strips out for efficiency - which is also why some existing Prometheus setups still point at it out of habit, or because they were built before `/public_metrics` existed. For anything you're shipping to a paid-per-metric tool like Datadog, prefer `/public_metrics`.

Redpanda Connect only has one metrics endpoint (`/metrics` on its HTTP server, port 4195) - the two-endpoint split is specific to the broker.

### Datadog integration

Redpanda's and Redpanda Connect's endpoints are both Prometheus-format already, so the Datadog Agent can scrape them directly - no separate Prometheus/Grafana stack needed in between. Two different checks are involved, because only Redpanda has an official one:

- **`redpanda`** - the official community integration ([`datadog-redpanda`](https://github.com/DataDog/integrations-extras/tree/master/redpanda)), which scrapes the broker's `/public_metrics` and reports under clean names like `redpanda.cluster.brokers`. This is what Datadog's out-of-the-box "Redpanda Overview" dashboard queries. It isn't in the stock Agent image, so `datadog-agent` here is built from `datadog/Dockerfile`, which installs it.
- **`openmetrics`** - the generic check, used for Redpanda Connect (`rpcn.*`), since there's no official Connect integration.

```bash
export DD_API_KEY=<your Datadog API key>
export DD_SITE=datadoghq.com   # or datadoghq.eu, us3.datadoghq.com, etc.
docker compose --profile datadog up -d --build
```

`--build` matters here specifically because of the custom image - without it, Compose won't pick up changes to `datadog/Dockerfile` after the first build.

Verify both checks are running and pulling samples:

```bash
docker exec datadog-agent agent status | grep -A8 "redpanda ("
docker exec datadog-agent agent status | grep -A8 "openmetrics ("
```

In Datadog, search Metrics Explorer for `redpanda.*` and `rpcn.*`, or open the built-in **Redpanda Overview** dashboard (Dashboards → search "Redpanda") to see it populated.

### Metrics and execution mode

This demo runs each pipeline (`connect-generator`, `connect-processor`, `connect-analytics`) as its own Redpanda Connect process - this is "standard" mode, where every process has its own isolated `/metrics` endpoint. That's why the Datadog config above needs three separate `rpcn.*` scrape targets.

Redpanda Connect can also run multiple pipelines inside a single process in [`streams` mode](https://docs.redpanda.com/connect/guides/streams_mode/about/) (e.g. `redpanda-connect streams -r "./pipelines/*.yaml" -o ./observability.yaml`). In that mode:
- All three pipelines share **one** `/metrics` endpoint, so a Datadog config only needs one scrape target instead of three.
- Every metric series is tagged with a `stream` label identifying which pipeline it came from, so you still get per-pipeline breakdowns - just via a label instead of separate ports/containers.

Which mode you run changes your scrape topology (one target vs. many) more than it changes what you can observe - plan your monitoring config accordingly if you move from containers-per-pipeline to a single streams-mode process (e.g. in Kubernetes).

### Logs

```bash
docker compose logs -f connect-generator
docker compose logs -f connect-processor
docker compose logs -f connect-analytics
```

### Consumer groups

```bash
docker exec redpanda rpk group describe processor-group
docker exec redpanda rpk group describe analytics-group
```

## Components

### Services

| Service | Image | Purpose |
|---------|-------|---------|
| Redpanda | `redpandadata/redpanda:v25.3.6` | Kafka-compatible streaming broker |
| Redpanda Console | `redpandadata/console:v3.5.0` | Web UI for topics and messages |
| Topic Setup | `redpandadata/redpanda:v25.3.6` | Creates topics on first run |
| Connect Generator | `redpandadata/connect:4.75.1` | Produces synthetic events |
| Connect Processor | `redpandadata/connect:4.75.1` | Enriches and transforms events |
| Connect Analytics | `redpandadata/connect:4.75.1` | Aggregates into time windows |
| Datadog Agent (optional) | Built from `datadog/Dockerfile` (base: `gcr.io/datadoghq/agent:7`) | Scrapes Redpanda (official check) + Connect (generic check) metrics; only starts with `--profile datadog` |

### Topics

| Topic | Partitions | Description |
|-------|------------|-------------|
| `user-events-raw` | 3 | Raw events from the generator |
| `user-events-enriched` | 3 | Events after processing |
| `user-analytics` | 3 | 10-second aggregate summaries |
| `user-events-dlq` | 1 | Failed messages |

## Throughput, partitioning, and latency

### Throughput scales with partition count, not with pipeline count

Each pipeline here (`connect-processor`, `connect-analytics`) runs as a single Redpanda Connect process using a Kafka `consumer_group`. Within one consumer group, a partition is only ever read by one member at a time - so the ceiling on parallel consumption is the topic's partition count, not how many Connect containers you're willing to run.

The topics in this demo have 3 partitions each (`user-events-raw`, `user-events-enriched`, `user-analytics`), but each pipeline currently runs as a single container/consumer. That means there's already 3x headroom without touching topic config - scale out by running more replicas of the same pipeline (same `consumer_group`, same YAML), e.g.:

```bash
docker compose up -d --scale connect-processor=3
```

Partitions rebalance across however many consumers are in the group, up to the partition count. Beyond that, extra consumers just sit idle - the only way to get more parallelism is to add partitions (which forfeits per-key ordering across the old and new partition layout).

Within a single partition, [`checkpoint_limit`](https://docs.redpanda.com/connect/components/inputs/kafka/#checkpoint_limit) (default `1024`) caps how many in-flight, unacknowledged messages from that partition can be processed concurrently. Setting it to `1` forces strictly sequential processing per partition (useful if downstream ordering must be exact); raising it lets more messages from the same partition be processed and batched concurrently, at the cost of a larger in-flight window if the process crashes.

### Partition key strategy

This demo already uses two different partitioning strategies, deliberately:

- `pipelines/processor.yaml` keys `user-events-enriched` by `user_id` (`meta kafka_key = this.user_id`). Same user always lands on the same partition, so per-user event order is preserved - important if downstream logic depends on seeing one user's events in order.
- `pipelines/analytics.yaml` keys `user-analytics` by a random `uuid_v4()`. The aggregated batches have no per-user ordering requirement, so they're spread round-robin-ish across partitions for even load instead.

The risk with key-based partitioning: if traffic isn't uniform across keys (a few very active users, one noisy device ID, etc.), you get hot partitions - one partition doing most of the work while others sit idle, which caps effective throughput below what the partition count suggests. Watch per-partition throughput (`docker exec redpanda rpk topic describe user-events-enriched -p`) if you suspect skew.

If you migrate off the legacy `kafka` input/output used here to the newer [`redpanda` output](https://docs.redpanda.com/connect/components/outputs/redpanda/#partitioner), it exposes the partitioning strategy directly via `partitioner`: `murmur2_hash` (the default here, key-based), `round_robin` (even distribution, no ordering, more broker CPU), `least_backup` (routes to whichever partition has the smallest backlog - good for throughput when ordering doesn't matter), or `manual` (you pick the partition per message).

### Observing latency

Every input, processor, and output in a Redpanda Connect pipeline emits its own latency metric: `input_latency_ns`, `processor_latency_ns`, `output_latency_ns` (all histograms). With the Datadog integration above, these show up as `rpcn.input_latency_ns`, etc., tagged by `rpcn_pipeline`, so you can see exactly which stage - reading from Kafka, running the bloblang mappings, or writing back out - is contributing the most latency.

This demo also computes its own end-to-end latency directly in the message payload: `pipelines/processor.yaml`'s last mapping stage sets `processing_metadata.processing_latency_ms` as `now() - this.timestamp` (event-generation time to processing time). Consume `user-events-enriched` and look at that field to see real numbers:

```bash
docker exec redpanda rpk topic consume user-events-enriched --num 5 | grep processing_latency_ms
```

Consumer lag (how far behind the processor/analytics groups are from the latest offsets) is a separate, complementary signal - already covered above under [Consumer groups](#consumer-groups).

### Improving latency

- **Batching trades latency for throughput.** `pipelines/analytics.yaml`'s output batches up to `count: 50` or every `period: 10s`, whichever comes first - so an aggregate can sit for up to 10 seconds before it's flushed. Lower `period` (and/or `count`) for fresher aggregates at the cost of smaller, more frequent batches; raise it to amortize output overhead across more records per batch.
- **`checkpoint_limit` interacts with batching.** If you add input-side batching, `checkpoint_limit` must be at least as large as the batch size, or batches never fill and instead flush undersized on every `period` tick.
- **Scale out to shrink queueing delay.** If a partition's backlog is growing, latency is dominated by time spent waiting in the queue, not processing time - see [Throughput scales with partition count](#throughput-scales-with-partition-count-not-with-pipeline-count) above.
- **Simplify hot processor stages.** `processor_latency_ns` is reported per processor, so if one bloblang mapping stage is unusually expensive, it'll show up as its own series rather than being hidden inside an aggregate pipeline latency number.

## Customization

### Generation rate

In `pipelines/generator.yaml`:
```yaml
input:
  generate:
    interval: "50ms"  # "1s" for slower, "10ms" for faster
```

Then `docker compose restart connect-generator`.

### New event types

1. Add the type to the array in `pipelines/generator.yaml`
2. Add scoring logic in `pipelines/processor.yaml`

### Engagement scoring

In `pipelines/processor.yaml`:
```yaml
- mapping: |
    root = this
    let base_score = match this.event_type {
      "purchase" => 100
      "your_new_event" => 75
      # ...
    }
```

### Batch windows

In `pipelines/analytics.yaml`:
```yaml
output:
  kafka:
    batching:
      count: 100      # batch size
      period: "30s"   # time window
```

## Troubleshooting

### Topics missing

```bash
docker compose run --rm topic-setup
```

### Pipeline stuck

```bash
docker compose logs connect-processor
docker compose restart connect-processor
```

### Reset consumer offsets

Kafka refuses to seek a group's offsets while it still has active members (`INVALID_OPERATION: seeking a non-empty group is not allowed`), so stop the pipeline first:

```bash
docker compose stop connect-processor connect-analytics

docker exec redpanda rpk group seek processor-group --to end
docker exec redpanda rpk group seek analytics-group --to end

docker compose start connect-processor connect-analytics
```

### Port conflicts

Change the host port (left side) in `docker-compose.yml`:
```yaml
ports:
  - "29092:19092"
```

## Cleanup

```bash
# Stop and remove containers
docker compose down

# If you also started the Datadog Agent (--profile datadog), include the
# profile flag here too, or datadog-agent is left running and orphaned:
docker compose --profile datadog down

# Also remove data volumes
docker compose down -v
```

## Resources

- [Redpanda documentation](https://docs.redpanda.com/)
- [Redpanda Connect documentation](https://docs.redpanda.com/redpanda-connect/)
- [Redpanda Connect configuration](https://docs.redpanda.com/redpanda-connect/configuration/)
- [Bloblang reference](https://docs.redpanda.com/redpanda-connect/configuration/bloblang/)
- [Docker Hub: Redpanda](https://hub.docker.com/r/redpandadata/redpanda)
- [Docker Hub: Redpanda Connect](https://hub.docker.com/r/redpandadata/connect)
- [Redpanda public metrics reference](https://docs.redpanda.com/current/reference/public-metrics-reference/)
- [Redpanda Connect streams mode](https://docs.redpanda.com/connect/guides/streams_mode/about/)
- [Datadog OpenMetrics check](https://docs.datadoghq.com/integrations/openmetrics/)
