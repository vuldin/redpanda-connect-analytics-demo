# Real-time user event analytics with Redpanda + Redpanda Connect

A clickstream analytics pipeline that takes raw user events, enriches them, and aggregates the results. Everything runs as YAML config on Redpanda Connect. No application code.

## Quick start

```bash
git clone <repository-url>
cd rpcn-demo
docker compose up -d
docker exec redpanda rpk cluster health
docker exec redpanda rpk topic list
docker exec redpanda rpk topic consume user-events-enriched --num 5
```

Redpanda Console: [http://localhost:8080](http://localhost:8080)

## What this is for

If you're building product analytics, you've dealt with this: raw clickstream data (page views, clicks, purchases) arrives fast and messy. Nobody can use it until it's been classified, scored, and aggregated. Batch ETL adds hours of delay. Writing custom stream processors means dealing with serialization, retries, scaling, and all the plumbing that has nothing to do with your actual business logic.

This demo takes a different approach. Three YAML files define the entire pipeline. Redpanda stores and moves the events. Redpanda Connect does the processing. No JVM, no custom code.

## What the pipeline does

A product analytics platform might ingest millions of events per day. Before any of that is useful for dashboards or cohort analysis, you need to answer some questions about each event: What tier is this user? How engaged are they? What region are they in? Should this event be flagged?

This pipeline answers those in real time across three stages.

The generator produces synthetic clickstream at 10 events/sec: page views, purchases, signups, with device type, browser, country, and session IDs. It simulates what an SDK or collector would send in production. Events go into `user-events-raw`.

The processor reads raw events and adds:
- A user tier (vip/premium/standard/free) derived from user ID
- An engagement score weighted by event type and tier
- A geographic region mapped from the country code
- A high-value flag for large purchases, signups, and VIP cart activity
- Processing metadata with latency tracking

Output goes to `user-events-enriched`.

The aggregator batches enriched events in 10-second windows and writes summaries to `user-analytics`.

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
docker exec -it redpanda bash -c '
echo "{\"event_id\": \"test-001\", \"user_id\": \"user_1234\", \"event_type\": \"purchase\", \"timestamp\": '$(date +%s)'000, \"session_id\": \"session_test\", \"properties\": {\"page_path\": \"/checkout\", \"device_type\": \"desktop\", \"browser\": \"chrome\", \"country\": \"US\", \"duration_ms\": 5000, \"order_id\": \"order_999\", \"amount\": 199.99, \"currency\": \"USD\"}}" | rpk topic produce user-events-raw
'
```

## Monitoring

### Pipeline health

```bash
docker exec connect-generator wget -qO- http://localhost:4195/benthos/ready
docker exec connect-processor wget -qO- http://localhost:4195/benthos/ready
docker exec connect-analytics wget -qO- http://localhost:4195/benthos/ready
```

### Prometheus metrics

```bash
docker exec connect-generator wget -qO- http://localhost:4195/metrics
docker exec connect-processor wget -qO- http://localhost:4195/metrics
```

### Logs

```bash
docker logs -f connect-generator
docker logs -f connect-processor
docker logs -f connect-analytics
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

### Topics

| Topic | Partitions | Description |
|-------|------------|-------------|
| `user-events-raw` | 3 | Raw events from the generator |
| `user-events-enriched` | 3 | Events after processing |
| `user-analytics` | 3 | 10-second aggregate summaries |
| `user-events-dlq` | 1 | Failed messages |

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
docker logs connect-processor
docker compose restart connect-processor
```

### Reset consumer offsets

```bash
docker exec redpanda rpk group seek processor-group --to end
docker exec redpanda rpk group seek analytics-group --to end
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
