# Development Log - Redpanda + Redpanda Connect Demo

## Initial Request (2026-01-27)

**User Request:**
> Create a demo project using Redpanda and Redpanda Connect with Docker Compose, showing the latest versions working together. Requirements:
> - Setup step for creating test topics
> - Redpanda Connect generates data into topics
> - Additional pipeline reads topics and writes to other topics with data modification
> - README with all steps and cleanup section
> - Use latest versions of both tools

## Implementation Steps

### Step 1: Research Latest Versions

Searched for latest versions:
- Redpanda: v25.3.6 (pinned)
- Redpanda Console: v3.5.0 (pinned)
- Redpanda Connect: 4.75.1 (pinned)

### Step 2: Created Initial Project Structure

Created the following files:
- `docker-compose.yml` - Multi-service setup
- `scripts/create-topics.sh` - Topic initialization
- `pipelines/generator.yaml` - Data generation pipeline
- `pipelines/processor.yaml` - Data enrichment pipeline
- `pipelines/analytics.yaml` - Analytics aggregation pipeline
- `README.md` - Documentation
- `.gitignore` - Git ignore patterns

### Step 3: Initial Docker Compose Configuration

```yaml
# Initial services:
- redpanda (broker)
- console (web UI)
- topic-setup (one-time job)
- connect-generator
- connect-processor
- connect-analytics
```

## Testing and Debugging

### Issue 1: Docker Compose Warnings

**Problem:** Docker Compose showed warning about obsolete `version` attribute.

**Status:** Non-critical warning, can be ignored or fixed by removing version line.

### Issue 2: Generator Pipeline Errors

**Error Log:**
```
lint: "/connect.yaml(10,76) unrecognised function 'format'"
lint: "/connect.yaml(78,1) field seed_brokers not recognised"
```

**Root Cause:** 
1. Using `format` function incorrectly (should be `"string".format()` not `format("string")`)
2. Kafka output uses `addresses` not `seed_brokers` in this version

**Fix Applied:**
```yaml
# Before
let user_id = format("user_%04d", random_int() % 1000)

# After  
let user_id = "user_%04d".format(random_int() % 1000)

# Before
output:
  kafka:
    seed_brokers:
      - redpanda:9092

# After
output:
  kafka:
    addresses:
      - redpanda:9092
```

### Issue 3: Match Expression Syntax

**Error Log:**
```
lint: "/connect.yaml(40,21) expected =, got: $even"
lint: "/connect.yaml(42,44) required: expected query, got: = "or"
```

**Root Cause:** Bloblang match expressions have specific syntax requirements.

**Fix Applied:**
```yaml
# Before (didn't work)
match $event_type {
  "purchase" => { ... }
}

# After (working)
if $event_type == "purchase" {
  root.properties.order_id = ...
}

# Or using ternary with match
root.properties.amount = match $amount_idx {
  0 => 29.99
  1 => 49.99
  ...
}
```

### Issue 4: Processor Pipeline - Round Function

**Error Log:**
```
lint: "/connect.yaml(68,73) wrong number of arguments, expected 0, got 1"
```

**Root Cause:** `round()` method doesn't take arguments in this version.

**Fix Applied:**
```yaml
# Before
root.engagement_score = (($base_score * $tier_mult) + $duration_bonus).number().round()

# After
root.engagement_score = ($base_score * $tier_mult) + $duration_bonus
```

### Issue 5: Analytics Pipeline - Tally Method

**Error Log:**
```
lint: "/connect.yaml(44,92) unrecognised method 'tally'"
```

**Root Cause:** `tally` method doesn't exist in this version of Redpanda Connect (v4.64.0).

**Attempted Fix:**
```yaml
# Tried using fold with custom logic
root.events_by_type = this.fold({}, item -> begin
  let key = item.event_type
  let current = this.get($key, 0)
  this.set($key, $current + 1)
end)
```

**Final Simplification:**
Since complex aggregation was failing, simplified to batch wrapper:
```yaml
- mapping: |
    root.batch_size = this.length()
    root.window_start = now().ts_unix_milli() - 10000
    root.window_end = now().ts_unix_milli()
    root.events = this
    root.pipeline_name = "analytics-aggregator"
```

### Issue 6: If Statement Syntax

**Error Log:**
```
lint: "/connect.yaml(57,19) expected or assignment, got: $dura"
```

**Root Cause:** Variable reassignment with `if` has syntax limitations.

**Fix Applied:**
```yaml
# Before (didn't work)
let duration_bonus = $duration / 1000
if $duration_bonus > 50 {
  $duration_bonus = 50
}

# After (removed the cap for simplicity)
let duration_bonus = $duration / 1000
root.engagement_score = ($base_score * $tier_mult) + $duration_bonus
```

### Issue 7: Analytics Fold Error

**Error Log:**
```
failed assignment (line 7): cannot add types array (from field `this`) and null
```

**Root Cause:** Fold operation couldn't handle null engagement_score values.

**Fix Applied:** Simplified analytics to just batch events without complex aggregation.

## Pinned Versions

| Component | Version | Image |
|-----------|---------|-------|
| Redpanda | v25.3.6 | `redpandadata/redpanda:v25.3.6` |
| Redpanda Console | v3.5.0 | `redpandadata/console:v3.5.0` |
| Redpanda Connect | 4.75.1 | `redpandadata/connect:4.75.1` |

**Note:** GitHub releases showed v4.78.0 as latest for Connect, but Docker images only had 4.75.1 available at the time of pinning. Similarly, Redpanda v25.3.6 was confirmed as latest and available.

## Working Configuration Summary

### Generator Pipeline (Working)
- Generates 10 events/second
- Uses `if` statements for conditional properties
- String formatting with `"%04d".format()` syntax
- Kafka output with `addresses` key

### Processor Pipeline (Working)
- Consumes from `user-events-raw`
- Adds enrichment fields:
  - `processed_at`: timestamp
  - `user_tier`: based on user_id hash (vip/premium/standard/free)
  - `engagement_score`: calculated from event type + tier + duration
  - `properties.region`: mapped from country code
  - `is_high_value`: boolean based on multiple conditions
  - `processing_metadata`: latency and version info
- Uses simple match expressions for country and event type mapping

### Analytics Pipeline (Working - Simplified)
- Consumes from `user-events-enriched`
- Batches 50 events or 10 seconds
- Wraps batch with metadata (batch_size, timestamps, pipeline_name)
- Produces to `user-analytics`

## Verification Steps Performed

1. **Started all services:**
   ```bash
   docker compose up -d
   ```

2. **Verified Redpanda health:**
   ```bash
   docker exec redpanda rpk cluster health
   # Status: healthy
   ```

3. **Verified topics created:**
   ```bash
   docker exec redpanda rpk topic list
   # user-events-raw, user-events-enriched, user-analytics, user-events-dlq
   ```

4. **Verified raw events:**
   ```bash
   docker exec redpanda rpk topic consume user-events-raw --num 3
   # ✓ Events with user_id, event_type, properties
   ```

5. **Verified enriched events:**
   ```bash
   docker exec redpanda rpk topic consume user-events-enriched --num 3
   # ✓ Events with user_tier, engagement_score, region, is_high_value
   ```

6. **Verified analytics:**
   ```bash
   docker exec redpanda rpk topic consume user-analytics --num 1
   # ✓ Batched events with metadata wrapper
   ```

7. **Verified consumer groups:**
   ```bash
   docker exec redpanda rpk group list
   # processor-group: Stable
   # analytics-group: Stable
   ```

## Key Learnings

### Redpanda Connect v4.64.0 Bloblang Syntax

1. **String formatting:** `"format_string".format(value)` not `format("string", value)`
2. **Kafka config:** Use `addresses` not `seed_brokers`
3. **Match expressions:** Have limited support for complex logic, prefer `if` statements
4. **Ternary operator:** Has specific syntax that can be tricky
5. **Method chaining:** Not all methods support chaining (e.g., `round()` has no args)
6. **Fold/tally:** Complex aggregation methods may not be available in all versions

### Working Patterns

```yaml
# String formatting
let user_id = "user_%04d".format($user_id_num)

# Conditional logic
if $event_type == "purchase" {
  root.properties.amount = 99.99
}

# Match with simple cases
root.user_tier = match $user_hash {
  this < 5 => "vip"
  this < 20 => "premium"
  _ => "free"
}

# Kafka output
output:
  kafka:
    addresses:
      - redpanda:9092
    topic: my-topic
    key: ${! meta("kafka_key") }
```

## Final State

All services running and processing data:
- Generator: 10 events/sec into `user-events-raw`
- Processor: Enriching and writing to `user-events-enriched`
- Analytics: Batching and writing to `user-analytics`
- Console: Available at http://localhost:8080

## Data Flow (Verified)

```
Generator (100ms) → user-events-raw → Processor → user-events-enriched → Analytics → user-analytics
   10 events/sec                           enrichment                    batching
   (page_view,                             (user_tier,                   (50 events/
    purchase,                               engagement_score,             10 sec)
    click, etc.)                            region, etc.)
```

## Access Points

- **Redpanda Console:** http://localhost:8080
- **Kafka API:** localhost:19092
- **Schema Registry:** localhost:18081
- **HTTP Proxy:** localhost:18082
- **Admin API:** localhost:19644

## Files Created/Modified

### Created Files
- `docker-compose.yml`
- `scripts/create-topics.sh`
- `pipelines/generator.yaml`
- `pipelines/processor.yaml`
- `pipelines/analytics.yaml`
- `README.md`
- `.gitignore`
- `DEVLOG.md` (this file)

### Total Lines of Code
- Docker Compose: ~110 lines
- Shell script: ~60 lines
- Pipeline configs: ~200 lines
- README: ~400 lines
- Total: ~770 lines
