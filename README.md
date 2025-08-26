# Real-Time Token Statistics Service

A high-throughput, low-latency service for processing 1000+ swaps/sec and calculating real-time token statistics with minimal latency.

## Problem

- 1000 swaps/sec from producer (who, token, amount, usd, side, etc.)
- Calculate real-time statistics: 5min/1H/24h volume, transaction counts
- Serve via HTTP API and WebSocket with minimal latency
- Handle duplicates and out-of-order events
- Zero data loss during restarts
- Horizontally scalable

## Architecture Overview

### Transport Mechanisms

#### From Producer
- **Kafka** - Primary choice for high-throughput event streaming
  - Partitioned by token for parallel processing
  - Guaranteed ordering within partition
  - Built-in replication and durability
  - Consumer groups for horizontal scaling

#### Alternative Options
- **Apache Pulsar** - Similar to Kafka with better multi-tenancy
- **Redis Streams** - For lower latency requirements
- **RabbitMQ** - For guaranteed delivery patterns

### Data Storage Strategy

#### Hot Data (Real-time aggregates)
- **Redis Cluster** 
  - Sub-millisecond latency
  - Automatic failover
  - Memory-optimized for sliding windows
  - TTL for automatic cleanup

#### Warm Data (Recent historical)
- **ClickHouse**
  - Columnar storage for analytics
  - Time-series optimized
  - High compression ratios
  - Real-time inserts

#### Cold Data (Long-term storage)
- **S3/Object Storage**
  - Cost-effective archival
  - Parquet format for analytics
  - Lifecycle policies

#### State Management
- **Persistent snapshots** to object storage
- **Write-ahead logs** for durability
- **Checkpoint-based recovery**

### High Availability & Zero Data Loss

#### Service Level
- **Multiple instances** behind load balancer
- **Health checks** and automatic failover
- **Circuit breakers** for downstream dependencies
- **Graceful shutdown** with state persistence

#### Data Level
- **At-least-once processing** with idempotency
- **Offset tracking** for exactly-once semantics
- **Distributed snapshots** across availability zones
- **Cross-region replication** for disaster recovery

#### Infrastructure
- **Kubernetes deployment** with auto-scaling
- **Service mesh** for traffic management
- **Monitoring** with Prometheus/Grafana
- **Alerting** for SLA violations

### Scalability Patterns

#### Horizontal Scaling
- **Stateless services** for easy scaling
- **Consistent hashing** for data distribution
- **Auto-scaling** based on queue depth/CPU
- **Load balancing** with sticky sessions for WebSocket

#### Data Partitioning
- **Partition by token** for parallel processing
- **Time-based sharding** for historical data
- **Read replicas** for query scaling

### Event Processing Pipeline

#### Ingestion Layer
```
Producer → Kafka → Service Instances
```

#### Processing Layer
```
Deduplication → Reordering → Aggregation → Storage
```

#### Serving Layer
```
HTTP API ← Redis ← Aggregator
WebSocket ← Event Stream ← Aggregator
```

## Implementation Details

### Core Components

#### Event Processor
- Kafka consumer with configurable parallelism
- Sliding window aggregation with ring buffers
- Deduplication using TTL-based cache
- Out-of-order handling with watermarks

#### Storage Layer
- Interface-based design for pluggable backends
- Write-through caching for hot data
- Batch writes for efficiency
- Automatic retention policies

#### API Layer
- REST endpoints for current statistics
- WebSocket for real-time updates
- Rate limiting and authentication
- Monitoring and metrics

### Data Flow

```
1. Kafka Consumer → Event Validation
2. Deduplication Check → Skip if duplicate
3. Reorder Buffer → Handle out-of-order
4. Sliding Window → Update aggregates
5. Storage Update → Persist state
6. WebSocket Notify → Push to clients
```


### Configuration Options

#### Backpressure Strategies
- `block` - Block until capacity available (default)
- `drop` - Drop events when full (demo only)
- `buffer` - Internal buffering with overflow handling

#### Time Windows
- Configurable via `WINDOWS=5m,1h,24h`
- Bucket granularity via `BUCKET_STEP=5s`
- Automatic expiration and cleanup
