# Telephony Event Sync

High-performance telephony event synchronization service between Redis instances, optimized for low latency and high throughput.

## Architecture

### System Overview

```mermaid
graph TD
    A[FreeSWITCH] -->|Publishes Events| B[Local Redis]
    B -->|XReadGroup| C[Telephony Event Sync]
    C -->|XAdd| D[Remote Redis]
    C -->|XAck| B
    E[Health Check] -->|Monitors| C
    F[Cleanup Service] -.->|Removes Idle Consumers on Shutdown| B
```

### Processing Flow

```mermaid
sequenceDiagram
    participant FS as FreeSWITCH
    participant LR as Local Redis
    participant TES as Telephony Event Sync
    participant RR as Remote Redis

    FS->>LR: XADD Event
    LR->>TES: XReadGroup
    TES->>TES: Process Batch
    TES->>RR: XADD Pipeline
    TES->>LR: XAck
    Note over TES: Monitor Latency
```

### Component Architecture

```mermaid
graph TD
    A[Reader] -->|Channel| B[Writer Pool]
    B -->|Pipeline| C[Remote Redis]
    D[Health Monitor] -->|Checks| A
    D -->|Checks| B
    E[Cleanup Service] -.->|Removes on Shutdown| F[Idle Consumers]
    G[HTTP Server] -->|Exposes| H[Health Endpoint]
```

## Features

- Real-time FreeSWITCH event synchronization between Redis instances
- Low-latency event processing with batch optimization
- Parallel processing with multiple writer workers
- Comprehensive latency monitoring
- Automatic consumer cleanup on service shutdown
- Health checks and metrics
- Graceful shutdown
- Protection against overload

## Requirements

- Go 1.21 or higher
- Redis 6.0 or higher
- FreeSWITCH with mod_event_socket

## Configuration

The service is configured through environment variables:

### Redis Local Configuration

```env
REDIS_LOCAL_ADDR=localhost:6379    # Local Redis address
REDIS_LOCAL_DB=2                   # Local Redis database number
REDIS_LOCAL_PASSWORD=              # Local Redis password (if any)
REDIS_LOCAL_POOL_SIZE=100          # Local Redis connection pool size
REDIS_LOCAL_MIN_IDLE_CONNS=10      # Minimum idle connections in pool
REDIS_LOCAL_MAX_RETRIES=3          # Maximum retries for failed operations
```

### Redis Remote Configuration

```env
REDIS_REMOTE_ADDR=redis.qa-uc-cloud1.gocontact.internal:6379  # Remote Redis address
REDIS_REMOTE_DB=2                 # Remote Redis database number
REDIS_REMOTE_PASSWORD=            # Remote Redis password (if any)
REDIS_REMOTE_POOL_SIZE=100        # Remote Redis connection pool size
REDIS_REMOTE_MIN_IDLE_CONNS=10    # Minimum idle connections in pool
REDIS_REMOTE_MAX_RETRIES=3        # Maximum retries for failed operations
```

### Redis Consumer Group Configuration

```env
REDIS_GROUP=sync_group            # Redis consumer group name
REDIS_CONSUMER=sync_worker        # Base consumer name (will be appended with hostname and ID)
```

### Stream Configuration

```env
STREAM_EVENTS=freeswitch:telephony:events        # Main events stream
STREAM_JOBS=freeswitch:telephony:background-jobs # Background jobs stream
```

### Processing Configuration

```env
# Reader configuration
READER_WORKERS=10                # Number of parallel reader workers
READER_BATCH_SIZE=1000           # Number of messages to read at once
READER_MAX_LATENCY=300ms         # Maximum acceptable read latency
READER_BLOCK_TIME=10ms           # Block time for XReadGroup operation

# Buffer configuration
BUFFER_SIZE=100000               # Size of the channel buffer between reader and writers

# Writer configuration
WRITER_WORKERS=10                # Number of parallel writer workers
WRITER_PIPELINE_TIMEOUT=25ms     # Pipeline execution timeout
WRITER_BATCH_SIZE=10             # Number of messages per pipeline
WRITER_MAX_LATENCY=300ms         # Maximum acceptable write latency
TOTAL_MAX_LATENCY=1000ms         # Maximum acceptable total latency (from event to remote)
```

### Health Check Configuration

```env
HEALTH_CHECK_INTERVAL=5s         # Interval between health checks
HEALTH_RECOVERY_TIMEOUT=30s      # Timeout for recovery attempts
HEALTH_MAX_RETRIES=5             # Maximum consecutive failures before unhealthy
HEALTH_PORT=8080                 # Port for health check endpoint
```

## Monitoring

The service provides comprehensive monitoring capabilities:

### Metrics

- Messages processed per interval
- Queue size
- Read latency
- Write latency
- Total event latency (from FreeSWITCH to remote Redis)
- Error counts
- Consumer status

### Health Check

HTTP endpoint for health verification:

```txt
GET /health
```

Response:

```json
{
    "status": "healthy",
    "last_sync": "2024-03-21T10:00:00Z",
    "queue_size": 0,
    "errors": 0
}
```

### Logs

- Connection errors
- Processing errors
- High latency warnings
- Cleanup status
- Metrics every 5 seconds
- Consumer lifecycle events

## Performance Optimizations

### Latency Optimization

```mermaid
graph LR
    A[Event] -->|1. FreeSWITCH| B[Local Redis]
    B -->|2. Reader| C[Channel]
    C -->|3. Writer Pool| D[Remote Redis]
    E[Latency Monitor] -->|Tracks| A
    E -->|Tracks| D
```

The service implements several optimizations:

1. **Batch Processing**:
   - Reads multiple messages at once
   - Uses Redis pipeline for writes
   - Configurable batch sizes

2. **Parallel Processing**:
   - Multiple writer workers
   - Non-blocking channel operations
   - Efficient resource utilization
   - Optimized metrics updates

3. **Memory Management**:
   - Pre-allocated buffers
   - Efficient string to bytes conversion
   - Reduced memory allocations
   - Optimized map operations

4. **Overload Protection**:
   - Buffered channels
   - Non-blocking operations
   - Configurable timeouts

## Build and Execution

### Local Build

```bash
# Build
go build -o telephonyEventSync

# Execution
./telephonyEventSync
```

### Docker

```bash
# Build image
docker compose build

# Run container
docker compose up -d
```

## Troubleshooting

### Common Issues

1. **High Latency**:
   - Check network connectivity
   - Verify Redis performance
   - Adjust batch sizes
   - Monitor system resources

2. **Connection Errors**:
   - Verify Redis credentials
   - Check network connectivity
   - Adjust connection pool settings
   - Monitor Redis logs

3. **Lost Messages**:
   - Check buffer size
   - Monitor queue size
   - Adjust worker count
   - Verify consumer group status

### Monitoring Tools

1. **Redis CLI**:

   ```bash
   # Check consumer group
   XINFO GROUPS <stream>

   # Check pending messages
   XPENDING <stream> <group>
   ```

2. **Metrics**:

```bash
# Check metrics endpoint
curl http://localhost:8080/metrics
```

## Contributing

1. Fork the project
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request
