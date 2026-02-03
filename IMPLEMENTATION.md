# Unified PostgreSQL Backend Implementation

## What Was Implemented

Option 1: Unified PostgreSQL Backend has been implemented with a hybrid approach for backward compatibility.

### Key Changes

1. **New PostgresDB Class** (`backend/src/backend/postgres_db.py`)
   - Unified database class for both caching and metrics
   - Uses asyncpg connection pooling
   - Creates all necessary tables (cache + metrics)
   - Supports TimescaleDB hypertables for time-series optimization

2. **Dual Storage Support**
   - **SQLite (CacheDB)**: Still used for reading cached data (backward compatibility)
   - **PostgreSQL (PostgresDB)**: Used for writing metrics + caching
   - Both databases are written to in parallel

3. **Automatic Metrics Writing**
   - Metrics are written automatically when data is fetched
   - No separate collector script needed
   - Writes happen in the same polling tasks

4. **Graceful Degradation**
   - If PostgreSQL is unavailable, backend continues with SQLite only
   - Logs warnings but doesn't crash
   - Metrics writing is optional

## How It Works

### Data Flow

**Before:**
```
Chain API → Backend → SQLite → API → Collector Script → PostgreSQL
```

**After:**
```
Chain API → Backend → SQLite (read cache) + PostgreSQL (metrics + cache)
                                    ↓
                              Grafana (direct queries)
```

### Metrics Writing

Metrics are automatically written when:
1. Current epoch stats are fetched (`get_current_epoch_stats`)
2. Historical epoch stats are fetched (`get_historical_epoch_stats`)
3. Jail statuses are updated
4. Node health is checked
5. Rewards are fetched

### Tables Created

**Cache Tables:**
- `epoch_cache` - Cached epoch/participant stats
- `epoch_status` - Epoch finish status
- `jail_status` - Validator jail information
- `node_health` - Node health status
- `participant_rewards_cache` - Cached rewards
- `participant_warm_keys` - Warm keys cache
- `participant_hardware_nodes` - Hardware nodes cache
- `epoch_total_rewards` - Total rewards per epoch
- `participant_inferences_cache` - Inferences cache
- `models_cache` - Models cache
- `models_api_cache` - Models API cache
- `confirmation_data` - Confirmation data
- `timeline_cache` - Timeline cache

**Metrics Tables (TimescaleDB hypertables):**
- `node_metrics` - Per-node time-series metrics
- `network_metrics` - Network aggregate metrics
- `participant_rewards_metrics` - Historical rewards metrics

## Configuration

PostgreSQL connection is configured via environment variables (already in `config.env`):

```bash
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=gonka_tracker
```

**Important:** In Docker, `POSTGRES_HOST` must be `postgres` (the service name). Backend now waits for Postgres to be healthy before starting (`depends_on: postgres` in docker-compose).

## No data in Grafana

Grafana dashboards read from PostgreSQL only. Data is written by the **backend** when it polls the Gonka API. If everything shows no data:

1. **Backend must connect to Postgres.** Ensure `config.env` has `POSTGRES_HOST=postgres`. Backend starts only after Postgres is healthy.
2. **Check that tables have data and the time range:**
   ```bash
   ./scripts/check_db.sh
   ```
   If row counts are 0, backend is not writing (check backend logs for "PostgreSQL database initialized" and for API polling).
3. **Grafana time range** must overlap the data. In the script output, use "earliest" and "latest" and set the dashboard time picker to include that range (e.g. "Last 24 hours" or a custom range).

**Participant Rewards Over Time** uses `participant_rewards_metrics`. That table is filled by the backend when it polls participant rewards (`poll_rewards`) and when it calculates epoch total rewards (`poll_epoch_total_rewards`). The backend now writes participant rows even when an epoch has 0 total rewards, so the dashboard can show participants with 0 reward. If the dashboard still shows no data, run `./scripts/check_db.sh` and confirm `participant_rewards_metrics` has rows; if it does, widen the dashboard time range.

## Migration Status

### Phase 1: ✅ Complete
- PostgresDB class created
- Metrics writing integrated
- Dual storage (SQLite + PostgreSQL)
- Backward compatible

### Phase 2: Future (Optional)
- Migrate all cache reads to PostgreSQL
- Remove SQLite dependency
- Single database system

### Phase 3: Future (Optional)
- Remove collector script (`collect_metrics.py`)
- All metrics written directly from backend

## Benefits Achieved

1. ✅ **Metrics written automatically** - No separate collector needed
2. ✅ **Lower latency** - Direct database writes (no API hop)
3. ✅ **Single database for Grafana** - PostgreSQL is the source of truth for metrics
4. ✅ **Backward compatible** - SQLite still works for caching
5. ✅ **Graceful degradation** - Works even if PostgreSQL is down

## Next Steps

1. **Verify metrics are being written:**
   ```bash
   docker compose exec postgres psql -U postgres -d gonka_tracker -c "SELECT COUNT(*) FROM node_metrics;"
   ```

2. **Monitor logs:**
   ```bash
   docker compose logs backend | grep -i postgres
   ```

3. **Optional: Remove collector script** once you verify metrics are being written correctly

4. **Optional: Migrate cache reads** to PostgreSQL for full unification

## Testing

To verify it's working:

1. Check backend logs for PostgreSQL initialization:
   ```bash
   docker compose logs backend | grep -i "PostgreSQL database initialized"
   ```

2. Check if metrics are being written:
   ```bash
   docker compose exec postgres psql -U postgres -d gonka_tracker -c "SELECT time, node_address, block_height FROM node_metrics ORDER BY time DESC LIMIT 5;"
   ```

3. Verify Grafana can query the data (should work as before)

## Rollback

If you need to rollback:
1. The system will continue working with SQLite only
2. Just ensure PostgreSQL connection fails gracefully (already implemented)
3. Collector script can still run if needed
