import asyncpg
import json
import os
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)


class PostgresDB:
    def __init__(
        self,
        host: str = None,
        port: int = None,
        user: str = None,
        password: str = None,
        database: str = None
    ):
        self.host = host or os.getenv("POSTGRES_HOST", "localhost")
        self.port = port or int(os.getenv("POSTGRES_PORT", "5432"))
        self.user = user or os.getenv("POSTGRES_USER", "postgres")
        self.password = password or os.getenv("POSTGRES_PASSWORD", "postgres")
        self.database = database or os.getenv("POSTGRES_DB", "gonka_tracker")
        self.pool: Optional[asyncpg.Pool] = None
    
    async def initialize(self):
        """Initialize connection pool and create tables if needed"""
        self.pool = await asyncpg.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            min_size=2,
            max_size=10,
            command_timeout=60
        )
        await self._create_tables()
        logger.info(f"PostgreSQL database initialized: {self.host}:{self.port}/{self.database}")
    
    async def _create_tables(self):
        """Create all necessary tables for caching and metrics"""
        async with self.pool.acquire() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS epoch_cache (
                    epoch_id INTEGER NOT NULL,
                    height INTEGER NOT NULL,
                    participant_index TEXT NOT NULL,
                    stats_json JSONB NOT NULL,
                    seed_signature TEXT,
                    ml_nodes_map JSONB,
                    cached_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (epoch_id, height, participant_index)
                )
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_epoch_cache_epoch_height 
                ON epoch_cache(epoch_id, height)
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS epoch_status (
                    epoch_id INTEGER PRIMARY KEY,
                    is_finished BOOLEAN NOT NULL,
                    finish_height INTEGER,
                    marked_at TIMESTAMPTZ NOT NULL
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS jail_status (
                    epoch_id INTEGER NOT NULL,
                    participant_index TEXT NOT NULL,
                    is_jailed BOOLEAN NOT NULL,
                    jailed_until TEXT,
                    ready_to_unjail BOOLEAN,
                    valcons_address TEXT,
                    moniker TEXT,
                    identity TEXT,
                    keybase_username TEXT,
                    keybase_picture_url TEXT,
                    website TEXT,
                    validator_consensus_key TEXT,
                    consensus_key_mismatch BOOLEAN,
                    recorded_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (epoch_id, participant_index)
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS node_health (
                    participant_index TEXT PRIMARY KEY,
                    is_healthy BOOLEAN NOT NULL,
                    last_check TIMESTAMPTZ NOT NULL,
                    error_message TEXT,
                    response_time_ms INTEGER
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS participant_rewards_cache (
                    epoch_id INTEGER NOT NULL,
                    participant_id TEXT NOT NULL,
                    rewarded_coins TEXT NOT NULL,
                    claimed BOOLEAN NOT NULL,
                    last_updated TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (epoch_id, participant_id)
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS participant_warm_keys (
                    epoch_id INTEGER NOT NULL,
                    participant_id TEXT NOT NULL,
                    grantee_address TEXT NOT NULL,
                    granted_at TEXT NOT NULL,
                    PRIMARY KEY (epoch_id, participant_id, grantee_address)
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS participant_hardware_nodes (
                    epoch_id INTEGER NOT NULL,
                    participant_id TEXT NOT NULL,
                    hardware_json JSONB NOT NULL,
                    PRIMARY KEY (epoch_id, participant_id)
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS epoch_total_rewards (
                    epoch_id INTEGER PRIMARY KEY,
                    total_rewards_gnk BIGINT NOT NULL,
                    calculated_at TIMESTAMPTZ NOT NULL
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS participant_inferences_cache (
                    epoch_id INTEGER NOT NULL,
                    participant_id TEXT NOT NULL,
                    inferences_json JSONB NOT NULL,
                    cached_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (epoch_id, participant_id)
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS models_cache (
                    epoch_id INTEGER NOT NULL,
                    model_id TEXT NOT NULL,
                    total_weight INTEGER,
                    participant_count INTEGER,
                    PRIMARY KEY (epoch_id, model_id)
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS models_api_cache (
                    epoch_id INTEGER NOT NULL,
                    height INTEGER,
                    models_all_json JSONB NOT NULL,
                    models_stats_json JSONB NOT NULL,
                    cached_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (epoch_id, height)
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS confirmation_data (
                    epoch_id INTEGER NOT NULL,
                    participant_index TEXT NOT NULL,
                    weight_to_confirm INTEGER,
                    confirmation_weight INTEGER,
                    confirmation_poc_ratio DECIMAL(10,4),
                    participant_status TEXT,
                    PRIMARY KEY (epoch_id, participant_index)
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS timeline_cache (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    timeline_json JSONB NOT NULL,
                    cached_at TIMESTAMPTZ NOT NULL
                )
            """)
            
            try:
                await self._create_metrics_tables(conn)
            except Exception as e:
                logger.warning(f"Error creating metrics tables: {e}")
                logger.info("Continuing with cache tables only - metrics tables may need manual setup")
    
    async def _create_metrics_tables(self, conn):
        """Create metrics tables (node_metrics, network_metrics, participant_rewards)"""
        try:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS node_metrics (
                    time TIMESTAMPTZ NOT NULL,
                    node_address TEXT NOT NULL,
                    epoch_id INTEGER,
                    block_height INTEGER,
                    inference_count BIGINT,
                    missed_requests BIGINT,
                    validated_inferences BIGINT,
                    invalidated_inferences BIGINT,
                    earned_coins BIGINT,
                    rewarded_coins BIGINT,
                    weight INTEGER,
                    missed_rate DECIMAL(5,4),
                    invalidation_rate DECIMAL(5,4),
                    is_jailed BOOLEAN,
                    node_healthy BOOLEAN,
                    PRIMARY KEY (time, node_address)
                )
            """)
            
            result = await conn.fetchval("""
                SELECT create_hypertable('node_metrics', 'time', if_not_exists => TRUE)
            """)
            if result:
                logger.debug(f"Created hypertable node_metrics: {result}")
        except Exception as e:
            logger.debug(f"node_metrics table/hypertable already exists or error: {e}")
        
        try:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS network_metrics (
                    time TIMESTAMPTZ NOT NULL,
                    epoch_id INTEGER,
                    block_height INTEGER,
                    total_nodes INTEGER,
                    active_nodes INTEGER,
                    total_weight BIGINT,
                    total_inferences BIGINT,
                    total_missed BIGINT,
                    avg_missed_rate DECIMAL(5,4),
                    avg_invalidation_rate DECIMAL(5,4),
                    PRIMARY KEY (time)
                )
            """)
            
            result = await conn.fetchval("""
                SELECT create_hypertable('network_metrics', 'time', if_not_exists => TRUE)
            """)
            if result:
                logger.debug(f"Created hypertable network_metrics: {result}")
        except Exception as e:
            logger.debug(f"network_metrics table/hypertable already exists or error: {e}")
        
        try:
            is_hypertable = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM timescaledb_information.hypertables 
                    WHERE hypertable_name = 'participant_rewards_metrics'
                )
            """)
            
            if not is_hypertable:
                existing_table = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'participant_rewards_metrics'
                    )
                """)
                
                if existing_table:
                    existing_pk_cols = await conn.fetchval("""
                        SELECT COALESCE(string_agg(a.attname, ', ' ORDER BY a.attnum), '')
                        FROM pg_index i
                        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                        WHERE i.indrelid = 'participant_rewards_metrics'::regclass
                        AND i.indisprimary
                    """)
                    
                    row_count = await conn.fetchval("SELECT COUNT(*) FROM participant_rewards_metrics")
                    
                    if (existing_pk_cols and 'time' not in str(existing_pk_cols)) or row_count == 0:
                        if row_count > 0:
                            logger.info(f"Dropping participant_rewards_metrics table ({row_count} rows) to recreate with correct structure")
                        await conn.execute("DROP TABLE IF EXISTS participant_rewards_metrics CASCADE")
                        existing_table = False
                
                if not existing_table:
                    await conn.execute("""
                        CREATE TABLE participant_rewards_metrics (
                            time TIMESTAMPTZ NOT NULL,
                            node_address TEXT NOT NULL,
                            epoch_id INTEGER NOT NULL,
                            assigned_reward_gnk BIGINT,
                            claimed BOOLEAN,
                            PRIMARY KEY (time, node_address, epoch_id)
                        )
                    """)
                    try:
                        result = await conn.fetchval("""
                            SELECT create_hypertable('participant_rewards_metrics', 'time', if_not_exists => TRUE)
                        """)
                        if result:
                            logger.debug(f"Created hypertable participant_rewards_metrics: {result}")
                    except Exception as e:
                        logger.debug(f"Could not create hypertable for participant_rewards_metrics: {e}")
                else:
                    logger.info("participant_rewards_metrics table exists with data - skipping hypertable conversion (will use regular table)")
        except Exception as e:
            logger.warning(f"participant_rewards_metrics setup issue: {e}")
            logger.info("Continuing - participant_rewards_metrics will work as regular table")
        except Exception as e:
            logger.warning(f"participant_rewards_metrics table/hypertable creation issue: {e}")
            logger.info("Continuing without participant_rewards_metrics hypertable - will use regular table")
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_node_metrics_address ON node_metrics(node_address)
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_node_metrics_epoch ON node_metrics(epoch_id)
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_network_metrics_epoch ON network_metrics(epoch_id)
        """)
    
    async def close(self):
        """Close the connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL connection pool closed")
    
    async def save_stats_batch(
        self,
        epoch_id: int,
        height: int,
        participants_stats: List[Dict[str, Any]]
    ):
        """Save participant stats to cache"""
        async with self.pool.acquire() as conn:
            cached_at = datetime.now(timezone.utc)
            
            for stats in participants_stats:
                participant_index = stats.get("index") or stats.get("participant_index")
                if not participant_index:
                    continue
                
                stats_copy = {k: v for k, v in stats.items() if not k.startswith("_")}
                stats_json = json.dumps(stats_copy)
                
                ml_nodes_map = stats.get("_ml_nodes_map", {})
                seed_signature = stats.get("_seed_signature") or stats.get("seed_signature")
                
                await conn.execute("""
                    INSERT INTO epoch_cache (
                        epoch_id, height, participant_index, stats_json,
                        seed_signature, ml_nodes_map, cached_at
                    ) VALUES ($1, $2, $3, $4::jsonb, $5, $6::jsonb, $7)
                    ON CONFLICT (epoch_id, height, participant_index) 
                    DO UPDATE SET
                        stats_json = EXCLUDED.stats_json,
                        seed_signature = EXCLUDED.seed_signature,
                        ml_nodes_map = EXCLUDED.ml_nodes_map,
                        cached_at = EXCLUDED.cached_at
                """, epoch_id, height, participant_index, stats_json,
                    seed_signature, json.dumps(ml_nodes_map), cached_at)
    
    async def get_stats(
        self,
        epoch_id: int,
        height: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get cached participant stats"""
        async with self.pool.acquire() as conn:
            if height is not None:
                rows = await conn.fetch("""
                    SELECT stats_json, seed_signature, ml_nodes_map, cached_at
                    FROM epoch_cache
                    WHERE epoch_id = $1 AND height = $2
                """, epoch_id, height)
            else:
                rows = await conn.fetch("""
                    SELECT stats_json, seed_signature, ml_nodes_map, cached_at
                    FROM epoch_cache
                    WHERE epoch_id = $1
                    ORDER BY height DESC
                    LIMIT 1
                """, epoch_id)
            
            result = []
            for row in rows:
                stats = json.loads(row["stats_json"])
                stats["_seed_signature"] = row["seed_signature"]
                stats["_ml_nodes_map"] = json.loads(row["ml_nodes_map"]) if row["ml_nodes_map"] else {}
                stats["_cached_at"] = row["cached_at"].isoformat()
                stats["_height"] = height or epoch_id
                result.append(stats)
            
            return result
    
    async def write_node_metrics(self, inference_data: dict):
        """Write node metrics to PostgreSQL"""
        now = datetime.now(timezone.utc)
        epoch_id = inference_data.get("epoch_id")
        height = inference_data.get("height")
        
        participants = inference_data.get("participants", [])
        
        async with self.pool.acquire() as conn:
            for participant in participants:
                stats = participant.get("current_epoch_stats", {})
                address = participant.get("address")
                
                if not address:
                    continue
                
                await conn.execute("""
                    INSERT INTO node_metrics (
                        time, node_address, epoch_id, block_height,
                        inference_count, missed_requests, validated_inferences,
                        invalidated_inferences, earned_coins, rewarded_coins,
                        weight, missed_rate, invalidation_rate,
                        is_jailed, node_healthy
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    ON CONFLICT (time, node_address) DO UPDATE SET
                        epoch_id = EXCLUDED.epoch_id,
                        block_height = EXCLUDED.block_height,
                        inference_count = EXCLUDED.inference_count,
                        missed_requests = EXCLUDED.missed_requests,
                        validated_inferences = EXCLUDED.validated_inferences,
                        invalidated_inferences = EXCLUDED.invalidated_inferences,
                        earned_coins = EXCLUDED.earned_coins,
                        rewarded_coins = EXCLUDED.rewarded_coins,
                        weight = EXCLUDED.weight,
                        missed_rate = EXCLUDED.missed_rate,
                        invalidation_rate = EXCLUDED.invalidation_rate,
                        is_jailed = EXCLUDED.is_jailed,
                        node_healthy = EXCLUDED.node_healthy
                """,
                    now,
                    address,
                    epoch_id,
                    height,
                    int(stats.get("inference_count", 0)),
                    int(stats.get("missed_requests", 0)),
                    int(stats.get("validated_inferences", 0)),
                    int(stats.get("invalidated_inferences", 0)),
                    int(stats.get("earned_coins", 0)),
                    int(stats.get("rewarded_coins", 0)),
                    participant.get("weight", 0),
                    Decimal(str(participant.get("missed_rate", 0))),
                    Decimal(str(participant.get("invalidation_rate", 0))),
                    participant.get("is_jailed"),
                    participant.get("node_healthy")
                )
    
    async def write_network_metrics(self, inference_data: dict):
        """Write network aggregate metrics to PostgreSQL"""
        now = datetime.now(timezone.utc)
        epoch_id = inference_data.get("epoch_id")
        height = inference_data.get("height")
        
        participants = inference_data.get("participants", [])
        
        total_nodes = len(participants)
        active_nodes = sum(
            1 for p in participants 
            if not p.get("is_jailed") and p.get("node_healthy")
        )
        total_weight = sum(p.get("weight", 0) for p in participants)
        
        total_inferences = sum(
            int(p.get("current_epoch_stats", {}).get("inference_count", 0))
            for p in participants
        )
        total_missed = sum(
            int(p.get("current_epoch_stats", {}).get("missed_requests", 0))
            for p in participants
        )
        
        total_requests = total_inferences + total_missed
        avg_missed_rate = Decimal(str(total_missed / total_requests)) if total_requests > 0 else Decimal("0")
        
        total_invalidated = sum(
            int(p.get("current_epoch_stats", {}).get("invalidated_inferences", 0))
            for p in participants
        )
        avg_invalidation_rate = Decimal(str(total_invalidated / total_inferences)) if total_inferences > 0 else Decimal("0")
        
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO network_metrics (
                    time, epoch_id, block_height,
                    total_nodes, active_nodes, total_weight,
                    total_inferences, total_missed,
                    avg_missed_rate, avg_invalidation_rate
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (time) DO UPDATE SET
                    epoch_id = EXCLUDED.epoch_id,
                    block_height = EXCLUDED.block_height,
                    total_nodes = EXCLUDED.total_nodes,
                    active_nodes = EXCLUDED.active_nodes,
                    total_weight = EXCLUDED.total_weight,
                    total_inferences = EXCLUDED.total_inferences,
                    total_missed = EXCLUDED.total_missed,
                    avg_missed_rate = EXCLUDED.avg_missed_rate,
                    avg_invalidation_rate = EXCLUDED.avg_invalidation_rate
            """,
                now,
                epoch_id,
                height,
                total_nodes,
                active_nodes,
                total_weight,
                total_inferences,
                total_missed,
                avg_missed_rate,
                avg_invalidation_rate
            )
    
    async def write_participant_rewards_metrics(
        self,
        rewards_data: List[Dict[str, Any]]
    ):
        """Write participant rewards to metrics table"""
        now = datetime.now(timezone.utc)
        
        async with self.pool.acquire() as conn:
            for reward in rewards_data:
                participant_address = reward.get("participant_id")
                epoch_id = reward.get("epoch_id")
                rewarded_coins = reward.get("rewarded_coins", "0")
                claimed = reward.get("claimed", False)
                
                if not participant_address or not epoch_id:
                    continue
                
                try:
                    reward_base_units = int(rewarded_coins) if rewarded_coins != "0" else 0
                    
                    existing = await conn.fetchrow("""
                        SELECT time FROM participant_rewards_metrics
                        WHERE node_address = $1 AND epoch_id = $2
                        ORDER BY time DESC LIMIT 1
                    """, participant_address, epoch_id)
                    
                    if existing:
                        await conn.execute("""
                            UPDATE participant_rewards_metrics
                            SET assigned_reward_gnk = $1, claimed = $2
                            WHERE node_address = $3 AND epoch_id = $4 AND time = $5
                        """, reward_base_units, claimed, participant_address, epoch_id, existing["time"])
                    else:
                        await conn.execute("""
                            INSERT INTO participant_rewards_metrics (
                                time, node_address, epoch_id, assigned_reward_gnk, claimed
                            ) VALUES ($1, $2, $3, $4, $5)
                        """,
                        now,
                        participant_address,
                        epoch_id,
                        reward_base_units,
                        claimed
                    )
                except Exception as e:
                    logger.debug(f"Failed to write reward for {participant_address} epoch {epoch_id}: {e}")
                    continue
    
    async def mark_epoch_finished(self, epoch_id: int, finish_height: int):
        """Mark an epoch as finished"""
        marked_at = datetime.now(timezone.utc)
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO epoch_status (epoch_id, is_finished, finish_height, marked_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (epoch_id) DO UPDATE SET
                    is_finished = EXCLUDED.is_finished,
                    finish_height = EXCLUDED.finish_height,
                    marked_at = EXCLUDED.marked_at
            """, epoch_id, True, finish_height, marked_at)
            logger.info(f"Marked epoch {epoch_id} as finished at height {finish_height}")
    
    async def is_epoch_finished(self, epoch_id: int) -> bool:
        """Check if epoch is marked as finished"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT is_finished FROM epoch_status WHERE epoch_id = $1
            """, epoch_id)
            return row["is_finished"] if row else False
    
    async def save_jail_status_batch(self, epoch_id: int, jail_statuses: List[Dict[str, Any]]):
        """Save jail statuses to cache"""
        recorded_at = datetime.now(timezone.utc)
        async with self.pool.acquire() as conn:
            for status in jail_statuses:
                await conn.execute("""
                    INSERT INTO jail_status (
                        epoch_id, participant_index, is_jailed, jailed_until, ready_to_unjail,
                        valcons_address, moniker, identity, keybase_username, keybase_picture_url,
                        website, validator_consensus_key, consensus_key_mismatch, recorded_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    ON CONFLICT (epoch_id, participant_index) DO UPDATE SET
                        is_jailed = EXCLUDED.is_jailed,
                        jailed_until = EXCLUDED.jailed_until,
                        ready_to_unjail = EXCLUDED.ready_to_unjail,
                        valcons_address = EXCLUDED.valcons_address,
                        moniker = EXCLUDED.moniker,
                        identity = EXCLUDED.identity,
                        keybase_username = EXCLUDED.keybase_username,
                        keybase_picture_url = EXCLUDED.keybase_picture_url,
                        website = EXCLUDED.website,
                        validator_consensus_key = EXCLUDED.validator_consensus_key,
                        consensus_key_mismatch = EXCLUDED.consensus_key_mismatch,
                        recorded_at = EXCLUDED.recorded_at
                """,
                    epoch_id,
                    status.get("participant_index"),
                    status.get("is_jailed", False),
                    status.get("jailed_until"),
                    status.get("ready_to_unjail", False),
                    status.get("valcons_address"),
                    status.get("moniker"),
                    status.get("identity"),
                    status.get("keybase_username"),
                    status.get("keybase_picture_url"),
                    status.get("website"),
                    status.get("validator_consensus_key"),
                    status.get("consensus_key_mismatch"),
                    recorded_at
                )
            logger.info(f"Saved {len(jail_statuses)} jail statuses for epoch {epoch_id}")
    
    async def get_jail_status(self, epoch_id: int, participant_index: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        """Get jail statuses"""
        async with self.pool.acquire() as conn:
            if participant_index:
                rows = await conn.fetch("""
                    SELECT * FROM jail_status
                    WHERE epoch_id = $1 AND participant_index = $2
                """, epoch_id, participant_index)
            else:
                rows = await conn.fetch("""
                    SELECT * FROM jail_status WHERE epoch_id = $1
                """, epoch_id)
            
            if not rows:
                return None
            
            return [dict(row) for row in rows]
    
    async def save_node_health_batch(self, health_statuses: List[Dict[str, Any]]):
        """Save node health statuses"""
        last_check = datetime.now(timezone.utc)
        async with self.pool.acquire() as conn:
            for status in health_statuses:
                await conn.execute("""
                    INSERT INTO node_health (
                        participant_index, is_healthy, last_check, error_message, response_time_ms
                    ) VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (participant_index) DO UPDATE SET
                        is_healthy = EXCLUDED.is_healthy,
                        last_check = EXCLUDED.last_check,
                        error_message = EXCLUDED.error_message,
                        response_time_ms = EXCLUDED.response_time_ms
                """,
                    status.get("participant_index"),
                    status.get("is_healthy", False),
                    last_check,
                    status.get("error_message"),
                    status.get("response_time_ms")
                )
            logger.info(f"Saved {len(health_statuses)} node health statuses")
    
    async def get_node_health(self, participant_index: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        """Get node health statuses"""
        async with self.pool.acquire() as conn:
            if participant_index:
                rows = await conn.fetch("""
                    SELECT * FROM node_health WHERE participant_index = $1
                """, participant_index)
            else:
                rows = await conn.fetch("SELECT * FROM node_health")
            
            if not rows:
                return None
            
            return [dict(row) for row in rows]
    
    async def save_reward_batch(self, rewards: List[Dict[str, Any]]):
        """Save participant rewards to cache"""
        last_updated = datetime.now(timezone.utc)
        async with self.pool.acquire() as conn:
            for reward in rewards:
                await conn.execute("""
                    INSERT INTO participant_rewards_cache (
                        epoch_id, participant_id, rewarded_coins, claimed, last_updated
                    ) VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (epoch_id, participant_id) DO UPDATE SET
                        rewarded_coins = EXCLUDED.rewarded_coins,
                        claimed = EXCLUDED.claimed,
                        last_updated = EXCLUDED.last_updated
                """,
                    reward.get("epoch_id"),
                    reward.get("participant_id"),
                    reward.get("rewarded_coins", "0"),
                    reward.get("claimed", False),
                    last_updated
                )
    
    async def get_reward(self, epoch_id: int, participant_id: str) -> Optional[Dict[str, Any]]:
        """Get cached reward for participant"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM participant_rewards_cache
                WHERE epoch_id = $1 AND participant_id = $2
            """, epoch_id, participant_id)
            return dict(row) if row else None
    
    async def get_rewards_for_participant(self, participant_id: str, epoch_ids: List[int]) -> List[Dict[str, Any]]:
        """Get rewards for participant across multiple epochs"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM participant_rewards_cache
                WHERE participant_id = $1 AND epoch_id = ANY($2::int[])
            """, participant_id, epoch_ids)
            return [dict(row) for row in rows]
    
    async def save_warm_keys_batch(self, epoch_id: int, participant_id: str, warm_keys: List[Dict[str, Any]]):
        """Save warm keys for participant"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                DELETE FROM participant_warm_keys
                WHERE epoch_id = $1 AND participant_id = $2
            """, epoch_id, participant_id)
            
            for key in warm_keys:
                await conn.execute("""
                    INSERT INTO participant_warm_keys (
                        epoch_id, participant_id, grantee_address, granted_at
                    ) VALUES ($1, $2, $3, $4)
                """,
                    epoch_id,
                    participant_id,
                    key.get("grantee_address"),
                    key.get("granted_at")
                )
    
    async def get_warm_keys(self, epoch_id: int, participant_id: str) -> Optional[List[Dict[str, Any]]]:
        """Get warm keys for participant"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT grantee_address, granted_at
                FROM participant_warm_keys
                WHERE epoch_id = $1 AND participant_id = $2
            """, epoch_id, participant_id)
            return [dict(row) for row in rows] if rows else None
    
    async def save_hardware_nodes_batch(self, epoch_id: int, participant_id: str, hardware_nodes: List[Dict[str, Any]]):
        """Save hardware nodes for participant"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                DELETE FROM participant_hardware_nodes
                WHERE epoch_id = $1 AND participant_id = $2
            """, epoch_id, participant_id)
            
            if hardware_nodes:
                await conn.execute("""
                    INSERT INTO participant_hardware_nodes (
                        epoch_id, participant_id, hardware_json
                    ) VALUES ($1, $2, $3::jsonb)
                """,
                    epoch_id,
                    participant_id,
                    json.dumps(hardware_nodes)
                )
    
    async def get_hardware_nodes(self, epoch_id: int, participant_id: str) -> Optional[List[Dict[str, Any]]]:
        """Get hardware nodes for participant"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT hardware_json FROM participant_hardware_nodes
                WHERE epoch_id = $1 AND participant_id = $2
            """, epoch_id, participant_id)
            return json.loads(row["hardware_json"]) if row and row["hardware_json"] else None
    
    async def save_epoch_total_rewards(self, epoch_id: int, total_rewards_gnk: int):
        """Save total rewards for epoch"""
        calculated_at = datetime.now(timezone.utc)
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO epoch_total_rewards (epoch_id, total_rewards_gnk, calculated_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (epoch_id) DO UPDATE SET
                    total_rewards_gnk = EXCLUDED.total_rewards_gnk,
                    calculated_at = EXCLUDED.calculated_at
            """, epoch_id, total_rewards_gnk, calculated_at)
    
    async def get_epoch_total_rewards(self, epoch_id: int) -> Optional[int]:
        """Get total rewards for epoch"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT total_rewards_gnk FROM epoch_total_rewards WHERE epoch_id = $1
            """, epoch_id)
            return row["total_rewards_gnk"] if row else None
    
    async def delete_epoch_total_rewards(self, epoch_id: int):
        """Delete total rewards for epoch"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                DELETE FROM epoch_total_rewards WHERE epoch_id = $1
            """, epoch_id)

    async def count_rewards_since_hours(self, hours: float) -> int:
        """Return count of participant_rewards_metrics rows in the last N hours"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT COUNT(*) AS n FROM participant_rewards_metrics
                WHERE time > NOW() - make_interval(hours => $1)
            """, hours)
            return row["n"] if row else 0

    async def save_models_batch(self, epoch_id: int, models: List[Dict[str, Any]]):
        """Save models cache"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                DELETE FROM models_cache WHERE epoch_id = $1
            """, epoch_id)
            
            for model in models:
                await conn.execute("""
                    INSERT INTO models_cache (
                        epoch_id, model_id, total_weight, participant_count
                    ) VALUES ($1, $2, $3, $4)
                """,
                    epoch_id,
                    model.get("model_id"),
                    model.get("total_weight", 0),
                    model.get("participant_count", 0)
                )
    
    async def get_models(self, epoch_id: int) -> Optional[List[Dict[str, Any]]]:
        """Get cached models"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT model_id, total_weight, participant_count
                FROM models_cache WHERE epoch_id = $1
            """, epoch_id)
            return [dict(row) for row in rows] if rows else None
    
    async def save_models_api_cache(
        self,
        epoch_id: int,
        height: int,
        models_all_data: Dict[str, Any],
        models_stats_data: Dict[str, Any]
    ):
        """Save models API cache"""
        cached_at = datetime.now(timezone.utc)
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO models_api_cache (
                    epoch_id, height, models_all_json, models_stats_json, cached_at
                ) VALUES ($1, $2, $3::jsonb, $4::jsonb, $5)
                ON CONFLICT (epoch_id, height) DO UPDATE SET
                    models_all_json = EXCLUDED.models_all_json,
                    models_stats_json = EXCLUDED.models_stats_json,
                    cached_at = EXCLUDED.cached_at
            """,
                epoch_id,
                height,
                json.dumps(models_all_data),
                json.dumps(models_stats_data),
                cached_at
            )
    
    async def get_models_api_cache(
        self,
        epoch_id: int,
        height: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Get cached models API data"""
        async with self.pool.acquire() as conn:
            if height is not None:
                row = await conn.fetchrow("""
                    SELECT models_all_json, models_stats_json, cached_at, height
                    FROM models_api_cache
                    WHERE epoch_id = $1 AND height = $2
                """, epoch_id, height)
            else:
                row = await conn.fetchrow("""
                    SELECT models_all_json, models_stats_json, cached_at, height
                    FROM models_api_cache
                    WHERE epoch_id = $1
                    ORDER BY height DESC
                    LIMIT 1
                """, epoch_id)
            
            if not row:
                return None
            
            return {
                "models_all": json.loads(row["models_all_json"]),
                "models_stats": json.loads(row["models_stats_json"]),
                "cached_at": row["cached_at"].isoformat(),
                "cached_height": row["height"]
            }
    
    async def save_participant_inferences_batch(
        self,
        epoch_id: int,
        participant_id: str,
        inferences: List[Dict[str, Any]]
    ):
        """Save participant inferences cache"""
        cached_at = datetime.now(timezone.utc)
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO participant_inferences_cache (
                    epoch_id, participant_id, inferences_json, cached_at
                ) VALUES ($1, $2, $3::jsonb, $4)
                ON CONFLICT (epoch_id, participant_id) DO UPDATE SET
                    inferences_json = EXCLUDED.inferences_json,
                    cached_at = EXCLUDED.cached_at
            """,
                epoch_id,
                participant_id,
                json.dumps(inferences),
                cached_at
            )
    
    async def get_participant_inferences(
        self,
        epoch_id: int,
        participant_id: str
    ) -> Optional[List[Dict[str, Any]]]:
        """Get cached participant inferences"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT inferences_json FROM participant_inferences_cache
                WHERE epoch_id = $1 AND participant_id = $2
            """, epoch_id, participant_id)
            return json.loads(row["inferences_json"]) if row and row["inferences_json"] else None
    
    async def save_confirmation_data_batch(self, epoch_id: int, data_list: List[Dict[str, Any]]):
        """Save confirmation data"""
        async with self.pool.acquire() as conn:
            for data in data_list:
                await conn.execute("""
                    INSERT INTO confirmation_data (
                        epoch_id, participant_index, weight_to_confirm,
                        confirmation_weight, confirmation_poc_ratio, participant_status
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (epoch_id, participant_index) DO UPDATE SET
                        weight_to_confirm = EXCLUDED.weight_to_confirm,
                        confirmation_weight = EXCLUDED.confirmation_weight,
                        confirmation_poc_ratio = EXCLUDED.confirmation_poc_ratio,
                        participant_status = EXCLUDED.participant_status
                """,
                    epoch_id,
                    data.get("participant_index"),
                    data.get("weight_to_confirm"),
                    data.get("confirmation_weight"),
                    data.get("confirmation_poc_ratio"),
                    data.get("participant_status")
                )
    
    async def get_confirmation_data(self, epoch_id: int) -> Optional[List[Dict[str, Any]]]:
        """Get confirmation data"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM confirmation_data WHERE epoch_id = $1
            """, epoch_id)
            return [dict(row) for row in rows] if rows else None
    
    async def save_timeline_cache(self, timeline_data: Dict[str, Any]):
        """Save timeline cache"""
        cached_at = datetime.now(timezone.utc)
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO timeline_cache (id, timeline_json, cached_at)
                VALUES (1, $1::jsonb, $2)
                ON CONFLICT (id) DO UPDATE SET
                    timeline_json = EXCLUDED.timeline_json,
                    cached_at = EXCLUDED.cached_at
            """, json.dumps(timeline_data), cached_at)
    
    async def get_timeline_cache(self) -> Optional[Dict[str, Any]]:
        """Get cached timeline"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT timeline_json FROM timeline_cache WHERE id = 1
            """)
            return json.loads(row["timeline_json"]) if row and row["timeline_json"] else None
    
