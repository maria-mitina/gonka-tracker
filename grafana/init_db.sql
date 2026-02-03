-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Node metrics table
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
);

-- Create hypertable for time-series optimization
SELECT create_hypertable('node_metrics', 'time', if_not_exists => TRUE);

-- Network metrics table
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
);

-- Create hypertable for network metrics
SELECT create_hypertable('network_metrics', 'time', if_not_exists => TRUE);

-- Participant rewards table (historical rewards per epoch)
CREATE TABLE IF NOT EXISTS participant_rewards (
    time TIMESTAMPTZ NOT NULL,
    node_address TEXT NOT NULL,
    epoch_id INTEGER NOT NULL,
    assigned_reward_gnk BIGINT,
    claimed BOOLEAN,
    
    PRIMARY KEY (node_address, epoch_id)
);

-- Create hypertable for rewards
SELECT create_hypertable('participant_rewards', 'time', if_not_exists => TRUE);

-- Participant rewards metrics (written by backend; used by Grafana rewards dashboard)
CREATE TABLE IF NOT EXISTS participant_rewards_metrics (
    time TIMESTAMPTZ NOT NULL,
    node_address TEXT NOT NULL,
    epoch_id INTEGER NOT NULL,
    assigned_reward_gnk BIGINT,
    claimed BOOLEAN,
    PRIMARY KEY (time, node_address, epoch_id)
);
SELECT create_hypertable('participant_rewards_metrics', 'time', if_not_exists => TRUE);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_node_metrics_address ON node_metrics(node_address);
CREATE INDEX IF NOT EXISTS idx_node_metrics_epoch ON node_metrics(epoch_id);
CREATE INDEX IF NOT EXISTS idx_network_metrics_epoch ON network_metrics(epoch_id);
CREATE INDEX IF NOT EXISTS idx_participant_rewards_address ON participant_rewards(node_address);
CREATE INDEX IF NOT EXISTS idx_participant_rewards_epoch ON participant_rewards(epoch_id);
CREATE INDEX IF NOT EXISTS idx_participant_rewards_metrics_address ON participant_rewards_metrics(node_address);
CREATE INDEX IF NOT EXISTS idx_participant_rewards_metrics_epoch ON participant_rewards_metrics(epoch_id);

-- Verify tables were created
SELECT tablename FROM pg_tables WHERE schemaname = 'public';
