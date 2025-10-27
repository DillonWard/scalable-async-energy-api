CREATE TABLE IF NOT EXISTS device_readings (
    id SERIAL PRIMARY KEY,
    home_id VARCHAR(50) NOT NULL,
    appliance_type VARCHAR(100) NOT NULL,
    energy_consumption DECIMAL(10, 4) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    date DATE NOT NULL,
    outdoor_temperature DECIMAL(6, 2),
    season VARCHAR(20),
    household_size INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS energy_aggregates (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    date_hour TIMESTAMP NOT NULL,
    avg_energy_usage DECIMAL(10, 4),
    max_energy_usage DECIMAL(10, 4),
    min_energy_usage DECIMAL(10, 4),
    avg_temperature DECIMAL(6, 2),
    avg_humidity DECIMAL(6, 2),
    avg_light_level DECIMAL(6, 2),
    reading_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(device_id, date_hour)
);

CREATE INDEX IF NOT EXISTS idx_device_readings_home_id ON device_readings(home_id);
CREATE INDEX IF NOT EXISTS idx_device_readings_appliance_type ON device_readings(appliance_type);
CREATE INDEX IF NOT EXISTS idx_device_readings_timestamp ON device_readings(timestamp);
CREATE INDEX IF NOT EXISTS idx_device_readings_date ON device_readings(date);
CREATE INDEX IF NOT EXISTS idx_device_readings_season ON device_readings(season);

CREATE INDEX IF NOT EXISTS idx_energy_aggregates_device_id ON energy_aggregates(device_id);
CREATE INDEX IF NOT EXISTS idx_energy_aggregates_date_hour ON energy_aggregates(date_hour);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO energy_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO energy_user;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO energy_user;