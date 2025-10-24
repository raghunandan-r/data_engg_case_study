# Configuration System

This ETL pipeline uses a centralized JSON configuration system to manage all tunable parameters.

## Quick Start

1. **Copy the example config:**
   ```bash
   cp config.example.json config.json
   ```

2. **Configure your environment variables in `.env`:**
   ```bash
   SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/...
   AWS_ACCESS_KEY_ID=...
   AWS_SECRET_ACCESS_KEY=...
   AWS_SESSION_TOKEN=...
   SSO_REGION=us-east-1
   SSO_START_URL=...
   ```

3. **Run the ETL:**
   ```bash
   python etl.py
   ```

## Configuration Structure

### Core Settings

- **`environment`**: Environment name (dev/staging/production)
- **`database_path`**: Path to DuckDB database file
- **`run_ts_format`**: Timestamp format for log filenames (strftime format)

### Paths (`paths.*`)

All paths can be relative (to project root) or absolute:

- **`raw_events_file`**: Temporary file for SQS messages
- **`staging_dir`**: Directory for processing batch files
- **`processed_dir`**: Archive for successfully processed batches
- **`archive_dir`**: Archive for raw batch files
- **`dead_letter_dir`**: Failed records go here
- **`logs_dir`**: Directory for all logs
- **`cache_dir`**: Disk cache for message deduplication

### SQS Settings (`sqs.*`)

- **`queue_url_env`**: Name of environment variable containing the SQS queue URL
- **`polling.max_messages_per_run`**: Maximum messages to poll per ETL run (default: 10000)
- **`polling.receive_batch_size`**: Messages per API call (AWS max: 10)
- **`polling.wait_time_seconds`**: Long polling wait time (AWS max: 20)
- **`message_cache_ttl_seconds`**: How long to remember seen message IDs (default: 1209600 = 2 weeks)

### Staging Settings (`staging.*`)

- **`staging_glob`**: Glob pattern to find staging files (default: `processing_batch_*.jsonl`)
- **`db_connect.max_retries`**: Retry attempts for DuckDB connection
- **`db_connect.retry_backoff_base_seconds`**: Base for exponential backoff (default: 0.5)

### Parsing Settings (`parsing.*`)

- **`required_fields`**: Fields that must be non-null in every event
- **`item_params_mappings`**: Maps upstream GA4 param names to internal column names
- **`experiment_pattern`**: Pattern to identify experiment codes (default: `NELO_XP_`)

### Silver Layer Settings (`silver.*`)

Business logic thresholds:

- **`session_minutes`**: Session inactivity timeout (default: 30)
- **`high_value_abandoned_checkout`**: USD threshold for high-value abandonment alerts (default: 500)

## Environment-Specific Configs

You can maintain multiple configs:

```bash
# Development
ETL_CONFIG=config.dev.json python etl.py

# Staging
ETL_CONFIG=config.staging.json python etl.py

# Production (uses config.json by default)
python etl.py
```

## Validation

The config loader validates on startup:

- Required sections exist
- SQS batch size is 1-10 (AWS limit)
- Numeric thresholds are positive
- All referenced files can be found

If validation fails, you'll get a clear error message.

## Testing

To use a test config in your tests:

```python
import os
os.environ['ETL_CONFIG'] = 'tests/config.test.json'
from config_loader import load_config

cfg = load_config()
```

## Common Adjustments

### Scale Up SQS Polling
```json
{
  "sqs": {
    "polling": {
      "max_messages_per_run": 50000,
      "wait_time_seconds": 10
    }
  }
}
```

### Change Session Window
```json
{
  "silver": {
    "session_minutes": 60
  }
}
```

### Update Parsing Mappings
```json
{
  "parsing": {
    "item_params_mappings": {
      "discountt": "discount_value",
      "newField": "new_column"
    }
  }
}
```

## Secrets Management

⚠️ **Never commit `config.json` with secrets!**

- Secrets (AWS keys, tokens) stay in `.env` (gitignored)
- Config only names which env vars to read
- `config.example.json` is safe to commit (no secrets)

