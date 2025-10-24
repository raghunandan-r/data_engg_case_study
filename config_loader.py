"""
Configuration loader for the ETL pipeline.
Loads config from JSON file and provides validation.
"""
import json
import os
from pathlib import Path
from typing import Any, Dict


class ConfigLoader:
    """Singleton configuration loader with validation."""

    _instance = None
    _config: Dict[str, Any] = {}
    _loaded = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigLoader, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """Load configuration from JSON file on first instantiation."""
        if not self._loaded:
            config_path = os.getenv("ETL_CONFIG", "config.json")
            self._load_config(config_path)
            self._validate()
            self._loaded = True

    def _load_config(self, config_path: str):
        """Load and parse the configuration file."""
        config_file = Path(config_path)

        if not config_file.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {config_path}. "
                "Please create it from config.json or set ETL_CONFIG env var."
            )

        with open(config_file, "r") as f:
            self._config = json.load(f)

    def _validate(self):
        """Validate required sections and value ranges."""
        # Required sections
        required_sections = [
            "database_path",
            "paths",
            "sqs",
            "staging",
            "parsing",
            "silver",
        ]
        missing = [s for s in required_sections if s not in self._config]
        if missing:
            raise ValueError(f"Missing required config sections: {missing}")

        # Validate SQS batch size (AWS limit is 1-10)
        batch_size = (
            self._config.get("sqs", {}).get("polling", {}).get("receive_batch_size", 10)
        )
        if not (1 <= batch_size <= 10):
            raise ValueError(
                f"sqs.polling.receive_batch_size must be between 1 and 10 (AWS limit), got {batch_size}"
            )

        # Validate numeric types
        session_mins = self._config.get("silver", {}).get("session_minutes")
        if not isinstance(session_mins, (int, float)) or session_mins <= 0:
            raise ValueError(
                f"silver.session_minutes must be positive number, got {session_mins}"
            )

        high_value = self._config.get("silver", {}).get("high_value_abandoned_checkout")
        if not isinstance(high_value, (int, float)) or high_value < 0:
            raise ValueError(
                f"silver.high_value_abandoned_checkout must be non-negative number, got {high_value}"
            )

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.

        Args:
            key_path: Dot-separated path like 'database_path' or 'sqs.polling.max_messages_per_run'
            default: Default value if key not found

        Returns:
            Configuration value or default

        Example:
            config.get('database_path')
            config.get('sqs.polling.max_messages_per_run', 10000)
        """
        keys = key_path.split(".")
        value = self._config

        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default

        return value

    def get_env_value(self, config_key: str, required: bool = True) -> str:
        """
        Get environment variable name from config and read its value.

        Args:
            config_key: Key path to env var name in config (e.g., 'sqs.queue_url_env')
            required: If True, raise error when env var is not set

        Returns:
            Environment variable value

        Example:
            # config has: "sqs": {"queue_url_env": "SQS_QUEUE_URL"}
            queue_url = config.get_env_value('sqs.queue_url_env')
        """
        env_var_name = self.get(config_key)
        if not env_var_name:
            raise ValueError(f"Config key '{config_key}' not found")

        value = os.getenv(env_var_name)
        if required and not value:
            raise ValueError(
                f"Environment variable '{env_var_name}' (from config key '{config_key}') is not set. "
                f"Please set it in your .env file."
            )
        return value

    def get_path(
        self, path_key: str, create: bool = False, base_dir: str = None
    ) -> Path:
        """
        Get a path from config and optionally create the directory.

        Args:
            path_key: Key to path in config (e.g., 'paths.staging_dir')
            create: If True, create the directory
            base_dir: Base directory for relative paths (defaults to current dir)

        Returns:
            Path object (absolute)
        """
        path_str = self.get(path_key)
        if not path_str:
            raise ValueError(f"Path key '{path_key}' not found in config")

        path = Path(path_str)

        # Make absolute if relative
        if not path.is_absolute():
            if base_dir:
                path = Path(base_dir) / path
            else:
                path = Path.cwd() / path

        if create:
            path.mkdir(parents=True, exist_ok=True)

        return path

    def reload(self):
        """Force reload configuration from file (useful for testing)."""
        self._loaded = False
        self.__init__()


# Global singleton instance
_loader = None


def load_config() -> ConfigLoader:
    """Get or create the global configuration loader instance."""
    global _loader
    if _loader is None:
        _loader = ConfigLoader()
    return _loader
