import os
import yaml
from airflow.models import Variable
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class Config:
    def __init__(self):
        self.is_local = os.getenv('AIRFLOW_ENV', 'local') == 'local'
        if self.is_local:
            load_dotenv()
        self._config = self._load_config()

    def _load_config(self):
        logger.info("Starting to load configuration")
        current_dir = os.path.dirname(os.path.abspath(__file__))
        settings_path = os.path.join(current_dir, '..', 'config', 'settings.yaml')
        with open(settings_path, 'r') as file:
            yaml_config = yaml.safe_load(file)

        processed_config = self._process_config(yaml_config)
        logger.info("Finished loading configuration")
        return processed_config

    def _process_config(self, config):
        if isinstance(config, dict):
            return {k: self._process_config(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._process_config(v) for v in config]
        elif isinstance(config, str) and config.startswith('${') and config.endswith('}'):
            env_var = config[2:-1]
            default_value = None
            if ':' in env_var:
                env_var, default_value = env_var.split(':', 1)
            value = self.get_variable(env_var, default=default_value)
            logger.info(f"Replaced {config} with {value}")
            return value
        else:
            return config

    def get_variable(self, key, default=None):
        value = os.environ.get(key)
        if value is not None:
            logger.info(f"Retrieved {key} from EnvironmentVariable")
            return value
        if not self.is_local:
            try:
                value = Variable.get(key)
                logger.info(f"Retrieved {key} from Airflow Variable")
                return value
            except KeyError:
                pass
        logger.info(f"Using default value for {key}: {default}")
        return default

    def get_config(self):
        return self._config

    def get_aws_credentials(self):
        return {
            'region_name': self._config['aws']['region_name'],
            'aws_access_key_id': self.get_variable('AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': self.get_variable('AWS_SECRET_ACCESS_KEY')
        }

# Create a singleton instance
_config_instance = Config()

# Provide a function to get the config
def get_config():
    return _config_instance.get_config()

# Provide a function to get AWS credentials
def get_aws_credentials():
    return _config_instance.get_aws_credentials()

# Provide a function to get a specific variable
def get_variable(key, default=None):
    return _config_instance.get_variable(key, default)
