import requests
import time
from typing import Optional, Dict, Any
from src.util.logger import setup_logger

logger = setup_logger('tsc_config_client')

# List of required fields that must be present in the configuration
REQUIRED_FIELDS = [
  "report_type",
  "frequency",
  "fileType",
  "fileName",
  "fileStructure",
  "job"
]

def build_url(customer_id: str, tsc_id: str) -> str:
  """
  Build the REST URL dynamically using customerId and tscId.

  :param customer_id: The customer identifier.
  :param tsc_id: The TSC identifier.
  :return: The constructed REST URL.
  """
  base_url = "http://<host>:<port>/rest/itt/mft/soa/configurationMgmt/interfaces/jsonRest"
  return f"{base_url}/customer/{customer_id}/configuration?tscId={tsc_id}"

def validate_config(data: Dict[str, Any]) -> bool:
  """
  Validate the presence of required fields in the configuration.

  :param data: The configuration dictionary.
  :return: True if all required fields are present, False otherwise.
  """
  missing = [field for field in REQUIRED_FIELDS if field not in data or data[field] is None]
  if missing:
      logger.error(f"Missing required configuration fields: {missing}")
      return False
  return True

def get_tsc_configuration(tsc_id: str, customer_id: str, max_retries: int = 3, backoff_factor: float = 1.5) -> Optional[Dict[str, Any]]:
  """
  Retrieve TSC configuration from the remote REST service with retry and validation.

  :param tsc_id: The TSC identifier to query the configuration for.
  :param customer_id: The customer identifier to build the REST URL.
  :param max_retries: Maximum number of retry attempts.
  :param backoff_factor: Backoff multiplier for exponential wait.
  :return: A dictionary with the relevant configuration fields, or None if the request fails or validation fails.
  """
  url = build_url(customer_id, tsc_id)
  attempt = 0
  while attempt < max_retries:
      try:
          # Send a GET request to the REST service with the constructed URL.
          response = requests.get(url, timeout=5)
          response.raise_for_status()
          data = response.json()
          # Extract and return only the required fields from the response.
          config = {
              "report_type": data.get("report_type"),
              "frequency": data.get("frequency"),
              "fileType": data.get("fileType"),
              "fileName": data.get("fileName"),
              "fileStructure": data.get("fileStructure"),
              "job": data.get("job"),
              "packages": data.get("packages"),
              "customConfiguration": data.get("customConfiguration"),
          }
          if validate_config(config):
              return config
          else:
              logger.error("Invalid configuration received from REST service.")
              return None
      except Exception as e:
          logger.warning(f"REST call failed (attempt {attempt + 1}/{max_retries}): {e}")
          if attempt < max_retries - 1:
              sleep_time = backoff_factor ** attempt
              logger.info(f"Retrying in {sleep_time:.1f} seconds...")
              time.sleep(sleep_time)
          attempt += 1
  logger.error("All REST attempts failed.")
  return None