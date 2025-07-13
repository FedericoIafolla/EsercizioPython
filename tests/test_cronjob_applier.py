import pytest
import yaml
from unittest.mock import patch, MagicMock
from kubernetes.client.rest import ApiException
from src.service.cronjob_applier import apply_cronjob_manifest

# Example YAML manifest for tests
VALID_MANIFEST_YAML = """
apiVersion: batch/v1
kind: CronJob
metadata:
  name: test-cronjob
  namespace: test-ns
spec:
  schedule: "* * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: test-container
            image: busybox
          restartPolicy: OnFailure
"""

# Common patches for all tests in this module
@patch('src.service.cronjob_applier.config.load_kube_config')
@patch('src.service.cronjob_applier.client.BatchV1Api')
@patch('src.service.cronjob_applier.logger')
def test_apply_creates_new_cronjob(mock_logger, mock_batch_v1_api, mock_load_config):
    """Tests the creation of a CronJob when one does not exist."""
    # Setup: The API raises a 404 (Not Found) when trying to read the CronJob
    mock_api_instance = MagicMock()
    mock_api_instance.read_namespaced_cron_job.side_effect = ApiException(status=404)
    mock_batch_v1_api.return_value = mock_api_instance

    result = apply_cronjob_manifest(VALID_MANIFEST_YAML)

    # Assertions
    mock_load_config.assert_called_once()
    manifest_dict = yaml.safe_load(VALID_MANIFEST_YAML)
    mock_api_instance.create_namespaced_cron_job.assert_called_once_with(
        body=manifest_dict,
        namespace='test-ns'
    )
    mock_api_instance.replace_namespaced_cron_job.assert_not_called()
    mock_logger.info.assert_any_call("CronJob test-cronjob not found. Creating...")
    assert result is True

@patch('src.service.cronjob_applier.config.load_kube_config')
@patch('src.service.cronjob_applier.client.BatchV1Api')
@patch('src.service.cronjob_applier.logger')
def test_apply_replaces_existing_cronjob(mock_logger, mock_batch_v1_api, mock_load_config):
    """Tests the update of an existing CronJob."""
    # Setup: Reading the CronJob succeeds, no exceptions are raised
    mock_api_instance = MagicMock()
    mock_api_instance.read_namespaced_cron_job.return_value = True # Simulate existence
    mock_batch_v1_api.return_value = mock_api_instance

    result = apply_cronjob_manifest(VALID_MANIFEST_YAML)

    # Assertions
    manifest_dict = yaml.safe_load(VALID_MANIFEST_YAML)
    mock_api_instance.replace_namespaced_cron_job.assert_called_once_with(
        name='test-cronjob',
        namespace='test-ns',
        body=manifest_dict
    )
    mock_api_instance.create_namespaced_cron_job.assert_not_called()
    mock_logger.info.assert_any_call("CronJob test-cronjob already exists. Replacing...")
    assert result is True

@patch('src.service.cronjob_applier.config.load_kube_config')
@patch('src.service.cronjob_applier.client.BatchV1Api')
@patch('src.service.cronjob_applier.logger')
def test_apply_api_error_on_read(mock_logger, mock_batch_v1_api, mock_load_config):
    """Tests the handling of a generic API error during read."""
    # Setup: The API raises a non-404 exception
    api_error = ApiException(status=500, reason="Internal Server Error")
    mock_api_instance = MagicMock()
    mock_api_instance.read_namespaced_cron_job.side_effect = api_error
    mock_batch_v1_api.return_value = mock_api_instance

    result = apply_cronjob_manifest(VALID_MANIFEST_YAML)

    # Assertions
    mock_logger.error.assert_called_with(f"Error checking CronJob test-cronjob: {api_error}")
    assert result is False

@patch('src.service.cronjob_applier.config.load_kube_config')
@patch('src.service.cronjob_applier.logger')
def test_apply_invalid_yaml(mock_logger, mock_load_config):
    """Tests the handling of a malformed YAML manifest."""
    invalid_yaml = "key: value: another_value"
    result = apply_cronjob_manifest(invalid_yaml)

    mock_logger.error.assert_called_once()
    assert "Error parsing YAML manifest" in mock_logger.error.call_args[0][0]
    assert result is False