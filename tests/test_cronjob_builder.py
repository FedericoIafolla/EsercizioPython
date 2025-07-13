import pytest
import yaml
from unittest.mock import Mock, patch
from src.service.cronjob_builder import (
    frequency_to_cron,
    generate_cronjob_manifest,
    send_to_job_orchestrator
)

# 1. Test for frequency_to_cron
@pytest.mark.parametrize("frequency, expected_cron", [
    ("hourly", "0 * * * *"),
    ("daily", "0 0 * * *"),
    ("weekly", "0 0 * * 0"),
    ("monthly", "0 0 1 * *"),
    ("invalid_frequency", "0 0 * * *"),  # Test default case
    ("DAILY", "0 0 * * *"),  # Test case-insensitivity
])
def test_frequency_to_cron(frequency, expected_cron):
    """Tests the conversion from frequency to cron schedule for various inputs."""
    assert frequency_to_cron(frequency) == expected_cron

# 2. Test for generate_cronjob_manifest
def test_generate_cronjob_manifest():
    """Tests the generation of the YAML manifest for the Kubernetes CronJob."""
    config = {
        "tsc_id": "abc-123",
        "customer_id": "customer-x",
        "frequency": "weekly",
        "report_type": "summary"
    }

    yaml_manifest = generate_cronjob_manifest(config)

    # Load the generated YAML to inspect it easily
    manifest_dict = yaml.safe_load(yaml_manifest)

    # Key assertions
    assert manifest_dict["kind"] == "CronJob"
    assert manifest_dict["metadata"]["name"] == "tsc-job-abc-123-customer-x"
    assert manifest_dict["spec"]["schedule"] == "0 0 * * 0"  # Corresponds to 'weekly'
    
    container_spec = manifest_dict["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]
    assert container_spec["name"] == "csv-exporter"
    assert "--tsc-id" in container_spec["args"]
    assert "abc-123" in container_spec["args"]
    assert "--report-type" in container_spec["args"]
    assert "summary" in container_spec["args"]

# 3. Test for send_to_job_orchestrator
def test_send_to_job_orchestrator_success():
    """Tests the successful sending of the job to Kafka using a mocked producer."""
    # Create a mock for the Kafka producer
    mock_producer = Mock()
    mock_producer.produce.return_value = None
    mock_producer.flush.return_value = None

    job_manifest = "apiVersion: batch/v1\nkind: CronJob..."
    topic = "test-topic"

    # Use the mocked logger to avoid output during tests
    with patch('src.service.cronjob_builder.logger') as mock_logger:
        result = send_to_job_orchestrator(job_manifest, mock_producer, topic)

        assert result is True
        mock_producer.produce.assert_called_once_with(topic, value=job_manifest.encode('utf-8'))
        mock_producer.flush.assert_called_once()
        mock_logger.info.assert_called_with(f"Job sent to topic {topic}")

def test_send_to_job_orchestrator_failure():
    """Tests error handling if sending to Kafka fails."""
    # The mock producer will raise an exception when .produce() is called
    mock_producer = Mock()
    kafka_exception = Exception("Kafka connection error")
    mock_producer.produce.side_effect = kafka_exception

    job_manifest = "apiVersion: batch/v1..."
    topic = "test-topic"

    with patch('src.service.cronjob_builder.logger') as mock_logger:
        result = send_to_job_orchestrator(job_manifest, mock_producer, topic)

        assert result is False
        mock_producer.produce.assert_called_once()
        mock_producer.flush.assert_not_called()  # flush should not be called if produce fails
        mock_logger.error.assert_called_with(f"Failed to send job to Kafka: {kafka_exception}")