from unittest.mock import patch, MagicMock
from src.services import job_builder


def test_create_job():
    data = {"foo": "bar"}
    job = job_builder.create_job(data)
    assert job["status"] == "CREATED"
    assert job["data"] == data
    assert "job_id" in job

@patch("src.job_builder.Producer")
def test_send_to_job_orchestrator(mock_producer):
    job_data = job_builder.create_job({"foo": "bar"})
    instance = mock_producer.return_value
    instance.produce = MagicMock()
    instance.flush = MagicMock()
    result = job_builder.send_to_job_orchestrator(job_data)
    assert result is True
    instance.produce.assert_called_once()
    instance.flush.assert_called_once()