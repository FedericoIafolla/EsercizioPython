import yaml

def frequency_to_cron(frequency: str) -> str:
    """
    Convert a frequency string to a cron schedule.
    Supported: 'daily', 'hourly', 'weekly', 'monthly'
    """
    mapping = {
        "hourly": "0 * * * *",
        "daily": "0 0 * * *",
        "weekly": "0 0 * * 0",
        "monthly": "0 0 1 * *"
    }
    return mapping.get(frequency.lower(), "0 0 * * *")  # Default: daily

def generate_cronjob_manifest(config: dict) -> str:
    """
    Generate a Kubernetes CronJob manifest YAML from a TSC configuration.
    :param config: Dictionary with TSC configuration fields.
    :return: YAML string for the CronJob manifest.
    """
    cron_schedule = frequency_to_cron(config["frequency"])
    cronjob = {
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {
            "name": f"tsc-job-{config['tsc_id']}-{config['customer_id']}".lower()
        },
        "spec": {
            "schedule": cron_schedule,
            "jobTemplate": {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "csv-exporter",
                                    "image": "my-registry/csv-exporter:latest",
                                    "command": ["python", "export.py"],
                                    "args": [
                                        "--tsc-id", str(config["tsc_id"]),
                                        "--customer-id", str(config["customer_id"]),
                                        "--report-type", str(config["report_type"])
                                    ],
                                    "volumeMounts": [
                                        {
                                            "name": "shared-storage",
                                            "mountPath": "/mnt/data"
                                        }
                                    ]
                                }
                            ],
                            "restartPolicy": "OnFailure",
                            "volumes": [
                                {
                                    "name": "shared-storage",
                                    "persistentVolumeClaim": {
                                        "claimName": "shared-pvc"
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }
    }
    return yaml.dump(cronjob, sort_keys=False)