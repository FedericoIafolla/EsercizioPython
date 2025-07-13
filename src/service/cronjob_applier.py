from kubernetes import config, client
from kubernetes.client.rest import ApiException
import yaml
from src.util.logger import setup_logger

logger = setup_logger('cronjob_applier')


def apply_cronjob_manifest(manifest_yaml: str):
    """
    Applies a Kubernetes CronJob manifest to the cluster.
    Creates the CronJob if it doesn't exist, updates it if it does.

    Args:
        manifest_yaml: The CronJob manifest as a YAML string.

    Returns:
        True if the CronJob was successfully applied, False otherwise.
    """
    try:
        # Load Kubernetes configuration
        config.load_kube_config()
        api_instance = client.BatchV1Api()

        # Parse the YAML manifest
        manifest = yaml.safe_load(manifest_yaml)

        # Extract name and namespace
        name = manifest["metadata"]["name"]
        namespace = manifest["metadata"].get("namespace", "default")  # Default to 'default' namespace

        # Try to read the CronJob to check if it already exists
        try:
            api_instance.read_namespaced_cron_job(name=name, namespace=namespace)
            # If it exists, replace it
            logger.info(f"CronJob {name} already exists. Replacing...")
            api_instance.replace_namespaced_cron_job(name=name, namespace=namespace, body=manifest)
            logger.info(f"CronJob {name} updated successfully.")
            return True
        except ApiException as e:
            if e.status == 404:
                # If not found, create it
                logger.info(f"CronJob {name} not found. Creating...")
                api_instance.create_namespaced_cron_job(body=manifest, namespace=namespace)
                logger.info(f"CronJob {name} created successfully.")
                return True
            else:
                logger.error(f"Error checking CronJob {name}: {e}")
                return False

    except ApiException as e:
        logger.error(f"Kubernetes API error: {e}")
        return False
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML manifest: {e}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return False