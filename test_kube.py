from kubernetes import client, config
from kubernetes.client.rest import ApiException


def _get_pod_logs(namespace, pod_name, container_name=None):
    # Load kubeconfig
    config.load_kube_config()

    # Create an instance of the API class
    v1 = client.CoreV1Api()

    try:
        if container_name:
            # Get logs from a specific container in the pod
            logs = v1.read_namespaced_pod_log(name=pod_name, namespace=namespace, container=container_name)
        else:
            # Get logs from the pod
            logs = v1.read_namespaced_pod_log(name=pod_name, namespace=namespace)
        print(logs)
        return logs
    except ApiException as e:
        print("Exception when calling CoreV1Api->read_namespaced_pod_log: %s\n" % e)


if __name__ == "__main__":
    print('driver log =============================================start')
    _get_pod_logs(namespace='default', pod_name='spark-pi-job-1356088fa0a20705-driver')
    print('driver log ============================================= end')
