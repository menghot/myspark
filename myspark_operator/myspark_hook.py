from __future__ import annotations

from typing import Any, Iterator
from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook
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


class MySparkHook(SparkSubmitHook):
    def __init__(self, *args: Any, **kwargs: Any, ) -> None:
        super().__init__(*args, **kwargs)

    def submit(self, application: str = "", **kwargs: Any) -> None:
        super().submit(application, **kwargs)
        print('driver log =============================================start' + str(self._kubernetes_driver_pod))
        _get_pod_logs(namespace='default', pod_name=self._kubernetes_driver_pod)
        print('driver log ============================================= end')
