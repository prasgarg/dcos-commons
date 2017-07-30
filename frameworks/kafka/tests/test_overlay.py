import os
import pytest
import shakedown

import sdk_install
import sdk_tasks
import sdk_plan
import sdk_networks
import sdk_utils

from tests.utils import  *

SERVICE_NAME = "test/overlay/{}".format(SERVICE_NAME)

@pytest.fixture(scope='module', autouse=True)
def configure_package(configure_universe):
    try:
        sdk_install.uninstall(SERVICE_NAME, PACKAGE_NAME)
        sdk_utils.gc_frameworks()

        sdk_install.install(
            PACKAGE_NAME,
            DEFAULT_BROKER_COUNT,
            service_name=SERVICE_NAME,
            additional_options=sdk_utils.merge_dictionaries(
                { "service": { "name": SERVICE_NAME } },
                sdk_networks.ENABLE_VIRTUAL_NETWORKS_OPTIONS
            ))
        sdk_plan.wait_for_completed_deployment(SERVICE_NAME)

        yield # let the test session execute
    finally:
        sdk_install.uninstall(SERVICE_NAME, PACKAGE_NAME)


@pytest.mark.overlay
@pytest.mark.smoke
@pytest.mark.sanity
@sdk_utils.dcos_1_9_or_higher
def test_service_overlay_health():
    """Installs SDK based Kafka on with virtual networks set to True. Tests that the deployment completes
    and the service is healthy, then checks that all of the service tasks (brokers) are on the overlay network
    """
    shakedown.service_healthy(SERVICE_NAME)
    broker_tasks = sdk_tasks.get_task_ids(SERVICE_NAME, "broker-")
    for task in broker_tasks:
        sdk_networks.check_task_network(task)


@pytest.mark.smoke
@pytest.mark.sanity
@pytest.mark.overlay
@sdk_utils.dcos_1_9_or_higher
def test_overlay_network_deployment_and_endpoints():
    # double check
    sdk_tasks.check_running(SERVICE_NAME, DEFAULT_BROKER_COUNT)
    endpoints = sdk_networks.get_and_test_endpoints(
            "",
            PACKAGE_NAME,
            2,
            service_name=SERVICE_NAME)
    assert "broker" in endpoints, "broker is missing from endpoints {}".format(endpoints)
    assert "zookeeper" in endpoints, "zookeeper missing from endpoints {}".format(endpoints)
    broker_endpoints = sdk_networks.get_and_test_endpoints(
            "broker",
            PACKAGE_NAME,
            3,
            service_name=SERVICE_NAME)
    sdk_networks.check_endpoints_on_overlay(broker_endpoints)

    zookeeper = service_cli('endpoints zookeeper', get_json=False, service_name=SERVICE_NAME)
    assert zookeeper.rstrip() == 'master.mesos:2181/dcos-service-{}'.format(SERVICE_NAME.replace("/", "__"))


@pytest.mark.sanity
@pytest.mark.overlay
@sdk_utils.dcos_1_9_or_higher
def test_pod_restart_on_overlay():
    restart_broker_pods(SERVICE_NAME)
    test_overlay_network_deployment_and_endpoints()


@pytest.mark.sanity
@pytest.mark.overlay
@sdk_utils.dcos_1_9_or_higher
def test_pod_replace_on_overlay():
    replace_broker_pod(SERVICE_NAME)
    test_overlay_network_deployment_and_endpoints()


@pytest.mark.sanity
@pytest.mark.overlay
@sdk_utils.dcos_1_9_or_higher
def test_topic_create_overlay():
    create_topic(SERVICE_NAME)


@pytest.mark.sanity
@pytest.mark.overlay
@sdk_utils.dcos_1_9_or_higher
def test_topic_delete_overlay():
    delete_topic(SERVICE_NAME)
