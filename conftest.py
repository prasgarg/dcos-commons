""" This file configures python logging for the pytest framework
integration tests

Note: pytest must be invoked with this file in the working directory
E.G. py.test frameworks/<your-frameworks>/tests
"""
import logging
import os
import subprocess

import pytest

log_level = os.getenv('TEST_LOG_LEVEL', 'INFO').upper()

log_levels = ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'EXCEPTION')

assert log_level in log_levels, '{} is not a valid log level. ' \
    'Use one of: {}'.format(log_level, ', '.join(log_levels))

logging.basicConfig(
    format='[%(asctime)s|%(name)s|%(levelname)s]: %(message)s',
    level=log_level)

log = logging.getLogger(__name__)


def get_task_ids(user: str=None):
    """ This function uses dcos task WITHOUT the JSON options because
    that can return the wrong user for schedulers
    """
    tasks = subprocess.check_output(['dcos', 'task']).decode().split('\n')
    for task_str in tasks[1:]: # First line is the header line
        task = task_str.split()
        if len(task) < 5:
            continue
        if not user or task[2] == user:
            yield task[4]


def get_task_logs_for_id(task_id: str,  task_file: str='stdout', lines: int=1000000):
    try:
        task_logs = subprocess.check_output([
            'dcos', 'task', 'log', task_id, '--lines', str(lines), task_file
        ]).decode()
        return task_logs
    except subprocess.CalledProcessError as e:
        log.exception('Failed to get {} task log for task_id={}'.format(task_file, task_id))
        return None


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """
    See: https://docs.pytest.org/en/latest/example/simple.html\
    #making-test-result-information-available-in-fixtures
    """
    # execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    # set a report attribute for each phase of a call, which can
    # be "setup", "call", "teardown"

    setattr(item, "rep_" + rep.when, rep)


@pytest.fixture(autouse=True)
def get_task_logs_on_failure(request):
    """ Scheduler should be the only task running as root
    """
    yield
    for report in ('rep_setup', 'rep_call', 'rep_teardown'):
        if not hasattr(request.node, report):
            continue
        if not getattr(request.node, report).failed:
            continue
        for task_id in get_task_ids():
            for task_file in ('stderr', 'stdout'):
                log_name = '{}_{}_{}.log'.format(request.node.name, task_id, task_file)
                task_logs = get_task_logs_for_id(task_id, task_file=task_file)
                if task_logs:
                    with open(log_name, 'w') as f:
                        f.write(task_logs)
