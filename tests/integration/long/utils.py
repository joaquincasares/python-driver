import logging
import time

from collections import defaultdict
from ccmlib.node import Node

from tests.integration import get_node, get_cluster, CCM_CLUSTER


log = logging.getLogger(__name__)


class CoordinatorStats():
    def __init__(self):
        self.coordinator_counts = defaultdict(int)

    def add_coordinator(self, future):
        coordinator = future._current_host.address
        self.coordinator_counts[coordinator] += 1

        if future._errors:
            log.error('future._errors: %s', future._errors)
        future.result()

    def reset_counts(self):
        self.coordinator_counts = defaultdict(int)

    def assert_query_count_equals(self, testcase, node, expected):
        ip = '127.0.0.%d' % node
        if self.coordinator_counts[ip] != expected:
            testcase.fail(
                'Expected %d queries to %s, but got %d. Query counts: %s' % (
                    expected, ip, self.coordinator_counts[ip],
                    dict(self.coordinator_counts)))


def create_schema(session, keyspace, simple_strategy=True,
                  replication_factor=1, replication_strategy=None):
    results = session.execute(
        'SELECT keyspace_name FROM system.schema_keyspaces')
    existing_keyspaces = [row[0] for row in results]
    if keyspace in existing_keyspaces:
        session.execute('DROP KEYSPACE %s' % keyspace, timeout=10)

    if simple_strategy:
        ddl = "CREATE KEYSPACE %s WITH replication" \
              " = {'class': 'SimpleStrategy', 'replication_factor': '%s'}"
        session.execute(ddl % (keyspace, replication_factor), timeout=10)
    else:
        if not replication_strategy:
            raise Exception('replication_strategy is not set')

        ddl = "CREATE KEYSPACE %s" \
              " WITH replication = { 'class' : 'NetworkTopologyStrategy', %s }"
        session.execute(ddl % (keyspace, str(replication_strategy)[1:-1]),
                        timeout=10)

    ddl = 'CREATE TABLE %s.cf (k int PRIMARY KEY, i int)'
    session.execute(ddl % keyspace, timeout=10)
    session.execute('USE %s' % keyspace)


def start(node):
    get_node(node).start()


def stop(node):
    get_node(node).stop()


def force_stop(node):
    get_node(node).stop(wait=False, gently=False)


def decommission(node):
    get_node(node).decommission()
    get_node(node).stop()


def bootstrap(node, data_center=None):
    print 'createnode'
    node_instance = Node('node%s' % node,
                         get_cluster(),
                         auto_bootstrap=True,
                         thrift_interface=('127.0.0.%s' % node, 9160),
                         storage_interface=('127.0.0.%s' % node, 7000),
                         jmx_port=str(7000 + 100 * node),
                         remote_debug_port=0,
                         initial_token=None)
    print 'add node'
    get_cluster().add(node_instance, is_seed=False, data_center=data_center)
    print 'start node'
    get_node(node).start()
    print 'done'


def ring(node):
    print 'From node%s:' % node
    get_node(node).nodetool('ring')


def wait_for_up(cluster, node, wait=True):
    start_time = time.time()
    while True:
        host = cluster.metadata.get_host('127.0.0.%s' % node)
        if host and host.is_up:
            # BUG: shouldn't have to, but we do
            if wait:
                time.sleep(5)
            return

        timeout = 60
        if time.time() - start_time > timeout:
            raise RuntimeError(
                'Host did not come up after %s seconds.' % timeout)


def wait_for_down(cluster, node, wait=True):
    start_time = time.time()
    while True:
        host = cluster.metadata.get_host('127.0.0.%s' % node)
        if not host or not host.is_up:
            # BUG: shouldn't have to, but we do
            if wait:
                time.sleep(5)
            return

        timeout = 60
        if time.time() - start_time > timeout:
            raise RuntimeError(
                'Host did not go down after %s seconds.' % timeout)
