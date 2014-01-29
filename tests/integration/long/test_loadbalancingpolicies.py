import struct
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy, DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement
from tests.integration import use_multidc, use_singledc
from tests.integration.long.utils import wait_for_up, create_schema, \
    CoordinatorStats, force_stop, wait_for_down, decommission, start, ring, \
    bootstrap

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa


class LoadBalancingPolicyTests(unittest.TestCase):
    def setUp(self):
        self.coordinator_stats = CoordinatorStats()

    def _insert(self, session, keyspace, count=12,
                consistency_level=ConsistencyLevel.ONE):
        session.execute('USE %s' % keyspace)
        for i in range(count):
            ss = SimpleStatement('INSERT INTO cf(k, i) VALUES (0, 0)',
                                 consistency_level=consistency_level)
            session.execute(ss)

    def _query(self, session, keyspace, count=12,
               consistency_level=ConsistencyLevel.ONE):
        routing_key = struct.pack('>i', 0)
        for i in range(count):
            ss = SimpleStatement('SELECT * FROM %s.cf WHERE k = 0' % keyspace,
                                 consistency_level=consistency_level,
                                 routing_key=routing_key)
            self.coordinator_stats.add_coordinator(session.execute_async(ss))


    def test_roundrobin(self):
        keyspace = 'test_roundrobin'
        cluster = Cluster(
            load_balancing_policy=RoundRobinPolicy())
        session = cluster.connect()
        wait_for_up(cluster, 1, wait=False)
        wait_for_up(cluster, 2, wait=False)
        wait_for_up(cluster, 3)

        create_schema(session, keyspace, replication_factor=3)
        self._insert(session, keyspace)
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 4)
        self.coordinator_stats.assert_query_count_equals(self, 2, 4)
        self.coordinator_stats.assert_query_count_equals(self, 3, 4)

        try:
            force_stop(3)
            wait_for_down(cluster, 3)

            self.coordinator_stats.reset_counts()
            self._query(session, keyspace)

            self.coordinator_stats.assert_query_count_equals(self, 1, 6)
            self.coordinator_stats.assert_query_count_equals(self, 2, 6)
            self.coordinator_stats.assert_query_count_equals(self, 3, 0)

            decommission(1)
            start(3)
            wait_for_down(cluster, 1)
            wait_for_up(cluster, 3)

            self.coordinator_stats.reset_counts()
            self._query(session, keyspace)

            self.coordinator_stats.assert_query_count_equals(self, 1, 0)
            self.coordinator_stats.assert_query_count_equals(self, 2, 6)
            self.coordinator_stats.assert_query_count_equals(self, 3, 6)
        finally:
            try: start(1)
            except: pass
            try: start(3)
            except: pass

            wait_for_up(cluster, 3)

    def test_roundrobin_two_dcs(self):
        use_multidc([2,2])
        keyspace = 'test_roundrobin_two_dcs'
        cluster = Cluster(
            load_balancing_policy=RoundRobinPolicy())
        session = cluster.connect()
        wait_for_up(cluster, 1, wait=False)
        wait_for_up(cluster, 2, wait=False)
        wait_for_up(cluster, 3, wait=False)
        wait_for_up(cluster, 4)

        create_schema(session, keyspace, replication_strategy=[2,2])
        self._insert(session, keyspace)
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 3)
        self.coordinator_stats.assert_query_count_equals(self, 2, 3)
        self.coordinator_stats.assert_query_count_equals(self, 3, 3)
        self.coordinator_stats.assert_query_count_equals(self, 4, 3)

        force_stop(1)
        bootstrap(5, 'dc3')

        # # BUG: PYTHON-45
        ring(2)
        ring(5)
        import time
        time.sleep(20)
        ring(2)
        ring(5)
        import time
        time.sleep(60)
        ring(2)
        ring(5)
        # session = cluster.connect()
        ring(2)
        ring(5)

        wait_for_up(cluster, 5)

        self.coordinator_stats.reset_counts()
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 3)
        self.coordinator_stats.assert_query_count_equals(self, 3, 3)
        self.coordinator_stats.assert_query_count_equals(self, 4, 3)
        self.coordinator_stats.assert_query_count_equals(self, 5, 3)

    def test_roundrobin_two_dcs_2(self):
        use_multidc([2,2])
        keyspace = 'test_roundrobin_two_dcs'
        cluster = Cluster(
            load_balancing_policy=RoundRobinPolicy())
        session = cluster.connect()
        wait_for_up(cluster, 1, wait=False)
        wait_for_up(cluster, 2, wait=False)
        wait_for_up(cluster, 3, wait=False)
        wait_for_up(cluster, 4)

        create_schema(session, keyspace, replication_strategy=[2,2])
        self._insert(session, keyspace)
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 3)
        self.coordinator_stats.assert_query_count_equals(self, 2, 3)
        self.coordinator_stats.assert_query_count_equals(self, 3, 3)
        self.coordinator_stats.assert_query_count_equals(self, 4, 3)

        force_stop(1)
        bootstrap(5, 'dc1')

        # BUG: PYTHON-45
        session = cluster.connect()

        wait_for_up(cluster, 5)

        self.coordinator_stats.reset_counts()
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 3)
        self.coordinator_stats.assert_query_count_equals(self, 3, 3)
        self.coordinator_stats.assert_query_count_equals(self, 4, 3)
        self.coordinator_stats.assert_query_count_equals(self, 5, 3)

    def test_dc_aware_roundrobin_one_remote_host(self):
        use_multidc([3,2])
        keyspace = 'test_roundrobin'
        cluster = Cluster(
            load_balancing_policy=DCAwareRoundRobinPolicy('dc2'))
        session = cluster.connect()
        wait_for_up(cluster, 1, wait=False)
        wait_for_up(cluster, 2, wait=False)
        wait_for_up(cluster, 3, wait=False)
        wait_for_up(cluster, 4)

        create_schema(session, keyspace, replication_strategy=[2, 2])
        self._insert(session, keyspace)
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 0)
        self.coordinator_stats.assert_query_count_equals(self, 3, 6)
        self.coordinator_stats.assert_query_count_equals(self, 4, 6)
