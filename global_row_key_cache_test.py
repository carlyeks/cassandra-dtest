import time

from dtest import Tester, debug
from loadmaker import LoadMaker

class TestGlobalRowKeyCache(Tester):

    def __init__(self, *argv, **kwargs):
        super(TestGlobalRowKeyCache, self).__init__(*argv, **kwargs)
        # When a node goes down under load it prints an error in it's log.
        # If we don't allow log errors, then the test will fail.
#        self.allow_log_errors = True

    def functional_test(self):
        """
        Test global caches.

        Test that save and load work in the situation when you write to
        different CFs. Read 2 or 3 times to make sure the page cache doesn't
        skew the results.
        """

        # create some rows to insert
        NUM_INSERTS = 100
        NUM_UPDATES = 10
        NUM_DELETES = 1

        cluster = self.cluster
        cluster.populate(3)
        node1 = cluster.nodelist()[0]

        for kcsim in (0, 10):
            for rcsim in (0, 10):
                setup_name = "%d_%d" % (kcsim, rcsim)
                ks_name = 'ks_' + setup_name

                debug("setup " + setup_name)
                cluster.set_configuration_options(values={
                        'key_cache_size_in_mb': kcsim,
                        'row_cache_size_in_mb': rcsim,
                        'row_cache_save_period': 5,
                        'key_cache_save_period': 5,
                        })
                cluster.start()
                time.sleep(.5)
                cursor = self.cql_connection(node1)
                self.create_ks(cursor, ks_name, 3)
                time.sleep(1) # wait for propagation

                host, port = node1.network_interfaces['thrift']

                # create some load makers
                lm_standard = LoadMaker(host, port,
                        keyspace_name=ks_name, column_family_type='standard')
                lm_counter = LoadMaker(host, port,
                        keyspace_name=ks_name, column_family_type='standard', is_counter=True)

                # insert some rows
                lm_standard.generate(NUM_INSERTS)
                lm_counter.generate(NUM_INSERTS)

                # flush everything to get it into sstables
                for node in cluster.nodelist():
                    node.flush()

                debug("Validating")
                for i in range(3):
                    # read and modify multiple times to get data into and invalidated out of the cache.
                    lm_standard.update(NUM_UPDATES).delete(NUM_DELETES).validate()
                    lm_counter.generate().validate()

                # let the data be written to the row/key caches.
                debug("Letting caches be written")
                time.sleep(10)
                debug("Stopping cluster")
                cluster.stop(gently=False)
                time.sleep(1)
                debug("Starting cluster")
                cluster.start()
                time.sleep(5) # read the data back from row and key caches

                lm_standard.refresh_connection()
                lm_counter.refresh_connection()

                debug("Validating again...")
                for i in range(2):
                    # read and modify multiple times to get data into and invalidated out of the cache.
                    lm_standard.validate()
                    lm_counter.validate()


                cluster.stop()
