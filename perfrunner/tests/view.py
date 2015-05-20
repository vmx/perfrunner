import json
from time import sleep
from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest
from perfrunner.workloads.viewgen import ViewGen, ViewGenDev


class ViewTest(PerfTest):

    """
    The test measures time it takes to build views. This is just a base
    class, actual measurements happen in initial and incremental
    indexing tests.
    """

    def __init__(self, *args):
        super(ViewTest, self).__init__(*args)

        if self.view_settings.disabled_updates:
            options = {'updateMinChanges': 0, 'replicaUpdateMinChanges': 0}
        else:
            options = None

        self.ddocs = self._parse_ddocs(self.view_settings, options)
        #ViewGen().generate_ddocs(index_settings.views, options)

    def define_ddocs(self):
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                for ddoc_name, ddoc in self.ddocs.iteritems():
                    self.rest.create_ddoc(master, bucket, ddoc_name, ddoc)

    def build_index(self, views_key):
        """Query the views in order to build up the index

        `views_key` is the name of the property where the definitions
        are stored. It an be `views` or `spatial`.
        """
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                for ddoc_name, ddoc in self.ddocs.iteritems():
                    for view_name in ddoc[views_key]:
                        if views_key == 'views':
                            self.rest.query_view(master, bucket, ddoc_name,
                                                 view_name,
                                                 params={'limit': 10})
                        elif views_key == 'spatial':
                            self.rest.query_spatial(master, bucket, ddoc_name,
                                                    view_name,
                                                    params={'limit': 10})
        sleep(self.MONITORING_DELAY)
        for master in self.cluster_spec.yield_masters():
            self.monitor.monitor_task(master, 'indexer')

    def compact_index(self):
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                for ddoc_name in self.ddocs:
                    self.rest.trigger_index_compaction(master, bucket,
                                                       ddoc_name)
        for master in self.cluster_spec.yield_masters():
            self.monitor.monitor_task(master, 'view_compaction')

    # XXX vmx 2015-05-08: Let's see if this works
    #@property
    #def view_settings(self):
    #    raise NotImplementedError(
    #        "The subclass must have a view_settings property")

    @staticmethod
    def _parse_ddocs(view_settings, options):
        ddocs = {}
        if view_settings.indexes is None:
            logger.interrupt('Missing indexes param')
        for index in view_settings.indexes:
            ddoc_name, ddoc = index.split('::', 1)
            ddocs[ddoc_name] = json.loads(ddoc)
            if options:
                ddocs[ddoc_name]['options'] = options
        return ddocs


# XXX vmx 2015-05-15: I could probably use a mixin for the `self.view_settings` and `build_*_index()`.
class InitialAndIncrementalSpatialTest(ViewTest):
    """
    Initial indexing test with access phase for data/index mutation.
    It is critical to disable automatic index updates so that we can
    control index building.
    """
    def __init__(self, cluster_spec, test_config, verbose, experiment=None):
        self.view_settings = test_config.spatial_settings
        super(InitialAndIncrementalSpatialTest, self).__init__(
            cluster_spec, test_config, verbose, experiment)

    @with_stats
    def build_init_index(self):
        return super(InitialAndIncrementalSpatialTest, self).build_index(
            'spatial')

    @with_stats
    def build_incr_index(self):
        super(InitialAndIncrementalSpatialTest, self).build_index('spatial')

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.reporter.start()
        self.define_ddocs()
        from_ts, to_ts = self.build_init_index()
        time_elapsed = (to_ts - from_ts) / 1000.0

        time_elapsed = self.reporter.finish('Initial index', time_elapsed)
        self.reporter.post_to_sf(
            *self.metric_helper.get_indexing_meta(value=time_elapsed,
                                                  index_type='Initial')
        )

        self.access()
        self.wait_for_persistence()
        self.compact_bucket()

        from_ts, to_ts = self.build_incr_index()
        time_elapsed = (to_ts - from_ts) / 1000.0

        time_elapsed = self.reporter.finish('Incremental index', time_elapsed)
        self.reporter.post_to_sf(
            *self.metric_helper.get_indexing_meta(value=time_elapsed,
                                                  index_type='Incremental')
        )

# NOTE vmx 2015-05-08
#class DevIndexTest(ViewTest):
# 
#    """
#    Unlike base test this one introduces measurements per different index type.
#    It only used as a base class for view query tests (in order to get separate
#    measurements for different types of queries).
#    """
# 
#    def __init__(self, *args):
#        super(IndexTest, self).__init__(*args)
# 
#        index_type = self.test_config.index_settings.index_type
#        if index_type is None:
#            logger.interrupt('Missing index_type param')
#        self.ddocs = ViewGenDev().generate_ddocs(index_type)
# 
# 
#class DevInitialIndexTest(DevIndexTest, InitialIndexTest):
# 
#    pass


class ViewQueryTest(ViewTest):

    """
    The base test which defines workflow for different view query tests. Access
    phase represents mixed KV workload and queries on spatial views.
    """

    #COLLECTORS = {'latency': True, 'query_latency': True}

    @with_stats
    def access(self):
        super(ViewTest, self).timer()

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.compact_bucket()

        self.hot_load()

        self.define_ddocs()
        self.build_index()

        self.workload = self.test_config.access_settings
        self.access_bg()
        self.access()


class SpatialMixin(object):
    # NOTE vmx 2015-05-26: Currently getting the get/set latency breaks things,
    # hence we only care for the query latency for now
    #COLLECTORS = {'latency': True, 'spatial_latency': True}
    COLLECTORS = {'spatial_latency': True}

    def __init__(self, cluster_spec, test_config, verbose, experiment=None):
        self.view_settings = test_config.spatial_settings
        super(SpatialMixin, self).__init__(
            cluster_spec, test_config, verbose, experiment)

    def build_index(self):
        super(SpatialMixin, self).build_index('spatial')


class SpatialQueryTest(SpatialMixin, ViewQueryTest):
    pass


class SpatialQueryThroughputTest(SpatialQueryTest):

    """
    The test adds a simple step to workflow: post-test calculation of average
    query throughput.
    """

    def run(self):
        super(SpatialQueryThroughputTest, self).run()
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                self.metric_helper.calc_avg_couch_views_ops()
            )


class SpatialQueryLatencyTest(SpatialQueryTest):

    """The basic test for latency measurements.

    The class itself only adds calculation and posting of query latency.
    """

    def run(self):
        super(SpatialQueryLatencyTest, self).run()

        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                *self.metric_helper.calc_query_latency(percentile=80)
            )
            if self.test_config.stats_settings.post_rss:
                self.reporter.post_to_sf(
                    *self.metric_helper.calc_max_beam_rss()
                )
