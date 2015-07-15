from perfrunner.tests.view import ViewIndexTest, ViewQueryTest


class MapreduceMixin(object):
    def __init__(self, cluster_spec, test_config, verbose, experiment=None):
        self._view_settings = test_config.mapreduce_settings
        super(MapreduceMixin, self).__init__(
            cluster_spec, test_config, verbose, experiment)

    @property
    def view_settings(self):
        return self._view_settings

    @property
    def view_key(self):
        return "views"


class MapreduceIndexTest(MapreduceMixin, ViewIndexTest):
    pass


class MapreduceQueryTest(MapreduceMixin, ViewQueryTest):
    COLLECTORS = {'latency': True, 'mapreduce_latency': True}


class MapreduceQueryThroughputTest(MapreduceQueryTest):

    """
    The test adds a simple step to workflow: post-test calculation of average
    query throughput.
    """

    def run(self):
        super(MapreduceQueryThroughputTest, self).run()
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                self.metric_helper.calc_avg_couch_views_ops()
            )


class MapreduceQueryLatencyTest(MapreduceQueryTest):

    """The basic test for latency measurements.

    The class itself only adds calculation and posting of query latency.
    """

    def run(self):
        super(MapreduceQueryLatencyTest, self).run()

        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                *self.metric_helper.calc_query_latency(percentile=80)
            )
            if self.test_config.stats_settings.post_rss:
                self.reporter.post_to_sf(
                    *self.metric_helper.calc_max_beam_rss()
                )


class MapreduceIndexLatencyTest(MapreduceQueryTest):

    """
    Measurement of end-to-end latency which is defined as time it takes for a
    document to appear in view output after it is stored in KV.

    The test only adds calculation phase. See cbagent project for details.
    """

    # TODO vmx 2015-07-14: Rename `index_latency` to something less generic
    COLLECTORS = {'index_latency': True, 'mapreduce_latency': True}

    def run(self):
        super(MapreduceIndexLatencyTest, self).run()

        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                *self.metric_helper.calc_observe_latency(percentile=95)
            )
