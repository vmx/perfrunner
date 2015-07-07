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
