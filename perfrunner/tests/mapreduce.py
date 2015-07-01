from perfrunner.tests.view import ViewIndexTest


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
