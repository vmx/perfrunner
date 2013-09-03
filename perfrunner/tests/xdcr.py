from threading import Thread

from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.settings import TargetSettings
from perfrunner.tests import target_hash, TargetIterator
from perfrunner.tests import PerfTest


class XdcrTest(PerfTest):

    def __init__(self, *args, **kwargs):
        super(XdcrTest, self).__init__(*args, **kwargs)
        self.settings = self.test_config.get_xdcr_settings()

    def _start_replication(self, m1, m2):
        name = target_hash(m1, m2)
        self.rest.add_remote_cluster(m1, m2, name)

        for bucket in self.test_config.get_buckets():
            params = {
                'replicationType': 'continuous',
                'toBucket': bucket,
                'fromBucket': bucket,
                'toCluster': name
            }
            if self.settings.replication_mode:
                params['type'] = self.settings.replication_mode
            self.rest.start_replication(m1, params)

    def init_xdcr(self):
        m1, m2 = self.cluster_spec.get_masters().values()

        if self.settings.replication_type == 'unidir':
            self._start_replication(m1, m2)
        if self.settings.replication_type == 'bidir':
            self._start_replication(m1, m2)
            self._start_replication(m2, m1)

        for target in self.target_iterator:
            self.monitor.monitor_xdcr_replication(target)

    @with_stats(xdcr_lag=True)
    def access(self):
        super(XdcrTest, self).access()

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.init_xdcr()
        self.wait_for_persistence()

        self.hot_load()
        self.wait_for_persistence()

        self.compact_bucket()

        self.access()
        self.reporter.post_to_sf(
            *self.metric_helper.calc_max_replication_changes_left()
        )
        self.reporter.post_to_sf(
            *self.metric_helper.calc_max_xdcr_lag()
        )
        self.reporter.post_to_sf(
            *self.metric_helper.calc_avg_xdcr_ops()
        )
        self.reporter.post_to_sf(
            *self.metric_helper.calc_avg_set_meta_ops()
        )


class SymmetricXdcrTest(XdcrTest):

    def __init__(self, *args, **kwargs):
        super(SymmetricXdcrTest, self).__init__(*args, **kwargs)
        self.target_iterator = TargetIterator(self.cluster_spec,
                                              self.test_config,
                                              prefix="symmetric")


class TimeDrivenXdcrTest(SymmetricXdcrTest):

    def access(self):
        super(XdcrTest, self).timer()

    def access_bg(self):
        access_settings = self.test_config.get_access_settings()
        logger.info('Running access phase in background: {0}'.format(
            access_settings))
        Thread(
            target=self.worker_manager.run_workload,
            args=(access_settings, self.target_iterator, self.shutdown_event)
        ).start()

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.init_xdcr()
        self.wait_for_persistence()

        self.access_bg()
        self.access()


class SrcTargetIterator(TargetIterator):

    def __iter__(self):
        username, password = self.cluster_spec.get_rest_credentials()
        src_master = self.cluster_spec.get_masters().values()[0]
        for bucket in self.test_config.get_buckets():
            prefix = target_hash(src_master, bucket)
            yield TargetSettings(src_master, bucket, username, password, prefix)


class XdcrInitTest(XdcrTest):

    def load(self):
        load_settings = self.test_config.get_load_settings()
        logger.info('Running load phase: {0}'.format(load_settings))
        src_target_iterator = SrcTargetIterator(self.cluster_spec,
                                                self.test_config)
        self.worker_manager.run_workload(load_settings, src_target_iterator)

    @with_stats()
    def init_xdcr(self):
        super(XdcrInitTest, self).init_xdcr()

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.reporter.start()
        self.init_xdcr()
        time_elapsed = self.reporter.finish('Initial replication')
        self.reporter.post_to_sf(
            self.metric_helper.calc_avg_replication_rate(time_elapsed)
        )


class XdcrTuningTest(XdcrInitTest):

    def run(self):
        super(XdcrTuningTest, self).run()
        for cluster, value in self.metric_helper.calc_cpu_utilization().items():
            self.reporter.post_to_sf(
                value=value,
                metric='{0}_avg_cpu_utilization_rate'.format(cluster)
            )
