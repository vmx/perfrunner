from uuid import uuid4

from celery import Celery
from fabric.api import cd, run
from kombu.common import Broadcast
from logger import logger
from spring.wgen import WorkloadGen

from perfrunner.settings import BROKER_URL, REPO
from perfrunner.helpers.remote import RemoteHelper, all_hosts

CELERY_QUEUES = (Broadcast('broadcast_tasks'), )
CELERY_ROUTES = {
    'perfrunner.herlpers.worker.task_run_workload': {
        'queue': 'broadcast_tasks'
    }
}
celery = Celery('workers', backend='amqp', broker=BROKER_URL)


@celery.task
def task_run_workload(settings, target):
    wg = WorkloadGen(settings, target)
    wg.run()


class WorkerManager(RemoteHelper):

    def __init__(self, cluster_spec):
        super(WorkerManager, self).__init__(cluster_spec)

        self.hosts = cluster_spec.get_workers()
        if self.hosts:
            self.is_remote = True
            self.temp_dir = '/tmp/{0}'.format(uuid4().hex[:12])
            self._initialize_project()
            self._start()
        else:
            self.is_remote = False

    @all_hosts
    def _initialize_project(self):
        logger.info('Intializing remote worker environment')
        run('mkdir {0}'.format(self.temp_dir))
        with cd(self.temp_dir):
            run('git clone {0}'.format(REPO))
        with cd('{0}/perfrunner'.format(self.temp_dir)):
            run('virtualenv env')
            run('env/bin/pip install -r requirements.txt')

    @all_hosts
    def _start(self):
        logger.info('Starting remote Celery worker')
        with cd('{0}/perfrunner'.format(self.temp_dir)):
            run('dtach -n /tmp/perfrunner.sock '
                'env/bin/celery worker -A perfrunner.helpers.worker -c 1')

    def run_workload(self, settings, target):
        if self.is_remote:
            logger.info('Starting workload generator remotely')
            task_run_workload.apply_async(args=(settings, target)).wait()
        else:
            logger.info('Starting workload generator locally')
            task_run_workload.apply(args=(settings, target))

    @all_hosts
    def terminate(self):
        if self.is_remote:
            logger.info('Terminating remote Celery worker')
            run('killall -9 celery; exit 0')
            logger.info('Cleaning up remote worker environment')
            run('rm -fr {0}'.format(self.temp_dir))