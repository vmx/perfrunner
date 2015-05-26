from collections import namedtuple
from optparse import OptionParser

import requests
from logger import logger
from requests.exceptions import ConnectionError

from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import ClusterSpec

Build = namedtuple('Build',
                   ['arch', 'pkg', 'version', 'release', 'build', 'toy'])


class CouchbaseInstaller(object):

    CBFS = 'http://cbfs-ext.hq.couchbase.com/builds/'
    LATEST_BUILDS = 'http://latestbuilds.hq.couchbase.com/'
    SHERLOCK_BUILDS = ''

    def __init__(self, cluster_spec, options):
        self.remote = RemoteHelper(cluster_spec, None, options.verbose)
        self.cluster_spec = cluster_spec

        arch = self.remote.detect_arch()
        pkg = self.remote.detect_pkg()
        release, build = options.version.split('-')
        self.SHERLOCK_BUILDS = 'http://latestbuilds.hq.couchbase.com/couchbase-server/sherlock/{}/'.format(build)

        self.build = Build(arch, pkg, options.version, release, build,
                           options.toy)
        logger.info('Target build info: {}'.format(self.build))

    def get_expected_filenames(self):
        patterns = ()  # Sentinel
        if self.build.toy:
            patterns = (
                'couchbase-server-community_toy-{toy}-{arch}_{version}-toy.{pkg}',
                'couchbase-server-community_toy-{toy}-{version}-toy_{arch}.{pkg}',
                'couchbase-server-community_cent58-2.5.2-toy-{toy}-{arch}_{version}-toy.{pkg}',
                'couchbase-server-community_cent58-3.0.0-toy-{toy}-{arch}_{version}-toy.{pkg}',
                'couchbase-server-community_cent64-3.0.0-toy-{toy}-{arch}_{version}-toy.{pkg}',
                'couchbase-server-community_cent64-3.0.1-toy-{toy}-{arch}_{version}-toy.{pkg}',
                'couchbase-server-community_cent58-master-toy-{toy}-{arch}_{version}-toy.{pkg}',
                'couchbase-server-community_cent54-master-toy-{toy}-{arch}_{version}-toy.{pkg}',
            )
        elif self.build.pkg == 'rpm':
            patterns = (
                'couchbase-server-enterprise_centos6_{arch}_{version}-rel.{pkg}',
                'couchbase-server-enterprise-{version}-centos6.{arch}.{pkg}',
                'couchbase-server-enterprise_{arch}_{version}-rel.{pkg}',
            )
        elif self.build.pkg == 'deb':
            patterns = (
                'couchbase-server-enterprise_ubuntu_1204_{arch}_{version}-rel.{pkg}',
                'couchbase-server-enterprise_{version}-ubuntu12.04_amd64.{pkg}',
                'couchbase-server-enterprise_{arch}_{version}-rel.{pkg}',
            )
        elif self.build.pkg == 'exe':
            patterns = (
                'couchbase-server-enterprise_{arch}_{version}-rel.setup.{pkg}',
                'couchbase_server-enterprise-windows-amd64-{version}.{pkg}',
                'couchbase_server/{release}/{build}/couchbase_server-enterprise-windows-amd64-{version}.exe',
            )

        for pattern in patterns:
            yield pattern.format(**self.build._asdict())

    def find_package(self):
        for filename in self.get_expected_filenames():
            for base in (self.LATEST_BUILDS, self.SHERLOCK_BUILDS, self.CBFS):
                url = '{}{}'.format(base, filename)
                try:
                    status_code = requests.head(url).status_code
                except ConnectionError:
                    continue
                else:
                    if status_code == 200:
                        logger.info('Found "{}"'.format(url))
                        return filename, url
        logger.interrupt('Target build not found')

    def kill_processes(self):
        self.remote.kill_processes()

    def uninstall_package(self):
        self.remote.uninstall_couchbase(self.build.pkg)

    def clean_data(self):
        self.remote.clean_data()

    def install_package(self):
        filename, url = self.find_package()
        self.remote.install_couchbase(self.build.pkg, url, filename,
                                      self.build.release)

    def install(self):
        self.kill_processes()
        self.uninstall_package()
        self.clean_data()
        self.install_package()


def main():
    usage = '%prog -c cluster -v version [-t toy]'

    parser = OptionParser(usage)

    parser.add_option('-c', dest='cluster_spec_fname',
                      help='path to cluster specification file',
                      metavar='cluster.spec')
    parser.add_option('-v', dest='version',
                      help='build version', metavar='2.0.0-1976')
    parser.add_option('-t', dest='toy',
                      help='optional toy build ID', metavar='couchstore')
    parser.add_option('--verbose', dest='verbose', action='store_true',
                      help='enable verbose logging')

    options, args = parser.parse_args()

    if not options.cluster_spec_fname or not options.version:
        parser.error('Missing mandatory parameter')

    cluster_spec = ClusterSpec()
    cluster_spec.parse(options.cluster_spec_fname, args)

    installer = CouchbaseInstaller(cluster_spec, options)
    installer.install()

if __name__ == '__main__':
    main()
