# Copyright (c) 2018 Tappest Sp. z o.o.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import collections
import errno
import json
import os
import re

from os_brick.remotefs import remotefs
from oslo_concurrency import processutils as putils
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import imageutils
from oslo_utils import units
import six

from cinder import exception
from cinder.i18n import _
from cinder.image import image_utils
from cinder import interface
from cinder import utils
from cinder.volume import configuration
from cinder.volume.drivers import remotefs as remotefs_drv

LOG = logging.getLogger(__name__)

moosefs_opts = [
    cfg.StrOpt('moosefs_shares_config',
               default='/etc/cinder/moosefs_shares',
               help='File with the list of available moosefs shares.'),
    cfg.BoolOpt('moosefs_sparsed_volumes',
                default=True,
                help=('Create volumes as sparsed files which take no space '
                      'rather than regular files when using raw format, '
                      'in which case volume creation takes lot of time.')),
    cfg.FloatOpt('moosefs_used_ratio',
                 default=0.95,
                 help=('Percent of ACTUAL usage of the underlying volume '
                       'before no new volumes can be allocated to the volume '
                       'destination.')),
    cfg.StrOpt('moosefs_mount_point_base',
               default='$state_path/mnt',
               help=('Base dir containing mount points for '
                     'moosefs shares.')),
    cfg.ListOpt('moosefs_mount_options',
                help=('Mount options passed to the moosefs client. '
                      'See section of the pstorage-mount man page '
                      'for details.')),
    cfg.StrOpt('moosefs_default_volume_format',
               default='raw',
               help=('Default format that will be used when creating volumes '
                     'if no volume format is specified.')),
]

CONF = cfg.CONF
CONF.register_opts(moosefs_opts, group=configuration.SHARED_CONF_GROUP)

PLOOP_BASE_DELTA_NAME = 'root.hds'
DISK_FORMAT_RAW = 'raw'
DISK_FORMAT_QCOW2 = 'qcow2'

driver_volume_type = 'moosefs'
driver_prefix = 'moosefs'
volume_backend_name = 'Generic_moosefs'

@interface.volumedriver
class MoosefsDriver(remotefs_drv.RemoteFSSnapDriver):
    """Cinder driver for MooseFS Storage.

    """

    VERSION = '1.0'
    CI_WIKI_NAME = "MooseFS_CI"

    SHARE_FORMAT_REGEX = r'(?:(\S+):\/)?([a-zA-Z0-9_-]+)(?::(\S+))?'

    def __init__(self, execute=putils.execute, *args, **kwargs):
        self.driver_volume_type = 'moosefs'
        self.driver_prefix = 'moosefs'
        self.volume_backend_name = 'moosefs'
        self._remotefsclient = None
        super(MoosefsDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(moosefs_opts)
        self._execute_as_root = False
        root_helper = utils.get_root_helper()
        # base bound to instance is used in RemoteFsConnector.
        self.base = self.configuration.moosefs_mount_point_base
        opts = self.configuration.moosefs_mount_options

        self._remotefsclient = remotefs.MooseFSRemoteFsClient(
            'moosefs', root_helper, execute=execute,
            moosefs_mount_point_base=self.base,
            moosefs_mount_options=opts)

    def _update_volume_stats(self):
        super(MoosefsDriver, self)._update_volume_stats()
        self._stats['vendor_name'] = 'MooseFS'

    def _init_vendor_properties(self):
        namespace = 'moosefs'
        properties = {}

        self._set_property(
            properties,
            "%s:volume_format" % namespace,
            "Volume format",
            _("Specifies volume format."),
            "string",
            enum=["qcow2", "raw"],
            default=self.configuration.moosefs_default_volume_format)

        return properties, namespace

    def do_setup(self, context):
        """Any initialization the volume driver does while starting."""
        super(MoosefsDriver, self).do_setup(context)

        config = self.configuration.moosefs_shares_config
        if not os.path.exists(config):
            msg = (_("Moosefs config file at %(config)s doesn't exist.") %
                   {'config': config})
            LOG.error(msg)
            raise exception.MoosefsException(msg)

        if not os.path.isabs(self.base):
            msg = _("Invalid mount point base: %s.") % self.base
            LOG.error(msg)
            raise exception.MoosefsException(msg)

        used_ratio = self.configuration.moosefs_used_ratio
        if not ((used_ratio > 0) and (used_ratio <= 1)):
            msg = _("Moosefs config 'mfsstorage_used_ratio' invalid. "
                    "Must be > 0 and <= 1.0: %s.") % used_ratio
            LOG.error(msg)
            raise exception.MoosefsException(msg)

        self.shares = {}

        # Check if mfsmount is installed on this system;
        # note that we don't need to be root to see if the package
        # is installed.
        package = 'mfsmount'
        try:
            self._execute(package, check_exit_code=False,
                          run_as_root=False)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                msg = _('%s is not installed.') % package
                raise exception.MoosefsException(msg)
            else:
                raise

        self.configuration.nas_secure_file_operations = 'true'
        self.configuration.nas_secure_file_permissions = 'true'

    def _find_share(self, volume):
        """Choose MooseFS share among available ones for given volume size.

        For instances with more than one share that meets the criteria, the
        first suitable share will be selected.
        :param volume: the volume to be created.
        """

        if not self._mounted_shares:
            raise exception.MoosefsNoSharesMounted()

        for share in self._mounted_shares:
            LOG.debug('Checking %s share.', share)
            if self._is_share_eligible(share, volume.size):
                break
        else:
            raise exception.MoosefsNoSuitableShareFound(
                volume_size=volume.size)

        LOG.debug('Selected %s share.', share)

        return share
        #raise NotImplementedError()

    def _ensure_share_mounted(self, moosefs_share):
        mnt_flags = []
        if self.shares.get(moosefs_share) is not None:
            mnt_flags = self.shares[moosefs_share].split()

        try:

            mounts = self._remotefsclient._read_mounts()
            LOG.debug('Mounts ', mounts)
            self._remotefsclient.mount(moosefs_share, mnt_flags)
            return
        except Exception as e:

            LOG.error('Mount failure for %(share)s  ',
                      {'share': moosefs_share})
            raise exception.MoosefsException(six.text_type(e))

            time.sleep(1)

    def _is_share_eligible(self, moosefs_share, volume_size_in_gib):
        """Verifies MooseFS share is eligible to host volume with given size.
        :param moosefs_share: moosefs share
        :param volume_size_in_gib: int size in GB
        """
        used_ratio = self.configuration.moosefs_used_ratio
        LOG.info("volume_size_in_gib  : %d", volume_size_in_gib)
        LOG.debug(units.Gi)

        volume_size = volume_size_in_gib * units.Gi

        total_size, available, allocated = self._get_capacity_info(moosefs_share)

        if (allocated + volume_size) // total_size > used_ratio:
            LOG.debug('_is_share_eligible: %s is above '
                      'moosefsstorage_used_ratio.', moosefs_share)
            return False

        return True

    def choose_volume_format(self, volume):
        volume_format = None
        volume_type = volume.volume_type

        # Retrieve volume format from volume metadata
        if 'volume_format' in volume.metadata:
            volume_format = volume.metadata['volume_format']

        # If volume format wasn't found in metadata, use
        # volume type extra specs
        if not volume_format and volume_type:
            extra_specs = volume_type.extra_specs or {}
            if 'moosefs:volume_format' in extra_specs:
                volume_format = extra_specs['moosefs:volume_format']

        # If volume format is still undefined, return default
        # volume format from backend configuration
        return (volume_format or
                self.configuration.moosefs_default_volume_format)

    def get_volume_format(self, volume):
        active_file = self.get_active_image_from_info(volume)
        active_file_path = os.path.join(self._local_volume_dir(volume),
                                        active_file)
        img_info = self._qemu_img_info(active_file_path, volume.name)
        return image_utils.from_qemu_img_disk_format(img_info.file_format)

    @remotefs_drv.locked_volume_id_operation
    def extend_volume(self, volume, size_gb):
        LOG.info('Extending volume %s.', volume.id)
        volume_format = self.get_volume_format(volume)
        self._extend_volume(volume, size_gb)

    def _extend_volume(self, volume, size_gb):
        volume_path = self.local_path(volume)

        self._check_extend_volume_support(volume, size_gb)
        LOG.info('Resizing file to %sG...', size_gb)

        self._do_extend_volume(volume_path, size_gb)

    def _do_extend_volume(self, volume_path, size_gb):
        image_utils.resize_image(volume_path, size_gb)

        if not self._is_file_size_equal(volume_path, size_gb):
            raise exception.ExtendVolumeError(
                reason='Resizing image file failed.')

    def _check_extend_volume_support(self, volume, size_gb):
        volume_path = self.local_path(volume)
        active_file = self.get_active_image_from_info(volume)
        active_file_path = os.path.join(self._local_volume_dir(volume),
                                        active_file)

        if active_file_path != volume_path:
            msg = _('Extend volume is only supported for this '
                    'driver when no snapshots exist.')
            raise exception.InvalidVolume(msg)

        extend_by = int(size_gb) - volume['size']
        if not self._is_share_eligible(volume['provider_location'],
                                       extend_by):
            raise exception.ExtendVolumeError(reason='Insufficient space to '
                                              'extend volume %s to %sG.'
                                              % (volume['id'], size_gb))

    def _is_file_size_equal(self, path, size):
        """Checks if file size at path is equal to size."""
        data = image_utils.qemu_img_info(path)
        virt_size = data.virtual_size / units.Gi
        return virt_size == size

    def _copy_volume_from_snapshot(self, snapshot, volume, volume_size):
        """Copy data from snapshot to destination volume.
        This is done with a qemu-img convert to raw/qcow2 from the snapshot
        qcow2.
        """

        LOG.debug("_copy_volume_from_snapshot: snapshot: %(snap)s, "
                  "volume: %(vol)s, volume_size: %(size)s.",
                  {'snap': snapshot['id'],
                   'vol': volume['id'],
                   'size': volume_size,
                   })

        info_path = self._local_path_volume_info(snapshot['volume'])
        snap_info = self._read_info_file(info_path)
        vol_dir = self._local_volume_dir(snapshot['volume'])
        out_format = self.choose_volume_format(volume)
        qemu_out_format = image_utils.fixup_disk_format(out_format)
        volume_format = self.get_volume_format(snapshot.volume)
        volume_path = self.local_path(volume)

        forward_file = snap_info[snapshot['id']]
        forward_path = os.path.join(vol_dir, forward_file)

        if volume_format in (DISK_FORMAT_QCOW2, DISK_FORMAT_RAW):
            forward_file = snap_info[snapshot.id]
            forward_path = os.path.join(vol_dir, forward_file)

            # Find the file which backs this file, which represents the point
            # when this snapshot was created.
            img_info = self._qemu_img_info(forward_path,
                                           snapshot.volume.name)
            path_to_snap_img = os.path.join(vol_dir, img_info.backing_file)

            LOG.debug("_copy_volume_from_snapshot: will copy "
                      "from snapshot at %s.", path_to_snap_img)

            image_utils.convert_image(path_to_snap_img,
                                      volume_path,
                                      qemu_out_format)
        else:
            msg = _("Unsupported volume format %s") % volume_format
            raise exception.InvalidVolume(msg)

        self._extend_volume(volume, volume_size, out_format)
        # Query qemu-img info to cache the output
        img_info = self._qemu_img_info(volume_path, volume.name)

    @remotefs_drv.locked_volume_id_operation
    def delete_volume(self, volume):
        """Deletes a logical volume."""
        if not volume['provider_location']:
            msg = (_('Volume %s does not have provider_location '
                     'specified, skipping.') % volume['name'])
            LOG.error(msg)
            raise exception.MoosefsException(msg)

        self._ensure_share_mounted(volume['provider_location'])
        volume_dir = self._local_volume_dir(volume)
        mounted_path = os.path.join(volume_dir,
                                    self.get_active_image_from_info(volume))
        if os.path.exists(mounted_path):
            self._delete(mounted_path)
        else:
            LOG.info(_LI("Skipping deletion of volume %s "
                         "as it does not exist."), mounted_path)

        info_path = self._local_path_volume_info(volume)
        self._delete(info_path)

    @remotefs_drv.locked_volume_id_operation
    def initialize_connection(self, volume, connector):
        """Allow connection to connector and return connection info.

        :param volume: volume reference
        :param connector: connector reference
        """
        # Find active image
        active_file = self.get_active_image_from_info(volume)

        data = {'export': volume.provider_location,
                'format': self.get_volume_format(volume),
                'name': active_file,
                }

        return {
            'driver_volume_type': self.driver_volume_type,
            'data': data,
            'mount_point_base': self._get_mount_point_base(),
        }

    def _delete_snapshot(self, snapshot):
        info_path = self._local_path_volume_info(snapshot.volume)
        snap_info = self._read_info_file(info_path, empty_if_missing=True)
        if snapshot.id not in snap_info:
            LOG.warning("Snapshot %s doesn't exist in snap_info",
                        snapshot.id)
            return

        snap_file = os.path.join(self._local_volume_dir(snapshot.volume),
                                 snap_info[snapshot.id])
        active_file = os.path.join(self._local_volume_dir(snapshot.volume),
                                   snap_info['active'])
        higher_file = self._get_higher_image_path(snapshot)
        if higher_file:
            higher_file = os.path.join(self._local_volume_dir(snapshot.volume),
                                       higher_file)
        elif active_file != snap_file:
            msg = (_("Expected higher file exists for snapshot %s") %
                   snapshot.id)
            raise exception.MoosefsException(msg)

        img_info = self._qemu_img_info(snap_file, snapshot.volume.name)
        base_file = os.path.join(self._local_volume_dir(snapshot.volume),
                                 img_info.backing_file)

        super(MoosefsDriver, self)._delete_snapshot(snapshot)