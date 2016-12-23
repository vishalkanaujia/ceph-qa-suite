
"""
Test to exercise killpoints available during directory export code path
After hitting a killpoint, MDS crashes and cluster must find a standby MDS
to become active. Exported data must be available after mds replacement.

"""
import logging
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)


class TestKillPoints(CephFSTestCase):
    # Two active MDS and a standby
    MDSS_REQUIRED = 3

    init = False

    def setup_cluster(self):
        # Set Multi-MDS cluster
        self.fs.set_allow_multimds()
        self.fs.set_max_mds(2)

        self.fs.wait_for_daemons()

        num_active_mds = len(self.fs.get_active_names())
        if num_active_mds != 2:
            log.error("Incorrect number of MDS active: %d" %num_active_mds)
            return False

        # Get a clean mount
        self.mount_a.umount_wait()
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()

        # Create test data
        if self.init is False:
            size_mb = 8
            self.mount_a.run_shell(["mkdir", "abc"])
            self.mount_a.write_n_mb("abc/abc_file", size_mb)
            self.init = True

        return True

    def run_export_dir(self, v):
        # Wait till all MDS becomes active
        self.fs.wait_for_daemons()

        # Get all active ranks
        ranks = self.fs.get_all_mds_rank()

        original_active = self.fs.get_active_names()
        active = original_active

        original_standbys = self.fs.get_standby_daemons()
        standby = original_standbys

        # Sanity checks
        if len(standby) != 1:
            log.error("Standby daemon does not exist! Exiting")
            return False

        if len(ranks) != 2:
            log.error("Incorrect number of MDS ranks, exiting the test")
            return False

        rank_0_id = original_active[0]
        command = ["config", "set", "mds_kill_export_at", str(v)]
        result = self.fs.mds_asok(command, rank_0_id)
        assert(result["success"])

        # This should kill the MDS process
        command = ["export", "dir", "/abc", "1"]
        try:
            result = self.fs.mds_asok(command, rank_0_id)
        except Exception as e:
            log.error(e.__str__())

        # Wait until the monitor promotes replacement for dead MDS
        grace = int(self.fs.get_config("mds_beacon_grace", service_type="mon"))

        def promoted():
            active = self.fs.get_active_names()
            return active and active[0] in original_standbys

        log.info("Waiting for promotion of one of the original standbys {0}".format(
            original_standbys))
        self.wait_until_true(promoted, timeout=grace*4)

        # Verify data
        out = self.mount_a.ls()
        if "abc" not in out:
            log.error("Directory contents are corrupted")
            return False

        # Restart the killed daemon. After restart it would move
        # to standby state
        self.fs.mds_restart(rank_0_id)

        self.wait_until_true(
           lambda: rank_0_id in self.mds_cluster.get_standby_daemons(),
           timeout=60  # Approximately long enough for MDS to start and mon to notice
        )

        # Post-test sanity check
        ranks = self.fs.get_all_mds_rank()
        if len(ranks) != 2:
            return False

        active = self.fs.get_active_names()
        if len(active) != 2:
            return False

        standby = self.fs.get_standby_daemons()
        if len(standby) != 1:
            return False

        return True


    def test_killpoint_dir_export(self):
        self.init = False
        self.setup_cluster()
        success_count = 0
        killpoints_count = 9

        # Specify the kill point value for export dir
        for v in range(1, killpoints_count + 1):
            if self.run_export_dir(v):
                success_count = success_count + 1
            else:
                log.error("Error for killpoint %d" %killpoints_count)
        assert(success_count == killpoints_count)

        log.info("All %s killpoints scenarios passed")
