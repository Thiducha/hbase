package org.apache.hadoop.hbase.master;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestRecovery {

    HBaseRecoveryTestingUtility TEST_UTIL = new HBaseRecoveryTestingUtility();

    private static final Log LOG = LogFactory.getLog(TestRecovery.class);
    private final int nbTests = 6;
    private final int nbMoves = 5;
    private final int nbVal = 50;

    static {
        Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);
        Logger.getLogger(DFSClient.class).setLevel(Level.DEBUG);
        Logger.getLogger("org.apache.hadoop.hdfs.server.datanode").setLevel(Level.DEBUG);
    }

    @After
    public void after() throws Exception {
        TEST_UTIL.stopCleanCluster();
    }


    // OK 6 Tests 2min53s
    @Test
    public void testKillRS() throws Exception {
        for (int i = 0; i < nbTests; i++) {
            LOG.info("Test "+i);
            TEST_UTIL.startClusterSynchronous(3, 1);
            TEST_UTIL.createTable(100, 0);

            HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(1000);
            puts.checkPuts();

            TEST_UTIL.startNewRegionServer();
            TEST_UTIL.stopDirtyRegionServer(0);
            puts.checkPuts();

            TEST_UTIL.stopCleanCluster();
            LOG.info("End test " + i);
        }
    }

    // Ok 6 Tests 2min56s
    @Test
    public void testAbortRS() throws Exception {
        for (int i = 0; i < nbTests; i++) {
            LOG.info("Test "+i);
            TEST_UTIL.startClusterSynchronous(3, 1);
            TEST_UTIL.createTable(100, 0);

            HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(1000);
            puts.checkPuts();

            TEST_UTIL.startNewRegionServer();
            TEST_UTIL.abortRegionServer(0);
            puts.checkPuts();

            TEST_UTIL.stopCleanCluster();
            LOG.info("End test " + i);
        }
    }

    // OK 6 Tests 7min55s
    @Test
    public void testKillDN() throws Exception {
        for (int i = 0; i < nbTests; i++) {
            LOG.info("Test " + i);
            TEST_UTIL.startClusterSynchronous(3, 1);
            TEST_UTIL.createTable(100, 0);

            HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(1000);
            puts.checkPuts();

            TEST_UTIL.stopDirtyDataNode(0);
            TEST_UTIL.stopDirtyDataNode(1);

            puts.checkPuts();

            TEST_UTIL.stopCleanCluster();
            LOG.info("End test " + i);
        }
    }

    // OK 6 Tests 10min32s
    @Test
    public void testStopDN() throws Exception {
         for (int i = 0; i < nbTests; i++) {
            LOG.info("Test " + i);
            TEST_UTIL.startClusterSynchronous(3, 1);
            TEST_UTIL.createTable(100, 0);

            HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(1000);
            puts.checkPuts();

            TEST_UTIL.stopCleanDataNode(0);
            TEST_UTIL.stopCleanDataNode(1);

            puts.checkPuts();

            TEST_UTIL.stopCleanCluster();
            LOG.info("End test " + i);
        }
    }

    // Ok 6 Tests 3min32s
    @Test
    public void testFlushWithDeadDN() throws Exception {
        for (int i = 0; i < nbTests; i++) {
            LOG.info("Test " + i);
            TEST_UTIL.startClusterSynchronous(3, 1);
            TEST_UTIL.createTable(100, 0);

            HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(1000);
            puts.checkPuts();

            TEST_UTIL.startNewDatanode();
            TEST_UTIL.startNewDatanode();

            TEST_UTIL.stopCleanDataNode(3);
            TEST_UTIL.stopCleanDataNode(4);

            TEST_UTIL.flushSynchronous(0);

            puts.checkPuts();

            TEST_UTIL.stopCleanCluster();
            LOG.info("End test " + i);
        }
    }

    // Fail
    // First iteration ok but not the second
    // checkPuts fail
    @Test
    public void testKillRSWithBadDN() throws Exception {
        for (int i = 0; i < nbTests; i++) {
            LOG.info("Test " + i);
            TEST_UTIL.startClusterSynchronous(3, 3);
            TEST_UTIL.createTable(100, 0);

            // Search Region Servers without root and meta
            int[] RS = TEST_UTIL.getRSNoRootAndNoMeta(3);

            HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(1000);
            puts.checkPuts();

            // Move table in a Region Server without root and meta
            TEST_UTIL.moveTableTo(TEST_UTIL.getTestTableNameToString(), RS[0]);

            // start 2 datanodes
            TEST_UTIL.startNewDatanode();
            TEST_UTIL.startNewDatanode();

            // shutdown 2 Datanodes
            TEST_UTIL.stopDirtyDataNodeStopIPC(1);
            TEST_UTIL.stopDirtyDataNodeStopIPC(2);

            // Kill all Regions Servers without root and meta
            for(int j = 0; j < RS.length; j++) {
                TEST_UTIL.stopDirtyRegionServer(RS[j]);
            }

            puts.checkPuts();

            TEST_UTIL.stopCleanCluster();
            LOG.info("End test " + i);
        }
    }

    // Fail
    // 2 iterations OK. The third failed
    // checkPuts fail
    @Test
    public void testKillRSWithBadDN2() throws Exception {
        for (int i = 0; i < nbTests; i++) {
            LOG.info("Test " + i);
            TEST_UTIL.startClusterSynchronous(3, 3);
            TEST_UTIL.createTable(100, 0);

            // Search datanodes without root and meta
            int[] RS = TEST_UTIL.getRSNoRootAndNoMeta(3);

            HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(1000);
            puts.checkPuts();

            // Move table in a Region Server without root and meta
            TEST_UTIL.moveTableTo(TEST_UTIL.getTestTableNameToString(), RS[0]);

            TEST_UTIL.startNewDatanode();
            TEST_UTIL.startNewDatanode();

            TEST_UTIL.stopDirtyDataNode(1);
            TEST_UTIL.stopDirtyDataNode(2);


            for(int j = 0; j < RS.length; j++) {
                TEST_UTIL.stopDirtyRegionServer(RS[j]);
            }

            puts.checkPuts();

            TEST_UTIL.stopCleanCluster();
            LOG.info("End test " + i);
        }
    }

    // Fail
    //  org.apache.hadoop.hbase.client.RetriesExhaustedException: Failed after attempts=20
    @Test
    public void testKillDNandRS() throws Exception {
        for(int i = 0; i < nbTests; i++) {
            LOG.info("Test " + i);
            TEST_UTIL.startClusterSynchronous(3, 3);
            TEST_UTIL.createTable(100, 0);

            // Search datanodes without Root and Meta
            int[] RS = TEST_UTIL.getRSNoRootAndNoMeta(3);

            HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(1000);
            puts.checkPuts();

            // Move table to a Region Server without meta and root
            TEST_UTIL.moveTableTo(TEST_UTIL.getTestTableNameToString(), RS[0]);

            TEST_UTIL.startNewDatanode();
            TEST_UTIL.startNewDatanode();

            TEST_UTIL.stopCleanDataNode(0);
            TEST_UTIL.stopCleanDataNode(1);

            for(int j = 0; j < RS.length; j++) {
                TEST_UTIL.stopDirtyRegionServer(RS[j]);
            }

            puts.checkPuts();

            TEST_UTIL.stopCleanCluster();

            LOG.info("End Test " + i);
        }
    }
    // Fail
    // First iteration ok but not the second
    // checkPuts fail
    @Test
    public void testStopDNandRS() throws Exception {
        for(int i = 0; i < nbTests; i++) {
            LOG.info("Test " + i);
            TEST_UTIL.startClusterSynchronous(3, 3);
            TEST_UTIL.createTable(100, 0);

            // Search Regions Servers without Root and Meta
            int[] RS = TEST_UTIL.getRSNoRootAndNoMeta(3);

            HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(1000);
            puts.checkPuts();

            // Move table to a Region Server without Root and Meta
            TEST_UTIL.moveTableTo(TEST_UTIL.getTestTableNameToString(), RS[0]);

            TEST_UTIL.startNewDatanode();
            TEST_UTIL.startNewDatanode();

            TEST_UTIL.stopCleanDataNode(0);
            TEST_UTIL.stopCleanDataNode(1);

            for(int j = 0; j < RS.length; j++) {
                TEST_UTIL.stopDirtyRegionServer(RS[j]);
            }

            puts.checkPuts();

            TEST_UTIL.stopCleanCluster();
            LOG.info("End test " + i);
        }
    }

    // Fail
    // Flush fail
    // testPutAndFlush(org.apache.hadoop.hbase.master.TestRecovery): org.apache.hadoop.hbase.DroppedSnapshotException:
    // region: rb_-1360161948,4\xB4\xB4\xB4\xB4\xB4\xB4\xB4\xB4\xB4,1344595556533.75387eaa1105faac71e4e0e83f8608a4.
    @Test
    public void testPutAndFlush() throws Exception {
        LOG.info("Test");
        // start cluster with 6DN and 1RS
        TEST_UTIL.startClusterSynchronous(6, 1);

        TEST_UTIL.stopCleanDataNode(0);
        TEST_UTIL.stopCleanDataNode(1);

        TEST_UTIL.createTable(6, 0);

        TEST_UTIL.stopCleanDataNode(2);

        for (int i = 0; i < 100000; i++) {
            LOG.info("Start Put " + i);
            HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(1);
            puts.checkPuts();
            LOG.info("Put "+i+" Finished");

            LOG.info("Start Flush " + i);
            TEST_UTIL.getHBaseAdmin().flush(TEST_UTIL.getTestTableName());
            TEST_UTIL.flushSynchronous(0);
            LOG.info("Flush " + i + " finished");
        }

        TEST_UTIL.stopCleanCluster();
        LOG.info("End test");
    }

    // Ok 2min47
    @Test
    public void testMoveAndKillRS() throws Exception {
        for (int j = 0; j < nbTests; j++) {
            LOG.info("Test " + j);
            TEST_UTIL.startClusterSynchronous(3, 4);
            TEST_UTIL.createTable(10, 0);

            int[] numDest;

            // Search Regions Servers without root and meta
            numDest = TEST_UTIL.getRSNoRootAndNoMeta(4);

            HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(100);
            puts.checkPuts();

            for(int i = 0; i < nbMoves; i++) {
                TEST_UTIL.moveTableTo(TEST_UTIL.getTestTableNameToString(), numDest[0]);

                puts.checkPuts();

                TEST_UTIL.moveTableTo(TEST_UTIL.getTestTableNameToString(), numDest[1]);

                // Kill server with table
                TEST_UTIL.stopDirtyRegionServer(numDest[0]);

                puts.checkPuts();

                // Prepare next iteration
                TEST_UTIL.startNewRegionServer();

                numDest[0] = numDest[1];
                numDest[1] = i + 4;
            }

            TEST_UTIL.stopCleanCluster();
            LOG.info("End test " + j);
        }
    }

    // Ok 34min36s
    @Test
    public void testTimeToMove() throws Exception {
        for (int i = 0; i < nbVal; i++) {
            TEST_UTIL.startClusterSynchronous(3, 1);
            TEST_UTIL.createTable(4, 0);

            HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(100000);
            puts.checkPuts();

            TEST_UTIL.startNewRegionServer();

            long start = System.currentTimeMillis();
            TEST_UTIL.moveTableTo(TEST_UTIL.getTestTableNameToString(), 1);
            long stop = System.currentTimeMillis();
            long res = stop - start;
            LOG.info("Time to move region attempt " + i + " : " + res + " ms");

            puts.checkPuts();

            TEST_UTIL.stopCleanCluster();
        }
    }

    // Ok
    @Test
    public void testTimeHLogWrite() throws Exception {
        for (int i = 0; i < nbVal; i++) {
            TEST_UTIL.startClusterSynchronous(3, 3);
            TEST_UTIL.createTable(4, 0);

            // Create Region
            HRegionInfo hri = TEST_UTIL.createRegion();

            Path filename = TEST_UTIL.createHLog(hri, 0);

            TEST_UTIL.checkHLog(1, filename, hri);

            TEST_UTIL.stopCleanCluster();
        }
    }

    // Ok
    @Test
    public void testTimeHLogWriteWithKillDN() throws Exception {
        for (int i = 0; i < nbVal; i++) {
            TEST_UTIL.startClusterSynchronous(3, 3);
            TEST_UTIL.createTable(4, 0);

            // Create Region
            HRegionInfo hri = TEST_UTIL.createRegion();

            Path filename = TEST_UTIL.createHLog(hri, 1);

            TEST_UTIL.checkHLog(1, filename, hri);

            TEST_UTIL.stopCleanCluster();
        }
    }

    /* Scénario qui ne se produit pas en prod
    @Test
    public void testTimeKillFirstsDNFirstsRSandRead() throws Exception {
        int nbVal = 25;

        long[] val = new long[nbVal];
        // Create in hbase-server dir
        FileOutputStream fos = new FileOutputStream(new File("resTimeKillFirstDNandRead.log"));

        for (int i = 0; i < nbVal; i++) {
            // Start 5 DN 20 DN
            startCluster(5, 20);

            doPuts(100, 1);
            checkPuts(100, 1);

            RootAndMeta rm = new RootAndMeta();
            int[] RS = rm.getNoRootAndNoMeta(20);

            for(int j = 0; j < RS.length; j++) {
                killRegionServer(RS[j]);
            }

            shutdownDataNode(0);
            shutdownDataNode(1);

            long start = System.currentTimeMillis();
            doPuts(100, 1);
            long stop = System.currentTimeMillis();
            long res = stop - start;
            val[i] = res;
            fos.write(("Time to read attempt " + i + " : " + res + " ms\n").getBytes());
        }

        long moy = calcAverage(nbVal, val);

        fos.write(("Average time to move region : " + moy + " ms\n").getBytes());
        fos.close();
     }  */
}