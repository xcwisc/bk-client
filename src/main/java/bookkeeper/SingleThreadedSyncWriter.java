package bookkeeper;

import com.google.common.primitives.Ints;
import util.Utility;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BKException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

public class SingleThreadedSyncWriter {

    final static String ZOOKEEPER_SERVER = "128.105.144.95:2181,128.105.144.105:2181,128.105.144.249:2181";
    static String streamName = "default";
    Utility utility = new Utility();
    final static byte[] PASSWD = "pass".getBytes();
    final static String DICE_LOG = "/dice-log";

    BookKeeper bookkeeper;

    volatile boolean leader = false;

    public SingleThreadedSyncWriter() throws IOException, InterruptedException, KeeperException {
        bookkeeper = new BookKeeper(ZOOKEEPER_SERVER);
    }

    private void start(int numberOfRequests, int bytes) throws BKException, InterruptedException {
        LedgerHandle lh = bookkeeper.createLedger(BookKeeper.DigestType.MAC, PASSWD);
        long ledgerId = lh.getId();

        List<Long> entryIds = new ArrayList<Long>();
        List<Long> runningTimes = new ArrayList<Long>();
        List<Long> dataGenerationTimes = new ArrayList<Long>();

        long totalBytes = bytes * numberOfRequests;
        long initialStartTime = System.currentTimeMillis();
        for (int i = 0; i < numberOfRequests; i++) {
            long dataGenStartTime = System.currentTimeMillis();
            byte[] entry = utility.generateRandomString(bytes);
            long dataGenStopTime = System.currentTimeMillis();

            long startTime = System.currentTimeMillis();
            long entryId = lh.addEntry(entry);
            long stopTime = System.currentTimeMillis();
            entryIds.add(entryId);
            runningTimes.add(stopTime - startTime);
            dataGenerationTimes.add(dataGenStopTime - dataGenStartTime);
        }
        long finalStopTime = System.currentTimeMillis();
        lh.close();
        bookkeeper.deleteLedger(ledgerId);
        try {
            utility.logInCSVFile(numberOfRequests,
                    totalBytes,
                    finalStopTime - initialStartTime,
                    entryIds,
                    runningTimes,
                    dataGenerationTimes,
                    streamName + ".csv");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        streamName = args[0];
        int numberOfRequests = Integer.parseInt(args[1]);
        int bytes = Integer.parseInt(args[2]);
        new SingleThreadedSyncWriter().start(numberOfRequests, bytes);
    }

}
