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

public class ParallelSyncWriter {

    final static String ZOOKEEPER_SERVER = "128.105.145.0:2181,128.105.144.249:2181,128.105.144.105:2181";
    static String streamName = "default";
    Utility utility = new Utility();
    final static byte[] PASSWD = "pass".getBytes();
    final static String DICE_LOG = "/dice-log";

    BookKeeper bookkeeper;

    public class MyRunnable implements Runnable {

        private int bytes;
        private int numberOfRequests;
        private int id;
        private LedgerHandle lh;

        public MyRunnable(int id, int bytes, int numberOfRequests, LedgerHandle lh) {
            this.bytes = bytes;
            this.numberOfRequests = numberOfRequests;
            this.id = id;
            this.lh = lh;
        }

        public void run() {
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
                long entryId;
                try {
                    entryId = lh.addEntry(entry);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    return;
                } catch (BKException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    return;
                }
                long stopTime = System.currentTimeMillis();
                entryIds.add(entryId);
                runningTimes.add(stopTime - startTime);
                dataGenerationTimes.add(dataGenStopTime - dataGenStartTime);
            }
            long finalStopTime = System.currentTimeMillis();
            try {
                utility.logInCSVFile(numberOfRequests,
                        totalBytes,
                        finalStopTime - initialStartTime,
                        entryIds,
                        runningTimes,
                        dataGenerationTimes,
                        "producer." + streamName + "_" + id + ".csv");
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            }

        }
    }

    public ParallelSyncWriter() throws IOException, InterruptedException, KeeperException {
        bookkeeper = new BookKeeper(ZOOKEEPER_SERVER);
    }

    private void start(int numberOfRequests, int bytes, int numEntriesPerBatch) throws InterruptedException {
        LedgerHandle lh;
        try {
            lh = bookkeeper.createLedger(BookKeeper.DigestType.MAC, PASSWD);
        } catch (BKException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        }

        Thread t1 = new Thread(new MyRunnable(1, bytes, numberOfRequests, lh));
        Thread t2 = new Thread(new MyRunnable(2, bytes, numberOfRequests, lh));
        Thread t3 = new Thread(new MyRunnable(3, bytes, numberOfRequests, lh));
        Thread t4 = new Thread(new MyRunnable(4, bytes, numberOfRequests, lh));
        Thread t5 = new Thread(new MyRunnable(5, bytes, numberOfRequests, lh));
        Thread t6 = new Thread(new MyRunnable(6, bytes, numberOfRequests, lh));
        Thread t7 = new Thread(new MyRunnable(7, bytes, numberOfRequests, lh));
        Thread t8 = new Thread(new MyRunnable(8, bytes, numberOfRequests, lh));
        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();
        t6.start();
        t7.start();
        t8.start();

        t1.join();
        t2.join();
        t3.join();
        t4.join();
        t5.join();
        t6.join();
        t7.join();
        t8.join();

        try {
            lh.close();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        } catch (BKException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        }

        try {
            bookkeeper.close();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (BKException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        streamName = args[0];
        int numberOfRequests = Integer.parseInt(args[1]);
        int bytes = Integer.parseInt(args[2]);
        int numEntriesPerBatch = Integer.parseInt(args[3]);
        new ParallelSyncWriter().start(numberOfRequests, bytes, numEntriesPerBatch);
    }

}
