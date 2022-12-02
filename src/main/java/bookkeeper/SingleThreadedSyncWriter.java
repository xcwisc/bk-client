package bookkeeper;

import util.Utility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BKException;
import org.apache.zookeeper.KeeperException;

public class SingleThreadedSyncWriter {

    final static String ZOOKEEPER_SERVER = "128.105.145.0:2181,128.105.144.249:2181,128.105.144.105:2181";
    static String streamName = "default";
    Utility utility = new Utility();
    final static byte[] PASSWD = "pass".getBytes();
    final static String DICE_LOG = "/dice-log";

    BookKeeper bookkeeper;

    volatile boolean leader = false;

    public SingleThreadedSyncWriter() throws IOException, InterruptedException, KeeperException {
        bookkeeper = new BookKeeper(ZOOKEEPER_SERVER);
    }

    private void start(int numberOfRequests, int bytes, int numEntriesPerBatch)
            throws BKException, InterruptedException, IOException {
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

        lh = bookkeeper.openLedger(ledgerId, BookKeeper.DigestType.MAC, PASSWD);
        List<Long> batchSizes = new ArrayList<Long>();
        List<Long> batchLatencies = new ArrayList<Long>();

        long startEntryId = 0L;
        long nextEntryId = startEntryId;
        long startFetchTime = System.currentTimeMillis();
        while (nextEntryId < numberOfRequests) {

            long endEntryId = Math.min(numberOfRequests - 1, nextEntryId + numEntriesPerBatch - 1);
            long startTime = System.currentTimeMillis();
            // System.out.printf("nextEntryId: %s, endEntryId: %s", nextEntryId,
            // endEntryId);
            Enumeration<LedgerEntry> entries = lh.readEntries(nextEntryId, endEntryId);
            long stopTime = System.currentTimeMillis();

            batchSizes.add(endEntryId - nextEntryId + 1);
            batchLatencies.add(stopTime - startTime);

            nextEntryId = endEntryId + 1;
        }
        long finalEndFetchTime = System.currentTimeMillis();

        lh.close();
        bookkeeper.deleteLedger(ledgerId);

        utility.logInCSVFile(numberOfRequests,
                totalBytes,
                finalStopTime - initialStartTime,
                entryIds,
                runningTimes,
                dataGenerationTimes,
                "producer." + streamName + ".csv");

        utility.logInCSVFileRead(numberOfRequests,
                finalEndFetchTime - startFetchTime,
                batchSizes,
                batchLatencies,
                "consumer." + streamName + ".csv");

        bookkeeper.close();
    }

    public static void main(String[] args) throws Exception {
        streamName = args[0];
        int numberOfRequests = Integer.parseInt(args[1]);
        int bytes = Integer.parseInt(args[2]);
        int numEntriesPerBatch = Integer.parseInt(args[3]);
        new SingleThreadedSyncWriter().start(numberOfRequests, bytes, numEntriesPerBatch);
    }

}
