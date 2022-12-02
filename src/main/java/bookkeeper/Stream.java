package bookkeeper;

import util.Utility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.zookeeper.KeeperException;

public class Stream {

  final static String ZOOKEEPER_SERVER = "128.105.145.0:2181,128.105.144.249:2181,128.105.144.105:2181";
  static String streamName = "default";
  Utility utility = new Utility();
  final static byte[] PASSWD = "pass".getBytes();

  BookKeeper bookkeeper;

  public class WriterRunnable implements Runnable {

    private int bytes;
    private int numberOfRequests;
    private LedgerHandle lh;

    public WriterRunnable(int bytes, int numberOfRequests, LedgerHandle lh) {
      this.bytes = bytes;
      this.numberOfRequests = numberOfRequests;
      this.lh = lh;
    }

    class NoOPAddCallback implements AsyncCallback.AddCallback {

      public void addComplete(int arg0, LedgerHandle arg1, long arg2, Object arg3) {
        // TODO Auto-generated method stub

      }
    }

    public void run() {
      List<Long> entryIds = new ArrayList<Long>();
      List<Long> runningTimes = new ArrayList<Long>();
      List<Long> dataGenerationTimes = new ArrayList<Long>();

      long totalBytes = bytes * numberOfRequests;
      long initialStartTime = System.currentTimeMillis();
      for (int i = 0; i < numberOfRequests; i++) {
        long dataGenStartTime = System.currentTimeMillis();
        byte[] entry = utility.generateRandomString(bytes, dataGenStartTime);
        long dataGenStopTime = System.currentTimeMillis();

        long startTime = System.currentTimeMillis();
        lh.asyncAddEntry(entry, new NoOPAddCallback(), null);

        long stopTime = System.currentTimeMillis();
        runningTimes.add(stopTime - startTime);
        dataGenerationTimes.add(dataGenStopTime - dataGenStartTime);
      }
      long finalStopTime = System.currentTimeMillis();

    }
  }

  public class ReaderRunnable implements Runnable {

    private int bytes;
    private int numberOfRequests;
    private LedgerHandle lh;
    private int numEntriesPerBatch;

    public ReaderRunnable(int bytes, int numberOfRequests, LedgerHandle lh, int numEntriesPerBatch) {
      this.bytes = bytes;
      this.numberOfRequests = numberOfRequests;
      this.lh = lh;
      this.numEntriesPerBatch = numEntriesPerBatch;
    }

    public void run() {
      List<Long> batchSizes = new ArrayList<Long>();
      List<Long> batchLatencies = new ArrayList<Long>();

      long startEntryId = 0L;
      long nextEntryId = startEntryId;
      long startFetchTime = System.currentTimeMillis();
      while (nextEntryId < numberOfRequests) {
        long lac = lh.getLastAddConfirmed();

        if (nextEntryId > lac) {
          // no more entries are added
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

          lac = lh.getLastAddConfirmed();
          continue;
        }

        long endEntryId = lac;
        long startTime = System.currentTimeMillis();
        Enumeration<LedgerEntry> entries;
        try {
          entries = lh.readEntries(nextEntryId, endEntryId);
        } catch (InterruptedException e) {
          e.printStackTrace();
          return;
        } catch (BKException e) {
          e.printStackTrace();
          return;
        }
        batchSizes.add(endEntryId - nextEntryId + 1);
        byte[] entry = entries.nextElement().getEntry();
        long insertTime = utility.bytesToLong(Arrays.copyOfRange(entry, 0, 8));
        // System.out.println(endEntryId - nextEntryId + 1);
        // System.out.println(startTime);
        // System.out.println(insertTime);
        batchLatencies.add(startTime - insertTime);

        nextEntryId = endEntryId + 1;

      }
      long finalEndFetchTime = System.currentTimeMillis();

      try {
        utility.logInCSVFileRead(numberOfRequests,
            finalEndFetchTime - startFetchTime,
            batchSizes,
            batchLatencies,
            "consumer." + streamName + ".csv");
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

    }
  }

  public Stream() throws IOException, InterruptedException, KeeperException {
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

    Thread tWrite = new Thread(new WriterRunnable(bytes, numberOfRequests, lh));
    Thread tRead = new Thread(new ReaderRunnable(bytes, numberOfRequests, lh, numEntriesPerBatch));

    tWrite.start();
    tRead.start();

    tWrite.join();
    tRead.join();

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
    new Stream().start(numberOfRequests, bytes, numEntriesPerBatch);
  }

}
