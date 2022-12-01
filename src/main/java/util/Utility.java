package util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class Utility {
    final int NUMBER_OF_ASCII_CHARS = 26 + 26 + 10;
    final String charSet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    public static Map<Integer, byte[]> map = new HashMap<Integer, byte[]>();

    // generate same value always -- reduces data generation time
    // Our focus is geared towards performance rather than correctness
    public byte[] generateRandomString(int length) {
        if (map.containsKey(length))
            return map.get(length);

        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int index = (int) (NUMBER_OF_ASCII_CHARS * Math.random());
            char ch = charSet.charAt(index);
            sb.append(ch);
        }
        map.put(length, sb.toString().getBytes());
        return map.get(length);

    }

    // public void writeLineToAFile(String filename, String line) throws IOException
    // {
    // FileWriter fileWriter = new FileWriter(filename);
    // fileWriter.append(line);
    // fileWriter.flush();
    // fileWriter.close();
    // }

    public void logInCSVFile(int numberOfRequests,
            long totalBytes,
            long totalTime,
            List<Long> entryIds,
            List<Long> runningTimes,
            List<Long> dataGenerationTimes,
            String filename) throws IOException {

        System.out.println("Now Logging into a CSV file");
        FileWriter csvWriter = new FileWriter(filename);

        String startRow = "id,entryId,latency,dataGenTime,totalBytes,totalTime\n";
        csvWriter.append(startRow);
        for (int i = 0; i < numberOfRequests; i++) {
            long entryId = entryIds.get(i);
            long latency = runningTimes.get(i);
            long datagenTime = dataGenerationTimes.get(i);
            String row = String.valueOf(i)
                    + ","
                    + String.valueOf(entryId)
                    + ","
                    + String.valueOf(latency)
                    + ","
                    + String.valueOf(datagenTime)
                    + ","
                    + String.valueOf(totalBytes)
                    + ","
                    + String.valueOf(totalTime);
            csvWriter.append(row);
            csvWriter.append("\n");
        }
        csvWriter.flush();
        csvWriter.close();
    }

    public void logInCSVFileRead(int numberOfRequests,
            long totalTime,
            List<Long> batchSizes,
            List<Long> batchLatencies,
            String filename) throws IOException {

        System.out.println("Now Logging into a CSV file");
        FileWriter csvWriter = new FileWriter(filename);

        String startRow = "id,latency,size,totalTime\n";
        csvWriter.append(startRow);
        for (int i = 0; i < batchSizes.size(); i++) {
            long size = batchSizes.get(i);
            long latency = batchLatencies.get(i);
            String row = String.valueOf(i)
                    + ","
                    + String.valueOf(latency)
                    + ","
                    + String.valueOf(size)
                    + ","
                    + String.valueOf(totalTime);
            csvWriter.append(row);
            csvWriter.append("\n");
        }
        csvWriter.flush();
        csvWriter.close();
    }

    // public void logInCSVFile(int numberOfRequests,
    // long totalBytes,
    // long totalTime,
    // List<Long> runningTimes,
    // List<Long> dataGenerationTimes,
    // String filename) throws IOException {

    // System.out.println("Now Logging into a CSV file");
    // FileWriter csvWriter = new FileWriter(filename);

    // String startRow = "id,latency,dataGenTime,totalBytes,totalTime\n";
    // csvWriter.append(startRow);
    // for (int i = 0; i < numberOfRequests; i++) {
    // long latency = runningTimes.get(i);
    // long datagenTime = dataGenerationTimes.get(i);
    // String row = String.valueOf(i)
    // + ","
    // + String.valueOf(latency)
    // + ","
    // + String.valueOf(datagenTime)
    // + ","
    // + String.valueOf(totalBytes)
    // + ","
    // + String.valueOf(totalTime);
    // csvWriter.append(row);
    // csvWriter.append("\n");
    // }
    // csvWriter.flush();
    // csvWriter.close();
    // }

    // public void writeLogLocationsInCSV(String filename,
    // List<Long> latency,
    // List<Long> entryIds,
    // List<Long> txSeq) throws Exception {
    // System.out.println("Now Logging into a CSV file");
    // FileWriter csvWriter = new FileWriter(filename);
    // String startRow = "id,latency,entryId,transactionSequenceId\n";
    // csvWriter.append(startRow);
    // for (int i = 0; i < latency.size(); i++) {
    // String row = String.valueOf(i)
    // + ","
    // + String.valueOf(latency.get(i))
    // + ","
    // + String.valueOf(entryIds.get(i))
    // + ","
    // + String.valueOf(txSeq.get(i));
    // csvWriter.append(row);
    // csvWriter.append("\n");
    // }
    // csvWriter.flush();
    // csvWriter.close();
    // }

    // public List<CorfuRecordId> readLogPositions(String filename) throws Exception
    // {
    // // String startRow = "id,latency,entryId,transactionSequenceId\n";
    // BufferedReader reader;
    // boolean firstLine = true;
    // List<CorfuRecordId> corfuRecordIds = new ArrayList<>();

    // reader = new BufferedReader(new FileReader(filename));
    // String line = reader.readLine();
    // while (line != null) {
    // if (!firstLine) {
    // String arr[] = line.split(",");
    // CorfuRecordId corfuRecordId = new CorfuRecordId(Long.parseLong(arr[3]),
    // Long.parseLong(arr[2]));
    // corfuRecordIds.add(corfuRecordId);
    // }
    // line = reader.readLine();
    // firstLine = false;
    // }
    // reader.close();

    // return corfuRecordIds;
    // }

}