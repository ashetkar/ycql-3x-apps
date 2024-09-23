package com.yugabyte.app;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.yugabyte.driver.core.policies.PartitionAwarePolicy;

/**
 * Hello world!
 */
public class App {
  private static int numOfThreads = 1;
    private static int numOfSessions = 1;
    private static Session[] sessions;
    private static String dataFile;
    private static boolean createTable = false;
    private static int queriesPerThreadPerSession = 1;

    public static void main(String[] args) {
        readWorkloadTest(args);
    }

    // For circuit breaker testing
    private static void readWorkloadTest(String[] args) {
        System.out.println("Read Workload Test");
        if (args == null || args.length == 0) {
            System.out.println("Usage: java -jar <jarfile> --create-table --sessions <num> --threads <num> --queries <num> --datafile <path>");
            return;
        }
        for (int i = 0; i < args.length; i++) {
            System.out.println("Arg #" + i + ": " + args[i]);
            switch (args[i]) {
                case "--queries":
                    try {
                        queriesPerThreadPerSession = Integer.parseInt(args[++i]);
                        if (queriesPerThreadPerSession < 1) {
                            queriesPerThreadPerSession = 1;
                        }
                        System.out.println("Queries per thread per session: " + queriesPerThreadPerSession);
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid number of queries: " + args[i] + ", setting to 1");
                        queriesPerThreadPerSession = 1;
                    }
                    break;
                case "--create-table":
                    createTable = true;
                    System.out.println("createTable = true");
                    break;
                case "--threads":
                    try {
                        numOfThreads = Integer.parseInt(args[++i]);
                        if (numOfThreads < 1) {
                            numOfThreads = 1;
                        }
                        System.out.println("Number of threads: " + numOfThreads);
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid number of threads: " + args[i] + ", setting to 1");
                        numOfThreads = 1;
                    }
                    break;
                case "--sessions":
                    try {
                        numOfSessions = Integer.parseInt(args[++i]);
                        if (numOfSessions < 1) {
                            numOfSessions = 1;
                        }
                        System.out.println("Number of sessions: " + numOfSessions);
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid number of sessions: " + args[i] + ", setting to 1");
                        numOfSessions = 1;
                    }
                    break;
                case "--datafile":
                    dataFile = args[i + 1];
                    System.out.println("Data file: " + dataFile);
                    break;
                default:
                    System.out.println("Unknown workload type: " + args[i]);
            }
        }

        PartitionAwarePolicy pap = new PartitionAwarePolicy();
      Cluster cluster = Cluster.builder()
              .addContactPoint("127.0.0.1:9042")
              .withLoadBalancingPolicy(pap)
              .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
//              .withPoolingOptions(new PoolingOptions()
//                      .setConnectionsPerHost())
              .withQueryOptions(new QueryOptions()
                      .setConsistencyLevel(ConsistencyLevel.QUORUM)
                      .setDefaultIdempotence(true)
                      .setSerialConsistencyLevel(ConsistencyLevel.QUORUM)
                      .setRefreshNodeIntervalMillis(10000)
                      .setRefreshNodeListIntervalMillis(10000)
                      .setRefreshSchemaIntervalMillis(10000))
              .build();

        for (int i = 0; i < numOfSessions; i++) {
            System.out.println("Initializing session #" + i);
            sessions[i] = cluster.connect();
        }

        if (createTable) {
            System.out.println("Creating table");
            createAndPopulateTable();
        }

        Thread[] threads = new Thread[numOfThreads];
        for (int i = 0; i < numOfThreads; i++) {
            final int fi = i;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    System.out.println("Thread " + Thread.currentThread().getId() + " started");
                    while (true) {
                        try {
                            readData(fi);
                        } catch (Exception e) {
                            System.out.println("Exception in thread #" + fi + ": " + e.getMessage());
                        }
                    }
                }
            });
            threads[i].start();
        }
    }

    private static void readData(int threadId) {
        for (int i = 0; i < numOfSessions; i++) {
            for (int j = 0; j < numOfThreads; j++) {
                for (int k = 0; k < queriesPerThreadPerSession; k++) {
                    ResultSet rs = sessions[i].execute("SELECT * FROM example.SensorData WHERE customer_name = 'customer1' AND device_id = 1");
                    for (Row row : rs) {
                        System.out.println("Thread #" + i + " - " + row.toString());
                    }
                }
            }
        }
    }

    private static void createAndPopulateTable() {
        sessions[0].execute("CREATE KEYSPACE example");
        sessions[0].execute("CREATE TABLE SensorData (customer_name text, device_id int, ts timestamp," +
                " sensor_data map<text, double>, PRIMARY KEY((customer_name, device_id), ts))");
        sessions[0].execute("COPY example.SensorData FROM '" + dataFile + "'");
    }
}
