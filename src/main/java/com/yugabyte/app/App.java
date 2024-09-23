package com.yugabyte.app;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.yugabyte.driver.core.policies.PartitionAwarePolicy;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static int numOfRows = 10;
  private static String path;
  private static int nodes = 3;
  private static int pauseSeconds = 15;

  private static final Logger LOG = LoggerFactory.getLogger(App.class);

  public static void main(String[] args) throws InterruptedException {

    if (args == null || args.length < 1) {
      System.out.println("Usage:\n  mvn exec:java -Dexec.mainClass=com.yugabyte.app.App -Dexec.args=\"--restart-nodes [--nodes <num>] [--pause-time <seconds>]\"\n"
        + "  # OR\n"
        + "  mvn exec:java -Dexec.mainClass=com.yugabyte.app.App -Dexec.args=\"--read-workload [--create-table] [--sessions <num>] [--threads <num>] [--queries <num>] [--datafile <path>] [--rows <num>]\"");
      return;
    }
    if (args[0].equals("--restart-nodes")) {
      path = System.getenv("YBDB_PATH");
      if (path == null || path.trim().isEmpty()) {
        throw new IllegalStateException("YBDB_PATH not defined.");
      }
      stopStartNodes(args);
    } else {
      readWorkloadTest(args);
    }
  }

  private static void stopStartNodes(String[] args) throws InterruptedException {
    processNodeRestartArgs(args);
    System.out.println("Restarting nodes continuously ...");
    Random rand = new Random();
    while (true) {
      int nodeIndex = rand.nextInt(nodes) + 1;
      // stop a node
      executeCmd(path + "/bin/yb-ctl stop_node " + nodeIndex, "Stop node " + nodeIndex, 10);
      // pause a bit
      Thread.sleep(pauseSeconds * 1000);
      // start the same node
      executeCmd(path + "/bin/yb-ctl start_node " + nodeIndex, "Start node " + nodeIndex, 10);
      boolean isAlive = false;
      do {
        try {
          executeCmd(path + "/bin/ycqlsh 127.0.0." + nodeIndex + " -e \"SELECT * FROM SYSTEM.LOCAL;\"", "Host 127.0.0." + nodeIndex + " is ready", 5);
          isAlive = true;
        } catch (RuntimeException e) {
          Thread.sleep(2000);
        }
      } while (!isAlive);
      Thread.sleep(pauseSeconds * 1000);
    }
  }

  private static void processNodeRestartArgs(String[] args) {
    for (int i = 1; i < args.length; i++) {
      System.out.println("Arg #" + i + ": " + args[i]);
      switch (args[i]) {
        case "--nodes":
          try {
            nodes = Integer.parseInt(args[++i]);
            if (nodes < 1 || nodes > 120) {
              throw new IllegalArgumentException("Invalid number of nodes: " + nodes);
            }
            System.out.println("Nodes in the cluster: " + nodes);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number of nodes: " + nodes);
          }
          break;
        case "--pause-time":
          try {
            pauseSeconds = Integer.parseInt(args[++i]);
            if (pauseSeconds < 1) {
              throw new IllegalArgumentException("Invalid pause interval: " + pauseSeconds);
            }
            System.out.println("Pause interval: " + pauseSeconds);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid pause interval: " + pauseSeconds);
          }
          break;
      }
    }
  }

  // For circuit breaker testing
  private static void readWorkloadTest(String[] args) {
    System.out.println("Usage:\n  mvn exec:java -Dexec.mainClass=com.yugabyte.app.App -Dexec.args=\"--read-workload [--create-table] [--sessions <num>] [--threads <num>] [--queries <num>] [--datafile <path>] [--rows <num>]\"");
    System.out.println("Read Workload Test");

    processArgs(args);
    sessions = new Session[numOfSessions];
    PartitionAwarePolicy pap = new PartitionAwarePolicy();
    Cluster cluster = Cluster.builder()
            .addContactPoint("127.0.0.1")
            .withLoadBalancingPolicy(pap)
            .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
            .withQueryOptions(new QueryOptions()
                    .setConsistencyLevel(ConsistencyLevel.QUORUM)
                    .setDefaultIdempotence(true)
                    .setSerialConsistencyLevel(ConsistencyLevel.QUORUM)
                    .setRefreshNodeIntervalMillis(10000)
                    .setRefreshNodeListIntervalMillis(10000)
                    .setRefreshSchemaIntervalMillis(10000))
            .build();

    try {
      for (int i = 0; i < numOfSessions; i++) {
        System.out.println("Initializing session #" + i);
        sessions[i] = cluster.connect();
      }

      if (createTable) {
        System.out.println("Creating table");
        createAndPopulateTable();
      }

      readData();
    } finally {
      for (int i = 0; i < numOfSessions; i++) {
        if (sessions[i] != null) sessions[i].close();
      }
      cluster.close();
    }
  }

  private static void processArgs(String[] args) {
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
          dataFile = args[++i];
          System.out.println("Data file: " + dataFile);
          break;
        case "--rows":
          try {
            numOfRows = Integer.parseInt(args[++i]);
            if (numOfRows < 1) {
              throw new IllegalArgumentException("Invalid number of rows: " + args[i]);
            }
            System.out.println("Number of rows to be queried: " + numOfRows);
          } catch (NumberFormatException e) {
            System.out.println("Invalid number of rows: " + args[i] + ", setting to 10");
            numOfRows = 10;
          }
          break;
        default:
          System.out.println("Unknown workload type: " + args[i]);
      }
    }
  }

  private static void readData() {
    String query = "SELECT customer_name, device_id, ts, sensor_data FROM example.SensorData WHERE customer_name = ? AND device_id = ?";
    Random rand = new Random();
    Thread[][] threads = new Thread[numOfSessions][numOfThreads];
    for (int i = 0; i < numOfSessions; i++) {
      final int fi = i;
      for (int j = 0; j < numOfThreads; j++) {
        final int fj = j;
        threads[fi][j] = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              PreparedStatement prepared = sessions[fi].prepare(query);
              for (int k = 0; k < queriesPerThreadPerSession; k++) {
                int r = rand.nextInt(numOfRows);
                ResultSet rs = sessions[fi].execute(prepared.bind("customer" + r, r));
                for (Row row : rs) {
                  String name = row.getString("customer_name");
                  int device = row.getInt("device_id");
                  if (name != null && device > 0) {}
                }
                if (k % 50 == 0 && k > 0) {
                  System.out.println("Thread " + fj + " ran " + k + " queries so far");
                }
              }
              System.out.println("Completed queries for thread " + fj);
            } catch (Throwable e) {
              System.out.println("Exception while selects for thread " + fj + ": " + e.getMessage());
              e.printStackTrace();
              System.exit(1);
            }
          }
        });
        threads[fi][j].start();
      }
      System.out.println("Started all threads for session " + i);
    }

    for (int i = 0; i < numOfSessions; i++) {
      for (int j = 0; j < numOfThreads; j++) {
        try {
          threads[i][j].join();
        } catch (InterruptedException e) {
          System.out.println("Thread [" + i + "," + j + "] interrupted: " + e);
        }
      }
    }
    System.out.println("Closed all sessions");
  }

  private static void createAndPopulateTable() {
    sessions[0].execute("CREATE KEYSPACE IF NOT EXISTS example");
    sessions[0].execute("CREATE TABLE example.SensorData (customer_name text, device_id int, ts timestamp," +
            " sensor_data map<text, double>, PRIMARY KEY((customer_name, device_id), ts))");
    // sessions[0].execute("COPY example.SensorData FROM '" + dataFile + "'"); 
    // com.datastax.driver.core.exceptions.SyntaxError: Feature Not Supported
    // COPY example.SensorData FROM '/home/centos/work/build-apps/100krows.csv'
    // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    // Populate the table via ycqlsh
  }

    public static void executeCmd(String cmd, String msg, int timeout) {
      try {
        System.out.println("Command to be executed: " + cmd);
        ProcessBuilder builder = new ProcessBuilder();
        builder.command("sh", "-c", cmd);
        builder.redirectErrorStream(true);
        Process process = builder.start();
        process.waitFor(timeout, TimeUnit.SECONDS);
        int exitCode = process.exitValue();
        if (exitCode != 0) {
          String result = new BufferedReader(new InputStreamReader(process.getInputStream()))
                  .lines().collect(Collectors.joining("\n"));
          throw new RuntimeException(msg + ": FAILED\n" + result);
        }
        System.out.println(msg + ": SUCCEEDED!");
      } catch (Exception e) {
        // if (rethrow && e instanceof RuntimeException) {
        //   throw (RuntimeException) e;
        // } else {
          System.out.println("Exception " + e);
        // }
      }
    }

}
