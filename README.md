
## Prerequisites

- `mvn` installed
- 'JDK 8' or later installed

## How to run

- Build the project
    ```shell
    mvn clean compile
    ```
- Usage
    ```shell
    YBDB_PATH=<path/to/YugabyteDB/dir> mvn exec:java -Dexec.mainClass=com.yugabyte.app.App -Dexec.args="--restart-nodes [--nodes <num>] [--pause-time <seconds>]"
    # OR
    mvn exec:java -Dexec.mainClass=com.yugabyte.app.App -Dexec.args="--perform-reads [--create-table] [--sessions <num>] [--threads <num>] [--queries <num>] [--datafile <path>] [--rows <num>] ]
    ```

    - `--restart-nodes` - Continuously stop/start a random node.
        - `--nodes <num>` - Total number of nodes available in cluster to choose from. Default is 3.
        - `--pause-time` - Time to pause in between stopping/starting a node. Default is 15 seconds.

    - `--perform-reads` - Run SELECT operations as per given options.
        - `--create-table` - Create the SensorData table (currently, needs to be populated separately).
        - `--sessions` - Total number of sessions to create.
        - `--threads` - Total number of threads to create for each session. Default 1.
        - `--queries` - Total number of queries each thread will perform. Default is 1.
        - `--rows` - Total number of rows in the table available for querying so that the app can randomly do SELECTs in this range. Default is 10.
