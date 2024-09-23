
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
    mvn exec:java -Dexec.mainClass=com.yugabyte.app.App -Dexec.args="[--create-table] [--sessions <num>] [--threads <num>] [--queries <num>] [--datafile <path>] [--rows <num>]"
    ```
- Currently, it only performs reads on a table.