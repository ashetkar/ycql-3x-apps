
## Prerequisites

- `mvn` installed
- 'JDK 8' or later installed

## How to run

- Build the project
    ```shell
    mvn clean package
    ```
- Usage
    ```shell
    java -jar <jarfile> --create-table --sessions <num> --threads <num> --queries <num> --datafile <path>
    ```
