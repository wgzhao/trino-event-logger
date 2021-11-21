# trino-event-logger

[Trino](https://trino.io) EventLogger implementation that logs all the queries to clickhouse .

# How to use 

1. Get the code

    ```shell
    git clone https://github.com/wgzhao/trino-event-logger
    cd trino-event-logger
    ```
   
2. Compile and package
   
   ```shell
   mvn clean package assembly:assembly
   ```
   
3. Put the logger as part of presto

    ```shell
    mkdir <path-to-trino>/plugin/event-logger/
    cp target/trino-event-logger-*-jar-with-dependencies.jar <path-to-trino>/plugin/event-logger/
    ```
    
4. Specify the event-logger as an event listener ``<path-to-trino>/etc/event-listener.properties``

    ```ini
    event-listener.name=query-event-logger
    log-dir=/var/log/trino
    log-file=query.log
    separator=,
    max-file-size=1000000
    ```

5. Start the presto server

    ```shell
    <path-to-trino>/bin/launcher start
    ```
