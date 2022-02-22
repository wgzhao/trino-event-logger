# trino-event-logger
Trino EventLogger implementation that logs all the queries to clickhouse .

# How to use 

1. Get the code

    ```shell
    git clone https://github.com/wgzhao/trino-event-logger && cd trino-event-logger
    ```
   
2. Compile and package
   
   ```shell
   mvn clean package assembly:assembly
   ```
   
3. Put the logger as part of trino

    ```shell
    mkdir <path-to-trino>/plugin/event-logger/
    cp target/trino-event-logger-*-jar-with-dependencies.jar <path-to-trino>/plugin/event-logger/
    ```
    
4. Specify the event-logger as an event listener ``<path-to-trino>/etc/event-listener.properties``

    ``
    event-listener.name=query-event-logger
    jdbc-url=jdbc:clickhouse://127.0.0.1:8123/default
    jdbc-table=trino_query_log
    jdbc-username=default
    jdbc-password=password
    ``

5. Start the Trino server

    ```shell
    <path-to-trino>/bin/launcher start
    ```
