# presto-event-logger
Presto EventLogger implementation that logs all the queries to clickhouse .

# How to use 

1. Get the code

    ```shell
    git clone https://gitlab.ds.cfzq.com/grp_ds/presto-event-logger && cd presto-event-logger
    ```
   
2. Compile and package
   
   ```shell
   mvn clean package assembly:assembly
   ```
   
3. Put the logger as part of presto

    ```shell
    mkdir <path-to-presto>/plugin/event-logger/
    cp target/presto-event-logger-*-jar-with-dependencies.jar <path-to-presto>/plugin/event-logger/
    ```
    
4. Specify the event-logger as an event listener ``<path-to-presto>/etc/event-listener.properties``

    ``
    event-listener.name=query-event-logger
    ``

5. Start the presto server

    ```shell
    <path-to-presto>/bin/launcher start
    ```
