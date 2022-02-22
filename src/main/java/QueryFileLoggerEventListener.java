import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryFailureInfo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

/**
 * @author zhaowg
 */
public class QueryFileLoggerEventListener
        implements EventListener {

    private final String tableName;

    private Connection connection;

    private static final List<String[]> columns = new ArrayList<>();

    static {
        columns.add(new String[]{"query_id", "String"});
        columns.add(new String[]{"query_state", "String"});
        columns.add(new String[]{"query_user", "String"});
        columns.add(new String[]{"query_source", "String"});
        columns.add(new String[]{"query_sql", "String"});
        columns.add(new String[]{"query_start", "DateTime"});
        columns.add(new String[]{"query_end", "DateTime"});
        columns.add(new String[]{"wall_time", "Int32"});
        columns.add(new String[]{"queue_time", "Int32"});
        columns.add(new String[]{"cpu_time", "Int32"});
        columns.add(new String[]{"peak_memory_bytes", "Int64"});
        columns.add(new String[]{"query_error_type", "Nullable(String)"});
        columns.add(new String[]{"query_error_code", "Nullable(String)"});
        columns.add(new String[]{"logdate", "Int32"});

    }

    /**
     * check table is exists or not
     *
     * @return True if exists, otherwise False
     */
    private boolean checkTable(Statement statement) {
        boolean isExists = false;
        try {
            if (statement.execute("select 1 from " + tableName)) {
                isExists = true;
            }
        } catch (SQLException ignore) {
            //ignore
        }
        return isExists;
    }

    private void createTable(Statement statement)
            throws SQLException {
        final StringJoiner stringJoiner = new StringJoiner(",");
        for (String[] item : columns) {
            stringJoiner.add(item[0] + " " + item[1]);
        }
        String sql = String.format(" CREATE TABLE %s  ( %s ) ENGINE = MergeTree() PARTITION BY logdate ORDER BY query_id SETTINGS index_granularity = 8192", tableName, stringJoiner);
        statement.execute(sql);
    }

    public QueryFileLoggerEventListener(Map<String, String> config) {
        String jdbcUrl = config.get("jdbc-url");
        tableName = config.getOrDefault("jdbc-table", "presto_query_log");
        String username = config.getOrDefault("jdbc-username", "default");
        String password = config.getOrDefault("jdbc-password", null);
        try {
            if (jdbcUrl.startsWith("jdbc://clickhouse")) {
                Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            }
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            Statement statement = connection.createStatement();
            if (!checkTable(statement)) {
                createTable(statement);
            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
        String datetimePattern = "yyyy-MM-dd HH:mm:ss";
        String querySQL = queryCompletedEvent.getMetadata().getQuery();
        // filter
        if (querySQL.startsWith("SELECT TABLE_CAT, TABLE_SCHEM")
                || querySQL.startsWith("START TRANSACTION")
                || querySQL.startsWith("EXECUTE statement")
                || querySQL.startsWith("DEALLOCATE PREPARE")
                || querySQL.startsWith("ROLLBACK")
        ) {
            return;
        }
        List<Object> insertVals = new ArrayList<>(16);
        // quyer id
        insertVals.add(queryCompletedEvent.getMetadata().getQueryId());
        // query state
        insertVals.add(queryCompletedEvent.getMetadata().getQueryState());
        // query user
        insertVals.add(queryCompletedEvent.getContext().getUser());
        // query source
        insertVals.add(queryCompletedEvent.getContext().getSource().orElse(""));
        // query sql
        insertVals.add(querySQL);
        // query started time
        ZonedDateTime startTime = queryCompletedEvent.getCreateTime().atZone(ZoneId.of("Asia/Chongqing"));

        insertVals.add(startTime.toLocalDateTime().format(DateTimeFormatter.ofPattern(datetimePattern)));
        // query end time
        ZonedDateTime endTime = queryCompletedEvent.getEndTime().atZone(ZoneId.of("Asia/Chongqing"));
        insertVals.add(endTime.toLocalDateTime().format(DateTimeFormatter.ofPattern(datetimePattern)));
        // wall times
        long wallTime = queryCompletedEvent.getStatistics().getWallTime().toSeconds();
        insertVals.add(wallTime);
        // queued times
        long queueTime = queryCompletedEvent.getStatistics().getQueuedTime().toSeconds();
        insertVals.add(queueTime);
        // CPU times
        long cpuTime = queryCompletedEvent.getStatistics().getCpuTime().toSeconds();
        insertVals.add(cpuTime);
        // peak memory bytes
        long peakMemoryBytes = queryCompletedEvent.getStatistics().getPeakUserMemoryBytes();
        insertVals.add(peakMemoryBytes);
        Optional<QueryFailureInfo> failureInfo = queryCompletedEvent.getFailureInfo();
        // query error type and query error code
        if (failureInfo.isPresent()) {
            insertVals.add(failureInfo.get().getFailureType().orElse(""));
            insertVals.add(String.valueOf(failureInfo.get().getErrorCode().getCode()));
        } else {
            insertVals.add(null);
            insertVals.add(null);
        }
        // logdate
        insertVals.add(Integer.parseInt(java.time.LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))));
        StringJoiner insertSQL = new StringJoiner(",");
        StringJoiner parameters = new StringJoiner(",");
        for (String[] item: columns) {
            insertSQL.add(item[0]);
            parameters.add("?");
        }
        String sql = "INSERT INTO " + tableName + " (" + insertSQL + ") VALUES (" + parameters + ")";
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setQueryTimeout(2);
            for (int i = 1; i <= insertVals.size(); i++) {
                preparedStatement.setObject(i, insertVals.get(i - 1));
            }
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(sql);
        }
    }
}
