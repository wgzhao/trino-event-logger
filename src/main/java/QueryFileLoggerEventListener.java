import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import io.prestosql.spi.eventlistener.QueryFailureInfo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.time.format.DateTimeFormatter.BASIC_ISO_DATE;

/**
 * @author zhaowg
 * @date 2020-11-13
 */
public class QueryFileLoggerEventListener
        implements EventListener
{
    private final String tableName;
    private Connection connection;

    public QueryFileLoggerEventListener(Map<String, String> config)
    {
        String jdbcUrl = config.get("jdbc-url") + "?timeout=5";
        tableName = config.get("jdbc-table");
        String username = config.getOrDefault("jdbc-username", "default");
        String password = config.getOrDefault("jdbc-password", null);
        try {
            connection = DriverManager.getConnection(jdbcUrl, username, password);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
	}

    @Override
	public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        String querySQL = queryCompletedEvent.getMetadata().getQuery();
        // filter
        if (querySQL.startsWith("SELECT TABLE_CAT, TABLE_SCHEM")
                || querySQL.startsWith("START TRANSACTION")
                || querySQL.startsWith("EXECUTE statement")
                || querySQL.startsWith("DEALLOCATE PREPARE")
                || querySQL.startsWith("ROLLBACK")
        )
        {
            return;
        }
        List<String> insertVals = new ArrayList<>(8);
        insertVals.add(queryCompletedEvent.getMetadata().getQueryId());
        insertVals.add(queryCompletedEvent.getMetadata().getQueryState());
        insertVals.add(queryCompletedEvent.getContext().getUser());
        insertVals.add(queryCompletedEvent.getContext().getSource().orElse(""));
        insertVals.add(querySQL);
        // query started time
        ZonedDateTime startTime = queryCompletedEvent.getCreateTime().atZone(ZoneId.of("Asia/Chongqing"));
        insertVals.add(startTime.toLocalDateTime().toString());
        // query end time
        ZonedDateTime endTime = queryCompletedEvent.getEndTime().atZone(ZoneId.of("Asia/Chongqing"));
        insertVals.add(endTime.toLocalDateTime().toString());
        Optional<QueryFailureInfo> failureInfo = queryCompletedEvent.getFailureInfo();
        if (failureInfo.isPresent()) {
            insertVals.add(failureInfo.get().getFailureType().orElse(""));
            insertVals.add(String.valueOf(failureInfo.get().getErrorCode().getCode()));
        } else {
            insertVals.add(null);
            insertVals.add(null);
        }
        insertVals.add(java.time.LocalDate.now().format(BASIC_ISO_DATE));

        String insertSQL = "insert into " + tableName
                + "(query_id, query_state, query_user, query_source, query_sql, "
                + "query_start, query_end, query_error_type, query_error_code, "
                + " logdate) values(?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement preparement = connection.prepareStatement(insertSQL)) {
            preparement.setQueryTimeout(2);
            for (int i = 1; i <= insertVals.size(); i++) {
                preparement.setString(i, insertVals.get(i - 1));
            }
            preparement.executeUpdate();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
