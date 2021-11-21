/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryFailureInfo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.logging.Logger;

public class QueryFileLoggerEventListener
        implements EventListener
{

    private final String[] header = {"query_id", "query_state", "query_user",
            "query_source", "query_sql", "query_start", "query_end", "wall_time",
            "queue_time", "cpu_time", "peak_memory_bytes", "query_error_type", "query_error_code"};

    private static final Logger logger = Logger.getLogger(QueryFileLoggerEventListener.class.getName());
    private FileWriter fileWriter;
    // the column separator symbol
    private final String separator;
    private final boolean hasHeader;
    private final long fileSize;
    private long currSize = 0;
    private final String filePath;
    private static long fileCount = 0;

    public QueryFileLoggerEventListener(Map<String, String> config)

    {
        // the directory events are written to
        String logDir = config.getOrDefault("log-dir", "/var/log/trino/");
        // the file name prefix
        String logFile = config.getOrDefault("log-file", "query.log");
        this.separator = config.getOrDefault("separator", "|");
        // write the column name or not
        this.hasHeader = Boolean.parseBoolean(config.getOrDefault("header", "true"));
        // the max size before rotate
        this.fileSize = Long.parseLong(config.getOrDefault("max-file-size", "104857600"));
        this.filePath = Path.of(logDir, logFile).toString();
        try {
            fileWriter = new FileWriter(this.filePath, true);
            if (hasHeader) {
                fileWriter.write(String.join(separator, header) + "\n");
                currSize += String.join(separator, header).getBytes(StandardCharsets.UTF_8).length;
            }
        }
        catch (IOException e) {
           logger.warning("Failed to create file writer" + e);
        }

    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
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
        StringJoiner sj = new StringJoiner(separator);
        // query id
        sj.add(queryCompletedEvent.getMetadata().getQueryId());
        // query state
        sj.add(queryCompletedEvent.getMetadata().getQueryState());
        // query user
        sj.add(queryCompletedEvent.getContext().getUser());
        // query source
        sj.add(queryCompletedEvent.getContext().getSource().orElse(""));
        // query sql
        sj.add(querySQL);
        // query started time
        ZonedDateTime startTime = queryCompletedEvent.getCreateTime().atZone(ZoneId.of("Asia/Chongqing"));

        sj.add(startTime.toLocalDateTime().format(DateTimeFormatter.ofPattern(datetimePattern)));
        // query end time
        ZonedDateTime endTime = queryCompletedEvent.getEndTime().atZone(ZoneId.of("Asia/Chongqing"));
        sj.add(endTime.toLocalDateTime().format(DateTimeFormatter.ofPattern(datetimePattern)));
        // wall times
        long wallTime = queryCompletedEvent.getStatistics().getWallTime().toSeconds();
        sj.add(String.valueOf(wallTime));
        // queued times
        long queueTime = queryCompletedEvent.getStatistics().getQueuedTime().toSeconds();
        sj.add(String.valueOf(queueTime));
        // CPU times
        long cpuTime = queryCompletedEvent.getStatistics().getCpuTime().toSeconds();
        sj.add(String.valueOf(cpuTime));
        // peak memory bytes
        long peakMemoryBytes = queryCompletedEvent.getStatistics().getPeakUserMemoryBytes();
        sj.add(String.valueOf(peakMemoryBytes));
        Optional<QueryFailureInfo> failureInfo = queryCompletedEvent.getFailureInfo();
        // query error type and query error code
        if (failureInfo.isPresent()) {
            sj.add(failureInfo.get().getFailureType().orElse(""));
            sj.add(String.valueOf(failureInfo.get().getErrorCode().getCode()));
        }
        else {
            sj.add("");
            sj.add("");
        }

        try {
            final byte[] bytes = sj.toString().getBytes(StandardCharsets.UTF_8);
            fileWriter.write(sj + "\n");
            fileWriter.flush();
            currSize += bytes.length;
            if (currSize >= fileSize) {
                // rotate
                fileWriter.flush();
                fileWriter.close();
                // rename current file to new filename
                File oldFile = new File(filePath);
                File newFile = new File(filePath + "-" + fileCount);
                ++fileCount;
                if (! oldFile.renameTo(newFile)) {
                    logger.info("Failed to rename file " + oldFile.getAbsolutePath() + " to " + newFile.getAbsolutePath());
                }

                // reopen log file
                fileWriter = new FileWriter(filePath, true);
                if (hasHeader) {
                    fileWriter.write(String.join(separator, header) + "\n");
                    fileWriter.flush();
                }
                // clean size
                currSize = 0;
            }
        }
        catch (IOException e) {
            logger.warning("Failed to write to file " + filePath + ": " + e);
        }
    }
}
