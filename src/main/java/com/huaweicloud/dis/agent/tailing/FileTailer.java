package com.huaweicloud.dis.agent.tailing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.huaweicloud.dis.agent.AgentContext;
import com.huaweicloud.dis.agent.IHeartbeatProvider;
import com.huaweicloud.dis.agent.metrics.Metrics;
import com.huaweicloud.dis.agent.tailing.checkpoints.FileCheckpointStore;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Component responsible for tailing a single file, parsing into records and passing buffers of records downstream to
 * the destination. The tailer is configured using a {@link FileFlow} and supports tailing logs within a single
 * directory.
 */
// TODO: Refactor into two classes: FileTailer and FileTailerService, similar to AsyncPublisher and
// AsyncPublisherService
public class FileTailer<R extends IRecord> extends AbstractExecutionThreadService implements IHeartbeatProvider
{
    private static final int NO_TIMEOUT = -1;
    
    private static final int MAX_SPIN_WAIT_TIME_MILLIS = 1000;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(FileTailer.class);
    
    @Getter
    private final AgentContext agentContext;
    
    @Getter
    private final FileFlow<R> flow;
    
    private final String serviceName;
    
    protected final SourceFileTracker fileTracker;
    
    protected final IParser<R> parser;
    
    @VisibleForTesting
    @Getter
    private final FileCheckpointStore checkpoints;
    
    protected final AsyncPublisherService<R> publisher;
    
    protected long lastStatusReportTime = 0;
    
    private R pendingRecord = null;
    
    private boolean isNewFile = false;
    
    protected final long minTimeBetweenFilePollsMillis;
    
    protected final long maxTimeBetweenFileTrackerRefreshMillis;
    
    private AbstractScheduledService metricsEmitter;
    
    private boolean isInitialized = false;
    
    private final AtomicLong recordsTruncated = new AtomicLong();
    
    private boolean isExit = false;
    
    public FileTailer(AgentContext agentContext, FileFlow<R> flow, AsyncPublisherService<R> publisher,
        IParser<R> parser, FileCheckpointStore checkpoints)
        throws IOException
    {
        super();
        this.agentContext = agentContext;
        this.flow = flow;
        this.serviceName = super.serviceName() + "[" + this.flow.getId() + "]";
        this.checkpoints = checkpoints;
        this.publisher = publisher;
        this.parser = parser;
        this.fileTracker = new SourceFileTracker(this.agentContext, this.flow, this.checkpoints);
        this.minTimeBetweenFilePollsMillis = flow.minTimeBetweenFilePollsMillis();
        this.maxTimeBetweenFileTrackerRefreshMillis = flow.maxTimeBetweenFileTrackerRefreshMillis();
        this.metricsEmitter = new AbstractScheduledService()
        {
            @Override
            protected void runOneIteration()
                throws Exception
            {
                FileTailer.this.emitStatus();
            }
            
            @Override
            protected Scheduler scheduler()
            {
                return Scheduler.newFixedRateSchedule(FileTailer.this.agentContext.logStatusReportingPeriodSeconds(),
                    FileTailer.this.agentContext.logStatusReportingPeriodSeconds(),
                    TimeUnit.SECONDS);
            }
            
            @Override
            protected String serviceName()
            {
                return FileTailer.this.serviceName() + ".MetricsEmitter";
            }
            
            @Override
            protected void shutDown()
                throws Exception
            {
                LOGGER.debug("{}: shutting down...", serviceName());
                // Emit status one last time before shutdown
                FileTailer.this.emitStatus();
                super.shutDown();
            }
        };
        
        if (flow instanceof DISFileFlow)
        {
            fileTailerExecutor = new DISFileTailerExecutor();
        }
        else if (flow instanceof SmallFileFlow || flow instanceof OBSFileFlow)
        {
            fileTailerExecutor = new SmallFileTailerExecutor();
        }
        else
        {
            throw new IllegalArgumentException("Illegal FileFlow " + flow);
        }
    }
    
    @Override
    public void run()
    {
        do
        {
            if (0 == runOnce() && !isNewFile)
            {
                // Sleep only if the previous run did not process any records
                if (isNotExit() && minTimeBetweenFilePollsMillis > 0)
                {
                    try
                    {
                        Thread.sleep(minTimeBetweenFilePollsMillis);
                    }
                    catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        LOGGER.trace("{}: Thread interrupted", e);
                    }
                }
            }
        } while (isNotExit());
    }
    
    protected int runOnce()
    {
        return fileTailerExecutor.processFile();
    }
    
    public synchronized void initializeFormHistory()
        throws IOException
    {
        if (isInitialized)
            return;
        if (!fileTracker.initFormHistory())
        {
            // 没有合适的历史文件，重新开始
            fileTracker.initFromCurrentFiles();
        }
        parser.continueParsingWithFile(fileTracker.getCurrentOpenFile());
        isInitialized = true;
    }
    
    @Override
    protected synchronized void startUp()
        throws Exception
    {
        LOGGER.debug("{}: Starting up...", serviceName());
        super.startUp();
        // initialize();
        initializeFormHistory();
        metricsEmitter.startAsync();
        publisher.startPublisher();
    }
    
    @Override
    protected void triggerShutdown()
    {
        LOGGER.debug("{}: Shutdown triggered...", serviceName());
        isExit = true;
        super.triggerShutdown();
        publisher.stopPublisher();
        metricsEmitter.stopAsync();
    }
    
    @Override
    protected String serviceName()
    {
        return serviceName;
    }
    
    protected synchronized int processRecordsInCurrentFile()
        throws IOException
    {
        isNewFile = false;
        // See if there's a pending record from the previous run, and start
        // with it, otherwise, read a new record from the parser.
        R record = pendingRecord == null ? parser.readRecord() : pendingRecord;
        pendingRecord = null;
        int processed = 0;
        while (record != null)
        {
            long length = record.length();
            if (length > flow.getMaxRecordSizeBytes())
            {
                record.truncate();
                recordsTruncated.incrementAndGet();
                LOGGER.warn(
                    "{}: Truncated a record(size {}) in {}, because it exceeded the the configured max record size: {}",
                    serviceName(),
                    length,
                    parser.getCurrentFile(),
                    flow.getMaxRecordSizeBytes());
            }
            // Process a slice of records, and then check if we've been asked to stop
            if (isNotExit() && publisher.publishRecord(record))
            {
                ++processed;
                // Read the next record
                record = parser.readRecord();
            }
            else
            {
                // Publisher is exerting back-pressure. Return immediately
                // to stop further processing and give time for publisher to
                // catch up.
                LOGGER.debug("{}: record publisher exerting backpressure. Backing off a bit.", serviceName());
                pendingRecord = record;
                return processed;
            }
        }
        return processed;
    }
    
    protected synchronized long bytesBehind()
    {
        try
        {
            long result = parser.bufferedBytesRemaining();
            TrackedFile currentFile = parser.getCurrentFile();
            if (currentFile != null && currentFile.getChannel() != null && currentFile.getChannel().isOpen())
            {
                result += currentFile.getChannel().size() - currentFile.getChannel().position();
            }
            for (TrackedFile f : fileTracker.getPendingFiles())
            {
                if (currentFile != null && !currentFile.getId().getId().equals(f.getId().getId()))
                {
                    result += (f.getSize() - f.getLastOffset());
                }
            }
            return result;
        }
        catch (IOException e)
        {
            LOGGER.error("{}: Failed when calculating bytes behind.", serviceName(), e);
            return 0;
        }
    }
    
    protected int filesBehind()
    {
        return fileTracker.getPendingFiles().size();
    }
    
    public String getId()
    {
        return flow.getId();
    }
    
    @Override
    public String toString()
    {
        return serviceName();
    }
    
    @Override
    public Object heartbeat(AgentContext agent)
    {
        return publisher.heartbeat(agent);
    }
    
    private void emitStatus()
    {
        try
        {
            Map<String, Object> metrics = getMetrics();
            if (flow.logEmitInternalMetrics())
            {
                try
                {
                    ObjectMapper mapper = new ObjectMapper();
                    LOGGER.info("{}: File Tailer Status: {}", serviceName(), mapper.writeValueAsString(metrics));
                }
                catch (JsonProcessingException e)
                {
                    LOGGER.error("{}: Failed when emitting file tailer status metrics.", serviceName(), e);
                }
            }
            
            long bytesBehind = Metrics.getMetric(metrics, Metrics.FILE_TAILER_BYTES_BEHIND_METRIC, 0L);
            int filesBehind = Metrics.getMetric(metrics, Metrics.FILE_TAILER_FILES_BEHIND_METRIC, 0);
            
            String metricsLog = flow.getMetricLog(metrics);
            
            if (fileTracker.getCurrentOpenFile() != null)
            {
                if (LOGGER.isDebugEnabled())
                {
                    metricsLog += "  [" + fileTracker.getCurrentOpenFile() + "].";
                }
                else
                {
                    metricsLog += " [TrackedFile(id=" + fileTracker.getCurrentOpenFile().getId() + ", name="
                        + fileTracker.getCurrentOpenFile().getPath().getFileName().toString() + ")].";
                }
            }
            
            LOGGER.info(metricsLog);
            String msg = String.format("%s: Tailer is %02f MB (%d bytes) behind.",
                serviceName(),
                bytesBehind / 1024 / 1024.0,
                bytesBehind);
            if (filesBehind > 0)
            {
                msg += String.format(" There are %d file(s) newer than current file(s) being tailed.", filesBehind);
            }
            if (bytesBehind >= Metrics.BYTES_BEHIND_WARN_LEVEL)
            {
                LOGGER.warn(msg);
            }
            else if (bytesBehind >= Metrics.BYTES_BEHIND_INFO_LEVEL || agentContext.logEmitInternalMetrics())
            {
                LOGGER.info(msg);
            }
            else if (bytesBehind > 0)
            {
                LOGGER.debug(msg);
            }
        }
        catch (Exception e)
        {
            LOGGER.error("{}: Failed while emitting tailer status.", serviceName(), e);
        }
    }
    
    public Map<String, Object> getMetrics()
    {
        Map<String, Object> metrics = publisher.getMetrics();
        metrics.putAll(parser.getMetrics());
        metrics.put("FileTailer.FilesBehind", filesBehind());
        metrics.put("FileTailer.BytesBehind", bytesBehind());
        metrics.put("FileTailer.RecordsTruncated", recordsTruncated);
        return metrics;
    }
    
    private boolean isNotExit()
    {
        return isRunning() && !isExit;
    }
    
    interface FileTailerExecutor
    {
        int processFile();
    }
    
    FileTailerExecutor fileTailerExecutor;
    
    public class SmallFileTailerExecutor implements FileTailerExecutor
    {
        public synchronized int processFile()
        {
            int processed = 0;
            try
            {
                TrackedFileList pendingFiles = TrackedFileList.emptyList();
                long elapsedSinceLastRefresh = System.currentTimeMillis() - fileTracker.getLastRefreshTimestamp();
                if (elapsedSinceLastRefresh >= maxTimeBetweenFileTrackerRefreshMillis
                    || fileTracker.mustRefreshSnapshot())
                {
                    pendingFiles = fileTracker.refreshSmallFileTrackFileList();
                }
                
                int flag = pendingFiles.size() - 1;
                if (flag >= 0)
                {
                    // 按从旧到新的顺序解析文件
                    while (flag > -1 && isNotExit())
                    {
                        TrackedFile trackedFile = pendingFiles.get(flag);
                        parser.startParsingFile(trackedFile);
                        R record = pendingRecord == null ? parser.readRecord() : pendingRecord;
                        parser.stopParsing("Finished.");
                        pendingRecord = null;
                        if (!publisher.publishRecord(record))
                        {
                            pendingRecord = record;
                        }
                        else if (pendingRecord == null && isNotExit())
                        {
                            flag--;
                        }
                        // TODO
                        Thread.sleep(100);
                    }
                }
            }
            catch (AccessDeniedException e)
            {
                TrackedFile currentOpenFile = fileTracker.getCurrentOpenFile();
                if (currentOpenFile != null && currentOpenFile.isOpen()
                    && e.getFile().equals(currentOpenFile.getPath().toString()))
                {
                    currentOpenFile.close();
                }
                LOGGER.warn("{}: Access denied on currentTrackedFile {}, already close it. ErrorMsg [{}]",
                    serviceName(),
                    currentOpenFile,
                    e.toString());
            }
            catch (Exception e)
            {
                LOGGER.error("{}: Error when processing current input file or when tracking its status.",
                    serviceName(),
                    e);
            }
            return processed;
        }
    }
    
    public class DISFileTailerExecutor implements FileTailerExecutor
    {
        @Override
        public int processFile()
        {
            int processed = 0;
            try
            {
                boolean refreshed = false;
                TrackedFileList pendingFiles = TrackedFileList.emptyList();
                long elapsedSinceLastRefresh = System.currentTimeMillis() - fileTracker.getLastRefreshTimestamp();
                if (elapsedSinceLastRefresh >= maxTimeBetweenFileTrackerRefreshMillis
                    || fileTracker.mustRefreshSnapshot())
                {
                    if (flow.isFileAppendable())
                    {
                        pendingFiles = fileTracker.refreshTrackFileList();
                    }
                    else
                    {
                        pendingFiles = fileTracker.refreshSmallFileTrackFileList();
                    }
                    refreshed = true;
                }
                
                if (pendingFiles.size() == 0 && fileTracker.getCurrentOpenFile() != null)
                {
                    pendingFiles = new TrackedFileList(Collections.singletonList(fileTracker.getCurrentOpenFile()));
                }
                
                int flag = pendingFiles.size() - 1;
                if (flag >= 0)
                {
                    // 按从旧到新的顺序解析文件
                    while (flag > -1 && isNotExit())
                    {
                        TrackedFile trackedFile = pendingFiles.get(flag);
                        if (trackedFile != fileTracker.getCurrentOpenFile())
                        {
                            // 切换新文件，将当前缓冲区入队列清空
                            // publisher.publishCurrentBuffer(pendingRecord, parser.getCurrentFile());
                            fileTracker.startTailingNewFile(flag);
                            parser.switchParsingToFile(fileTracker.getCurrentOpenFile());
                        }
                        else if (refreshed && pendingRecord == null
                            || !parser.isParsing() && fileTracker.getCurrentOpenFile() != null)
                        {
                            parser.continueParsingWithFile(fileTracker.getCurrentOpenFile());
                        }
                        
                        processed += processRecordsInCurrentFile();
                        if (pendingRecord != null)
                        {
                            if (isNotExit() && minTimeBetweenFilePollsMillis > 0)
                            {
                                try
                                {
                                    LOGGER.trace("trigger backpressure. {}", processed);
                                    Thread.sleep(minTimeBetweenFilePollsMillis);
                                }
                                catch (InterruptedException e)
                                {
                                    Thread.currentThread().interrupt();
                                    LOGGER.trace("{}: Thread interrupted", e);
                                }
                            }
                        }
                        else if (parser.isAtEndOfCurrentFile())
                        {
                            // We think we reached the end of the current file...
                            LOGGER.trace("{}: We reached end of file {}.", serviceName(), parser.getCurrentFile());
                            if (isNotExit() && fileTracker.onEndOfCurrentFile())
                            {
                                flag--;
                            }
                        }
                    }
                }
                // 上次监控文件删除，且没有其他文件变化，更新监控
                else if (parser.isParsing())
                {
                    // publisher.publishCurrentBuffer(pendingRecord, parser.getCurrentFile());
                    TrackedFile nextTrackedFile = null;
                    for (TrackedFile file : fileTracker.getCurrentSnapshot())
                    {
                        // 如果文件已经在删除中，则不再监控此文件
                        if (!file.getIsDeleting())
                        {
                            nextTrackedFile = file;
                            break;
                        }
                    }
                    
                    if (nextTrackedFile == null)
                    {
                        // 停止监控
                        parser.switchParsingToFile(null);
                        return pendingRecord == null ? 0 : 1;
                    }
                    else
                    {
                        // 监控最新的
                        fileTracker.startTailingNewFile(nextTrackedFile);
                        parser.switchParsingToFile(fileTracker.getCurrentOpenFile());
                    }
                }
            }
            catch (AccessDeniedException e)
            {
                TrackedFile currentOpenFile = fileTracker.getCurrentOpenFile();
                if (currentOpenFile != null && currentOpenFile.isOpen()
                    && e.getFile().equals(currentOpenFile.getPath().toString()))
                {
                    fileTracker.stopTailingCurrentFile();
                }
                LOGGER.warn("{}: Access denied on currentTrackedFile {}, close it. ErrorMsg [{}]",
                    serviceName(),
                    currentOpenFile,
                    e.toString());
            }
            catch (Exception e)
            {
                TrackedFile currentOpenFile = fileTracker.getCurrentOpenFile();
                // 文件打开失败则重置当前打开文件
                if (currentOpenFile != null && !currentOpenFile.isOpen())
                {
                    fileTracker.stopTailingCurrentFile();
                }
                LOGGER.error("{}: Error when processing current input file or when tracking its status.",
                    serviceName(),
                    e);
            }
            return processed;
        }
    }
}
