package com.huaweicloud.dis.agent.tailing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.huaweicloud.dis.agent.ByteBuffers;
import com.huaweicloud.dis.agent.processing.exceptions.DataConversionException;
import com.huaweicloud.dis.agent.processing.interfaces.IDataConverter;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The base record parser implementation which implements all the mechanics of reading a stream of records from an IO
 * channel. Details about the specific shape and format of a record are delegated to the specific {@link ISplitter} for
 * the {@link FileFlow}, and the logic of constructing a new new record is encapsulated in the
 * {@link #buildRecord(TrackedFile, ByteBuffer, long, int)} method which must be implemented by subclasses.
 *
 * @see IParser
 */
public abstract class AbstractParser<R extends IRecord> implements IParser<R>
{
    protected final Logger logger;
    
    @Getter
    protected final FileFlow<R> flow;
    
    @Getter
    protected final String name;
    
    @Getter
    protected final ISplitter recordSplitter;
    
    @Getter
    protected final int bufferSize;
    
    @Getter
    protected TrackedFile currentFile;
    
    @Getter
    protected final IDataConverter dataConverter;
    
    @VisibleForTesting
    FileChannel currentFileChannel;
    
    private long currentFileChannelOffset = -1;
    
    private int headerLinesToSkip;
    
    @VisibleForTesting
    ByteBuffer currentBuffer;
    
    private long currentBufferStartOffset = -1;
    
    private TrackedFile currentBufferFile;
    
    private boolean currentBufferExhausted = false;
    
    private boolean currentBufferFileEnded = false;
    
    private int recordsFromCurrentBuffer;
    
    private int currentBufferSavedReadPosition = -1;
    
    private final AtomicLong totalBytesConsumed = new AtomicLong();
    
    private final AtomicLong totalBytesDiscarded = new AtomicLong();
    
    private final AtomicLong totalRecordsParsed = new AtomicLong();
    
    private final AtomicLong totalRecordsLargerThanBuffer = new AtomicLong();
    
    private final AtomicLong totalUndhandledErrors = new AtomicLong();
    
    private final AtomicLong totalRecordsProcessed = new AtomicLong();
    
    private final AtomicLong totalRecordsSkipped = new AtomicLong();
    
    private final AtomicLong totalDataProcessingErrors = new AtomicLong();
    
    public AbstractParser(FileFlow<R> flow)
    {
        this(flow, flow.getParserBufferSize());
    }
    
    public AbstractParser(FileFlow<R> flow, int bufferSize)
    {
        this.flow = flow;
        this.name = getClass().getSimpleName() + "[" + flow.getId() + "]";
        this.recordSplitter = this.flow.getRecordSplitter();
        this.dataConverter = this.flow.getDataConverter();
        this.bufferSize = bufferSize;
        this.logger = LoggerFactory.getLogger(getClass());
        Preconditions.checkArgument(bufferSize >= getMaxRecordSize(),
            "The buffer size needs to be larger than the max record size (" + getMaxRecordSize() + ")");
    }
    
    @Override
    public synchronized boolean isParsing()
    {
        return currentFile != null;
    }
    
    @Override
    public synchronized boolean startParsingFile(TrackedFile file)
    {
        if (setCurrentFile(file, true))
        {
            try
            {
                goToInitialPosition();
                return true;
            }
            catch (IOException e)
            {
                logger.error("{}: Failed setting the initial position on file {}", name, file, e);
                stopParsing("Unhandled error.");
                totalUndhandledErrors.incrementAndGet();
                return false;
            }
        }
        return false;
    }
    
    @Override
    public synchronized boolean continueParsingWithFile(TrackedFile file)
    {
        return setCurrentFile(file, false);
    }
    
    @Override
    public synchronized boolean switchParsingToFile(TrackedFile file)
    {
        return setCurrentFile(file, true);
    }
    
    @Override
    public synchronized boolean stopParsing(String reason)
    {
        if (currentBuffer != null || currentFile != null)
        {
            discardCurrentBuffer(reason);
            currentFile = null;
            currentFileChannel = null;
            currentFileChannelOffset = -1;
            headerLinesToSkip = 0;
            return true;
        }
        else
        {
            return false;
        }
    }
    
    @Override
    public synchronized boolean isAtEndOfCurrentFile()
    {
        try
        {
            return currentFile != null && currentFileChannelOffset >= currentFileChannel.size();
        }
        catch (IOException e)
        {
            logger.error("{}: Failed when getting the size of current channel for file {}.", name, currentFile, e);
            stopParsing("Unhandled error.");
            totalUndhandledErrors.incrementAndGet();
            return false;
        }
    }
    
    private boolean setCurrentFile(TrackedFile file, boolean resetParsing)
    {
        try
        {
            Preconditions.checkArgument(file == null || file.isOpen());
            if (resetParsing || file == null)
                stopParsing("Parsing is reset by caller.");
            if (file != null)
            {
                long newOffset = file.getChannel().position();
                if (currentFile == null || resetParsing)
                {
                    logger.debug("{}: Opening {} for parsing.", name, file.getPath());
                    currentBufferFileEnded = true;
                }
                else
                {
                    // if the offset changed, the file must have rotated and we can't continue with the current buffer
                    currentBufferFileEnded = currentBufferFileEnded || (currentFileChannelOffset != newOffset);
                    if (!sameAsCurrentFile(file))
                    {
                        logger.info("Continuing to parse {}, ", file.getPath());
                    }
                    logger.debug("{}: Old offset: {}, new offset: {}", name, currentFileChannelOffset, newOffset);
                }
                currentFile = file;
                currentFileChannel = currentFile.getChannel();
                currentFileChannelOffset = currentFileChannel.position();
                headerLinesToSkip = currentFileChannelOffset == 0 ? flow.getSkipHeaderLines() : 0;
                return true;
            }
            else
            {
                return false;
            }
        }
        catch (IOException e)
        {
            logger.error("{}: Failed when setting current file to {} (reset={}).", name, file, resetParsing, e);
            stopParsing("Unhandled error.");
            totalUndhandledErrors.incrementAndGet();
            return false;
        }
    }
    
    private boolean sameAsCurrentFile(TrackedFile file)
    {
        return file != null && file.getId().equals(currentFile.getId()) && file.getPath().equals(currentFile.getPath());
    }
    
    private void goToInitialPosition()
        throws IOException
    {
        if (flow.getInitialPosition() == FileFlow.InitialPosition.END_OF_FILE)
        {
            logger.trace("{}: Moving to last record of file {}", name, currentFile);
            if (!ensureHeaderLinesSkipped())
            {
                logger.trace("{}: File {} was not large enough for header lines. Not moving to end of file any more.",
                    name,
                    currentFile);
                return;
            }
            // Find the last record in the file: read the last chunk of the file, and parse all the records in it
            // stopping at the last record
            long lastChunkOffset = Math.max(0, currentFile.getSize() - bufferSize);
            if (currentFileChannelOffset >= 0)
            {
                // ... could happed if we just skipped header files
                lastChunkOffset = Math.max(currentFileChannelOffset, lastChunkOffset);
            }
            // Artificially advance to the last chunk
            resetCurrentBuffer();
            currentFileChannel.position(lastChunkOffset);
            currentFileChannelOffset = lastChunkOffset;
            if (readNextChunk() > 0)
            {
                int previousOffset = 0;
                int nextOffset = recordSplitter.locateNextRecord(currentBuffer);
                while (nextOffset != -1)
                {
                    previousOffset = nextOffset;
                    nextOffset = recordSplitter.locateNextRecord(currentBuffer);
                }
                // We reached the end of the buffer, now roll back to last known
                // record boundary
                currentBuffer.position(previousOffset);
                logger.trace("{}: Advanced to last record boundary in file at offset {}.",
                    name,
                    toChannelOffset(previousOffset));
                // NOTE: If previousOffset == 0 it means either:
                // a) the last chunk of the file contains no record
                // boundaries (last record is too large)
                // b) the file contains a single record
                // There's no way to know which... If a) we have a malformed
                // record and will be handled when we next read it.
            } // else: the file is empty
        } // else: we're already at beginning of file
    }
    
    @Override
    public synchronized int bufferedBytesRemaining()
    {
        return currentBuffer != null ? currentBuffer.remaining() : 0;
    }
    
    @Override
    public synchronized R readRecord()
    {
        try
        {
            if (currentBuffer == null || currentBufferExhausted)
            {
                tryReadMoreRecordsFromChannel();
            }
            R record = readRecordFromCurrentBuffer();
            if (record != null)
                return record;
            else
            {
                currentBufferExhausted = true;
                tryReadMoreRecordsFromChannel();
                return readRecordFromCurrentBuffer();
            }
        }
        catch (IOException e)
        {
            logger.error("{}: Failed when parsing record from current file {}", name, currentFile, e);
            stopParsing("Unhandled error.");
            totalUndhandledErrors.incrementAndGet();
            return null;
        }
    }
    
    private R readRecordFromCurrentBuffer()
        throws IOException
    {
        if (currentBuffer == null)
            return null;
        int currentRecordOffset = currentBuffer.position();
        int nextRecordOffset = recordSplitter.locateNextRecord(currentBuffer);
        if (currentBufferFile.getFlow().isMissLastRecordDelimiter() && (currentRecordOffset + 1) == nextRecordOffset && currentBuffer.hasRemaining())
        {
            // 忽略第一个换行符，防止发送空数据
            currentRecordOffset++;
            nextRecordOffset = recordSplitter.locateNextRecord(currentBuffer);
        }
        if (nextRecordOffset != -1)
        {
            return buildRecord(currentRecordOffset, nextRecordOffset - currentRecordOffset);
        }
        else
        {
            // Either there isn't a complete record in the buffer yet,
            // or the record size exceeds the buffer size. Find out which.
            if (currentRecordOffset == 0 && currentBuffer.limit() == currentBuffer.capacity())
            {
                // Rewind to where we started...
                currentBuffer.position(currentRecordOffset);
                
                // We encountered a record that's larger than the buffer size.
                // We need to truncate the record to the buffer size, and then
                // discard the remainder of the file until we reach next
                // record. This is essential to handle "runaway" records and
                // corrupted files.
                logger.warn("{}: Detected record larger than buffer size ({} bytes) in {} at offset {}. "
                    + "Discarding data until next record.", name, bufferSize, currentBufferFile, toChannelOffset(0));
                totalRecordsLargerThanBuffer.incrementAndGet();
                // 1. Use the whole buffer as the current record.
                currentBuffer.position(currentBuffer.limit());
                R record = buildRecord(0, currentBuffer.limit());
                // 2. Discard data from file until we locate the beginning of a new record.
                while (readNextChunk() > 0)
                {
                    nextRecordOffset = recordSplitter.locateNextRecord(currentBuffer);
                    // Check if we found a new record, or we still need to discard some more...
                    if (nextRecordOffset > 0)
                    {
                        onDiscardedData(0, nextRecordOffset - 1, "Runaway record.");
                        break;
                    }
                    else
                    {
                        onDiscardedData(0, currentBuffer.limit(), "Runaway record.");
                        currentBuffer.position(currentBuffer.limit());
                    }
                }
                return record;
            }
            else
            {
                // 已经解析到文件结尾但是还有数据没有生成Record，则缺失最后一个分隔符
                if (currentBufferExhausted && currentBuffer.position() > currentRecordOffset)
                {
                    if (!flow.isFileAppendable())
                    {
                        // 文件上传模式，即使没有分隔符，最后一行也需上传
                        return buildRecord(currentRecordOffset, currentBuffer.limit() - currentRecordOffset);
                    }

                    if (flow.isMissLastRecordDelimiter())
                    {
                        // 如果当前文件缺失分隔符，且等待时间超过指定值，则将内容解析
                        if (currentBufferFile.getMissLastRecordDelimiterTime() > 0
                                && System.currentTimeMillis() - currentBufferFile.getMissLastRecordDelimiterTime() >= flow.getMaxFileCheckingMillis())
                        {
                            return buildRecord(currentRecordOffset, currentBuffer.limit() - currentRecordOffset);
                        }
                        else if (currentBufferFile.getMissLastRecordDelimiterTime() <= 0)
                        {
                            // 设置缺失分隔符的时间点
                            currentBufferFile.setMissLastRecordDelimiterTime(System.currentTimeMillis());
                        }
                    }
                }
                // 设置buffer的位置为上一次生成Record的位置
                currentBuffer.position(currentRecordOffset);
                currentBufferExhausted = true;
                return null;
            }
        }
    }
    
    /**
     * 读取数据到currentBuffer中(最多5M数据)
     *
     * @return
     * @throws IOException
     */
    private int readNextChunk()
        throws IOException
    {
        prepareCurrentBufferForWriting();
        long startOffset = currentFileChannel.position();
        Preconditions.checkState(currentFileChannelOffset == -1 || currentFileChannelOffset == startOffset,
            "%s: Channel expected to be at offset %s but was at offset %s.",
            name,
            currentFileChannelOffset,
            startOffset);
        currentBufferFile = currentFile;
        if (currentBufferStartOffset == -1)
        {
            currentBufferStartOffset = currentFileChannel.position();
        }
        int bytes = currentFileChannel.read(currentBuffer);
        currentFileChannelOffset = currentFileChannel.position();
        prepareCurrentBufferForReading();
        if (bytes > 0)
        {
            currentBufferExhausted = false;
            totalBytesConsumed.addAndGet(bytes);
            if (logger.isTraceEnabled())
            {
                try
                {
                    logger.trace("{}: Consumed bytes {}-{} of {} from {}",
                        name,
                        currentBufferStartOffset,
                        currentFileChannel.position(),
                        currentFileChannel.size(),
                        currentBufferFile.getPath());
                }
                catch (IOException e)
                {
                    logger.debug("{}: Ignoring error after successful read.", e);
                }
            }
        }
        return bytes;
    }
    
    /**
     * @return The number of bytes read from the channel, which can be {@code 0}, or {@code -1} if the EOF was reached
     *         on the channel. (See {@link ReadableByteChannel#read(ByteBuffer)}.)
     * @throws IOException
     */
    private void tryReadMoreRecordsFromChannel()
        throws IOException
    {
        if (currentFile != null)
        {
            ensureHeaderLinesSkipped();
            readNextChunk();
        }
    }
    
    private boolean ensureHeaderLinesSkipped()
        throws IOException
    {
        // skipping configured number of header lines
        if (headerLinesToSkip > 0)
        {
            if (skipHeaderLines() > 0)
            {
                // if we haven't skipped all the lines
                return false;
            }
        }
        return true;
    }
    
    private void discardCurrentBuffer(String reason)
    {
        if (currentBuffer != null)
        {
            // 在缺失分隔符的情况下，丢弃buffer中的数据是正常情况不用提示
            if (currentBuffer.remaining() > 0 && currentBufferFile.getMissLastRecordDelimiterTime() <= 0)
            {
                onDiscardedData(currentBuffer.position(), currentBuffer.remaining(), reason);
                if (logger.isDebugEnabled())
                {
                    try
                    {
                        logger.debug("{}: Data being discarded: {}",
                            name,
                            ByteBuffers
                                .visibleWhiteSpaces(ByteBuffers.toString(currentBuffer, flow.getFileEncoding())));
                    }
                    catch (Exception e)
                    {
                        logger.debug("{}: Error when printing data chunck.", e);
                    }
                }
            }
            // Discard old buffer
            resetCurrentBuffer();
        }
    }
    
    private void resetCurrentBuffer()
    {
        currentBuffer = null;
        currentBufferStartOffset = -1;
        currentBufferFile = null;
        currentBufferExhausted = false;
        recordsFromCurrentBuffer = 0;
        currentBufferFileEnded = false;
    }
    
    /**
     * @return The remaining number of header lines that still need to be skipped, if any.
     * @throws IOException
     */
    private int skipHeaderLines()
        throws IOException
    {
        int skippedLines = 0;
        logger.trace("{}: Skipping {} lines from current file {}", name, headerLinesToSkip, currentFile);
        while (headerLinesToSkip > 0 && readNextChunk() != -1)
        {
            while (headerLinesToSkip > 0
                && ByteBuffers.advanceBufferToNextLine(currentBuffer, flow.getRecordDelimiter()) != -1)
            {
                skippedLines++;
                --headerLinesToSkip;
            }
        }
        if (headerLinesToSkip > 0)
        {
            logger.trace(
                "{}: Have read through the entire file, skipped {} lines, and {} more lines remain to be skipped.",
                name,
                skippedLines,
                headerLinesToSkip);
        }
        logger.debug("{}: Skipped {} lines ({} bytes) from the beginning of {}",
            name,
            skippedLines,
            currentFileChannelOffset - currentBuffer.remaining(),
            currentFile,
            currentFileChannelOffset);
        return headerLinesToSkip;
    }
    
    private void prepareCurrentBufferForWriting()
    {
        currentBufferSavedReadPosition = -1;
        if (currentBuffer == null)
        {
            getNewCurrentBuffer();
        }
        else if (currentBufferFileEnded)
        {
            discardCurrentBuffer("New file was openeded.");
            getNewCurrentBuffer();
        }
        else if (currentBuffer.limit() == currentBuffer.capacity())
        {
            if (recordsFromCurrentBuffer > 0)
            {
                // TODO: Add metrics for bytes copied between buffers
                if (currentBuffer.hasRemaining())
                {
                    ByteBuffer oldBuffer = currentBuffer;
                    TrackedFile oldBufferFile = currentBufferFile;
                    long oldBufferStartOffset = currentBufferStartOffset;
                    getNewCurrentBuffer();
                    currentBufferFile = oldBufferFile;
                    currentBufferStartOffset = oldBufferStartOffset + oldBuffer.position();
                    currentBuffer.put(oldBuffer);
                    // NOTE: Here we have a slight duplication of data in memory:
                    // the data copied from the old buffer now exists in both
                    // buffers, though in the first buffer it's not referenced
                    // by any records.
                    if (logger.isTraceEnabled())
                    {
                        logger.trace("{}: Copied {} bytes from the current buffer to new one.",
                            name,
                            currentBuffer.position());
                    }
                }
                else
                {
                    getNewCurrentBuffer();
                }
            }
            else
            {
                // Reuse the current buffer
                logger.trace("{}: Reusing current buffer (capacity:{}, limit: {}, remaining: {}).",
                    name,
                    currentBuffer.capacity(),
                    currentBuffer.limit(),
                    currentBuffer.remaining());
                currentBufferStartOffset += currentBuffer.position();
                currentBuffer.compact();
            }
        }
        else
        {
            logger.trace(
                "{}: Current buffer still has space to write and will be kept (capacity: {}, limit: {}, remaining: {}).",
                name,
                currentBuffer.capacity(),
                currentBuffer.limit(),
                currentBuffer.remaining());
            // Keep track where we should reposition in the buffer when it's time to read again...
            currentBufferSavedReadPosition = currentBuffer.position();
            currentBuffer.position(currentBuffer.limit());
            currentBuffer.limit(currentBuffer.capacity());
        }
    }
    
    private void prepareCurrentBufferForReading()
    {
        currentBuffer.flip();
        if (currentBufferSavedReadPosition > 0)
        {
            currentBuffer.position(currentBufferSavedReadPosition);
            currentBufferSavedReadPosition = -1;
        }
    }
    
    private void getNewCurrentBuffer()
    {
        if (logger.isTraceEnabled() && currentBuffer != null)
        {
            logger.trace("{}: Parsed {} records from current buffer. Allocating new buffer of size {}.",
                name,
                recordsFromCurrentBuffer,
                bufferSize);
        }
        recordsFromCurrentBuffer = 0;
        currentBuffer = ByteBuffer.allocate(bufferSize);
        currentBufferStartOffset = -1;
        currentBufferFile = null;
        currentBufferFileEnded = false;
        currentBufferExhausted = true;
    }
    
    private R buildRecord(int offset, int length)
    {
        ByteBuffer data = ByteBuffers.getPartialView(currentBuffer, offset, length);
        ++recordsFromCurrentBuffer;
        Preconditions.checkNotNull(currentBufferFile);
        long channelOffsetStart = toChannelOffset(offset);
        // 重置缺失分隔符的时间点
        currentBufferFile.setMissLastRecordDelimiterTime(-1);
        // 设置文件已经解析到的位置
        currentBufferFile.setLastOffset(channelOffsetStart + length);

        R record = null;
        try
        {
            record = buildRecord(currentBufferFile, convertData(data), channelOffsetStart, length);
        }
        catch (DataConversionException e)
        {
            totalDataProcessingErrors.incrementAndGet();
            logger.warn("Cannot process input data: " + e.getMessage() + ", falling back to raw data.");
            record = buildRecord(currentBufferFile, data, channelOffsetStart, length);
        }
        finally
        {
            totalRecordsParsed.incrementAndGet();
        }
        
        return record;
    }
    
    private ByteBuffer convertData(ByteBuffer data)
        throws DataConversionException
    {
        if (getDataConverter() == null)
            return data;
        
        ByteBuffer result = getDataConverter().convert(data);
        if (result != null)
        {
            totalRecordsProcessed.incrementAndGet();
        }
        else
        {
            totalRecordsSkipped.incrementAndGet();
            logger.warn("1 record parsed but skipped for processing and delivering");
        }
        return result;
    }
    
    private long toChannelOffset(int bufferOffset)
    {
        Preconditions.checkState(currentBufferStartOffset >= 0,
            "Buffer start offset (%s) is expected to be non-negative!",
            currentBufferStartOffset);
        Preconditions.checkState(bufferOffset >= 0, "Buffer offset (%s) is expected to be non-negative!", bufferOffset);
        return currentBufferStartOffset + bufferOffset;
    }
    
    @VisibleForTesting
    synchronized void onDiscardedData(int bufferOffset, int length, String reason)
    {
        totalBytesDiscarded.addAndGet(length);
        logger.warn("{}: Discarded {} bytes from {} at offset {}. Reason: {}",
            name,
            length,
            currentBufferFile,
            toChannelOffset(bufferOffset),
            reason);
    }
    
    protected abstract R buildRecord(TrackedFile recordFile, ByteBuffer data, long offset, int length);
    
    protected abstract int getMaxRecordSize();
    
    @SuppressWarnings("serial")
    @Override
    public Map<String, Object> getMetrics()
    {
        final String className = getClass().getSimpleName();
        return new HashMap<String, Object>()
        {
            {
                put(className + ".TotalBytesConsumed", totalBytesConsumed);
                put(className + ".TotalRecordsParsed", totalRecordsParsed);
                put(className + ".TotalBytesDiscarded", totalBytesDiscarded);
                put(className + ".TotalRecordsLargerThanBuffer", totalRecordsLargerThanBuffer);
                put(className + ".TotalUnhandledErrors", totalUndhandledErrors);
                put(className + ".TotalRecordsProcessed", totalRecordsProcessed);
                put(className + ".TotalRecordsSkipped", totalRecordsSkipped);
                put(className + ".TotalDataProcessingErrors", totalDataProcessingErrors);
            }
        };
    }
}
