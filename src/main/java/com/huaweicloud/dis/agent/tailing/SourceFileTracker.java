package com.huaweicloud.dis.agent.tailing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.huaweicloud.dis.agent.AgentContext;
import com.huaweicloud.dis.agent.tailing.checkpoints.FileCheckpointStore;
import lombok.Getter;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Component responsible for tracking a collection source files specified by a {@link FileFlow}. It maintains an
 * internal snapshot of the current list of files that belong to the flow (that match the {@code file} pattern) as well
 * as a pointer (and open channel) to the file currently tailed. Every time the {@link #refresh()} method is called, the
 * internal snapshot is refreshed making sure the file currently tailed does not change to maintain continuity, but
 * keeping track of any rotations and new files that might have showed up since the last snapshot.
 * <p>
 * Once the current file is fully read by some consumer {@link #onEndOfCurrentFile()} must be called so that the current
 * file is advanced to the next pending file if any.
 * <p>
 * When {@link #refresh()} is called, it will do the following:
 * <p>
 * <ul>
 * <li>If there's a file currently tailed ({@code {@link #currentFile()} != null}):
 * <ol>
 * <li>List all exisisting files that belong to the flow, and order them by last modified date with most-recently
 * modified file on top (see {@link SourceFile#listFiles()}).</li>
 * <li>Compare file path, {@link FileId id}, size and last modified date to heuristically determine if a file rotation
 * has happened (see {@link #updateCurrentFile(List)} for details of logic).</li>
 * <li>If file rotation is detected, it modifies the {@link #currentFile()} to make sure that it remains pointing to the
 * same file at the same offset.</li>
 * <li>Updated its internal snapshot of the current files, including any new files that have showed up after the
 * rotation.</li>
 * </ol>
 * </li>
 * <li>If there isn't a file currently tailed ({@code {@link #currentFile()} == null}), or if the instance if being
 * initialized:
 * <ol>
 * <li>If a checkpoint was found for the given flow, it reads it and uses it as the current file.</li>
 * <li>Re-runs the heuristics described above and opens the appropriate file for tailing.</li>
 * </ol>
 * </li>
 * </ul>
 * <p>
 * File rotations supported (see {@linkplain http://linuxcommand.org/man_pages/logrotate8.html logrotate} man page for
 * some information on typical rotation configuration):
 * <p>
 * <ul>
 * <li>Rename: close current file + rename + create new file with same name. Example: file {@code access.log} is closed
 * then renamed {@code access.log.1} or {@code access.log.2014-12-29-10-00-00}, then a new file named {@code access.log}
 * is created. This is the default behavior of <code>logrotate<utility></li>
 * <li>Truncate: copy contents of current file to new file + truncate current file (keeping same name and inode).
 * Example: contents of file {@code access.log} are copied into {@code access.log.1} or
 * {@code access.log.2014-12-29-10-00-00}, then {@code access.log} is truncated (size reset to 0). This is similar to
 * {@code logrotate}'s {@code copytruncate} configuration option.<br />
 * Note: The edge case where a file is truncated and then populated by same (or larger) number of bytes as original file
 * between calls to {@link #refresh()} is not handled by this implementation. If this happens, the rotation will not be
 * detected by the current heuristic. This is thought to be a very rare edge case in typical systems. To handle it, we
 * need to use a fingerprinting scheme and use a hash of the content of the file as a file identifier.</li>
 * <li>Create: the file name is typically constructed with a timestamp providing time-based rotation. Example: to rotate
 * a new file {@code access.log.2014-12-29-10-00-00}, a new file {@code access.log.2014-12-29-11-00-00} is created.</li>
 * <li>Copy: copy contents of current file to new file but don't truncate the current file. This is similar to
 * {@code logrotate}'s {@code copy} configuration option. The way to handle this rotation mode is to disable rotation
 * tracking completely by specifying a specific file name (rather than a glob) to track. The tests of this class include
 * cases that cover this mode.</li>
 * </ul>
 * <p>
 * Notes:
 * <p>
 * <ul>
 * <li>This class does not modify the file checkpoints.</li>
 * <li>Behavior is undefined if rotation occurs <strong>while</strong> {@link #refresh()} is running. TODO: investigate
 * ability to lock latest file in {@code newSnapshot} as a way to prevent rotation.</li>
 * <li>If any anomaly (something unexpected by rotation-detection heuristic) is detected, it assumes the worse and
 * resets the tailing to the latest file (by modification time).</li>
 * <li>The assumption made by this class is once a file is rotatoted, it can only be deleted, but cannot be modified,
 * and will keep the ID (inode). The following entails:
 * <ol>
 * <li>If {@code newerFilesPending() == true} then {@code currentFile().size()} is the final size of the file. When the
 * reader finishes reading the file up to its size, they can move to the next pending file.</li>
 * </ol>
 * </li>
 * </ul>
 */
@NotThreadSafe
public class SourceFileTracker
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceFileTracker.class);
    
    @VisibleForTesting
    final FileFlow<?> flow;
    
    private final SourceFile sourceFile;
    
    @Getter
    private TrackedFile currentOpenFile = null;
    
    @Getter
    private int currentOpenFileIndex = -1;
    
    @Getter
    protected TrackedFileList pendingFiles;
    
    @Getter
    protected TrackedFileList currentSnapshot;
    
    @Getter
    protected long lastRefreshTimestamp = 0;
    
    private FileCheckpointStore checkpoints;
    
    public SourceFileTracker(AgentContext agentContext, FileFlow<?> flow)
        throws IOException
    {
        this.flow = flow;
        this.sourceFile = flow.getSourceFile();
    }
    
    public SourceFileTracker(AgentContext agentContext, FileFlow<?> flow, FileCheckpointStore checkpoints)
        throws IOException
    {
        this(agentContext, flow);
        this.checkpoints = checkpoints;
    }
    
    /**
     * @return {@code true} if current open file was changed, otherwise {@code false}.
     * @throws IOException
     */
    public boolean onEndOfCurrentFile()
        throws IOException
    {
        if (currentOpenFile != null && currentOpenFile.getCurrentOffset() < currentOpenFile.getCurrentSize())
        {
            LOGGER.debug(
                "Current file offset ({}) is smaller than its size ({}), " + "and consumer reported it has ended! ({})",
                currentOpenFile.getCurrentOffset(),
                currentOpenFile.getCurrentSize(),
                currentOpenFile);
            return false;
        }
        return true;
    }
    
    private void closeCurrentFileIfOpen()
    {
        if (currentOpenFile != null && currentOpenFile.isOpen())
            closeCurrentFile();
    }
    
    private void closeCurrentFile()
    {
        currentOpenFile.close();
    }
    
    protected void startTailingNewFile(TrackedFile trackedFile)
        throws IOException
    {
        closeCurrentFileIfOpen();
        currentOpenFile = trackedFile;
        // TODO start from 0 ?
        currentOpenFile.open(currentOpenFile.getLastOffset());
        currentOpenFileIndex = 0;
        LOGGER.trace("Started tailing new file (index {}): {}", currentOpenFileIndex, currentOpenFile);
    }
    
    protected void startTailingNewFile(int index)
        throws IOException
    {
        Preconditions.checkArgument(index >= 0 && index < pendingFiles.size(),
            "Index " + index + ", but pengdingFile size is " + pendingFiles.size());
        closeCurrentFileIfOpen();
        currentOpenFile = pendingFiles.get(index);
        currentOpenFile.open(currentOpenFile.getLastOffset());
        currentOpenFileIndex = index;
        pendingFiles = index > 0 ? pendingFiles.subList(0, pendingFiles.size() - 1) : TrackedFileList.emptyList();
        LOGGER.trace("Started tailing new file (index {}): {}", currentOpenFileIndex, currentOpenFile);
    }
    
    protected void continueTailingCurrentFile(TrackedFile newCurrentFile)
        throws IOException
    {
        Preconditions.checkArgument(newCurrentFile != null);
        if (currentOpenFile.isOpen())
            newCurrentFile.inheritChannel(currentOpenFile);
        currentOpenFile = newCurrentFile;
        currentOpenFileIndex = 0;
        LOGGER.trace("Continuing to tail current file : {}", currentOpenFile);
    }
    
    protected void stopTailingCurrentFile()
    {
        closeCurrentFileIfOpen();
        currentOpenFile = null;
        currentOpenFileIndex = -1;
    }
    
    public boolean initFormHistory()
        throws IOException
    {
        TrackedFileList oldSnapshot = new TrackedFileList(checkpoints.getAllCheckpointForFlow(this.flow));
        if (!oldSnapshot.isEmpty())
        {
            for (TrackedFile trackedFile : oldSnapshot)
            {
                if (Files.exists(trackedFile.getPath()))
                {
                    // && FileId.get(Files.readAttributes(trackedFile.getPath(),
                    // BasicFileAttributes.class)).equals(trackedFile.getId())
                    this.currentOpenFile = trackedFile;
                    this.currentOpenFileIndex = 0;
                    this.currentSnapshot = oldSnapshot;
                    closeCurrentFileIfOpen();
                    currentOpenFile.open(currentOpenFile.getLastOffset());
                    this.pendingFiles = TrackedFileList.emptyList();
                    break;
                }
            }
            
            if (currentOpenFile != null)
            {
                LOGGER.info("Recover track [{}] from history.", currentOpenFile);
            }
            else
            {
                // 文件路径没有匹配的，则只记录数据库的snapshot
                this.currentOpenFile = null;
                this.currentOpenFileIndex = 0;
                this.currentSnapshot = oldSnapshot;
                closeCurrentFileIfOpen();
                // currentOpenFile.open(currentOpenFile.getLastOffset());
                this.pendingFiles = TrackedFileList.emptyList();
                LOGGER.info("No file is tracked due to all files in history db have been deleted or rename. {}",
                    flow.getId());
            }
            return true;
        }
        else
        {
            LOGGER.info("No file is tracked due to no data in history db. {}", flow.getId());
        }
        
        currentSnapshot = pendingFiles = TrackedFileList.emptyList();
        return false;
    }
    
    public boolean initFromCurrentFiles()
        throws IOException
    {
        TrackedFileList oldSnapshot = sourceFile.listFiles();
        if (!oldSnapshot.isEmpty())
        {
            Iterator<TrackedFile> iterator = oldSnapshot.getSnapshot().iterator();
            while (iterator.hasNext())
            {
                TrackedFile trackedFile = iterator.next();
                if (Files.exists(trackedFile.getPath())
                    && flow.getInitialPosition() == FileFlow.InitialPosition.END_OF_FILE)
                {
                    trackedFile.setLastOffset(trackedFile.getSize());
                    checkpoints.saveCheckpoint(trackedFile, trackedFile.getLastOffset());
                    LOGGER.info("File [{}] starts from end.", trackedFile);
                }
                else
                {
                    iterator.remove();
                }
            }
            
            if (!oldSnapshot.isEmpty())
            {
                this.currentOpenFile = oldSnapshot.get(0);
                this.currentOpenFileIndex = 0;
                this.currentSnapshot = oldSnapshot;
                closeCurrentFileIfOpen();
                currentOpenFile.open(currentOpenFile.getLastOffset());
                this.pendingFiles = TrackedFileList.emptyList();
                LOGGER.info("Recover track [{}] from history.", currentOpenFile);
                return true;
            }
            else
            {
                LOGGER.info("No file is tracked due to all matched files match have been deleted. {}", flow.getId());
            }
        }
        else
        {
            LOGGER.info("No file is tracked due to no matched files. {}", flow.getId());
        }
        
        currentSnapshot = pendingFiles = TrackedFileList.emptyList();
        return false;
    }
    
    public TrackedFileList refreshTrackFileList()
        throws IOException
    {
        LOGGER.debug("Start to refreshTrackFileList");
        TrackedFileList newSnapshot = sourceFile.listFiles();
        Preconditions.checkNotNull(currentSnapshot);
        
        // Initialize the analysis and log any anomalies
        TrackedFileRotationAnalyzer analyzer =
            new TrackedFileRotationAnalyzer(currentSnapshot, newSnapshot, currentOpenFile);
        
        TrackedFile sameAsCurrentOpenFile = null;
        List<TrackedFile> needUpdateFile = new ArrayList<>();
        // 新文件的数量
        int noCounterpartsCount = analyzer.getIncomingNoCounterpartsCount();
        // 文件truncate
        List<TrackedFile> truncateTrackedFileList = new ArrayList<>();
        if (noCounterpartsCount > 0)
        {
            for (TrackedFile oldFile : currentSnapshot)
            {
                if (analyzer.trackedFileWasTruncated(oldFile))
                {
                    truncateTrackedFileList.add(oldFile);
                }
            }
        }
        
        for (TrackedFile newTrackFile : newSnapshot)
        {
            TrackedFile oldTrackFile = analyzer.getCounterpart(newTrackFile);
            
            if (oldTrackFile == null)
            {
                List<TrackedFile> copyAndTruncateTrackedFileList = new ArrayList<>();
                for (TrackedFile truncateTrackedFile : truncateTrackedFileList)
                {
                    if (newTrackFile.isStartingSameAs(truncateTrackedFile)
                        && newTrackFile.size >= truncateTrackedFile.size)
                    {
                        copyAndTruncateTrackedFileList.add(truncateTrackedFile);
                    }
                }
                
                // 找到唯一一个精确匹配 或者
                // 只有一个新增文件+只有一个truncate文件+新文件不为空+旧文件当时的字节为0，则认为是CopyAndTruncate文件
                if (copyAndTruncateTrackedFileList.size() == 1 || (truncateTrackedFileList.size() == 1
                    && noCounterpartsCount == 1 && newTrackFile.getHeaderBytesLength() != 0
                    && truncateTrackedFileList.get(0).getHeaderBytesLength() == 0))
                {
                    TrackedFile origFile =
                        copyAndTruncateTrackedFileList.size() == 1 ? copyAndTruncateTrackedFileList.get(0)
                            : truncateTrackedFileList.get(0);
                    
                    // 将原文件的offset更新到新文件的offset中
                    // TODO 当出现CopyAndTruncate时，如果存在原始文件还没有发送完毕，会导致offset无法更新到Copy文件记录上，导致重启时部分重传
                    long currentOffset =
                        checkpoints.getOffsetForFileID(origFile.getFlow(), origFile.getId().toString());
                    checkpoints.saveCheckpoint(newTrackFile, currentOffset);
                    newTrackFile.setLastOffset(copyAndTruncateTrackedFileList.get(0).getLastOffset());
                    LOGGER.info("Find [CopyAndTruncate] file. \n\tThe Current  file [{}]\n\tThe Previous file [{}]",
                        newTrackFile,
                        origFile);
                }
                else
                {
                    newTrackFile.setLastOffset(0);
                    checkpoints.saveCheckpoint(newTrackFile, 0);
                    LOGGER.info("Find [New] file. \n\tThe Current  file [{}]", newTrackFile);
                }
                
                needUpdateFile.add(newTrackFile);
                continue;
            }
            
            if (oldTrackFile == currentOpenFile)
            {
                newTrackFile.setSameAsCurrentOpenFile(true);
                sameAsCurrentOpenFile = newTrackFile;
            }

            // 继承缺失分隔符的时间点(用于等待一段时间上传没有分隔符结尾的记录)
            newTrackFile.setMissLastRecordDelimiterTime(oldTrackFile.getMissLastRecordDelimiterTime());

            // 两个文件标识头不相等 且 原文件标识头长度>0 且 新文件标识头长度>=-1 时，表示文件iNode被重复使用
            if (!newTrackFile.isStartingSameAs(oldTrackFile) && newTrackFile.getHeaderBytesLength() >= -1
                && oldTrackFile.getHeaderBytesLength() > 0)
            {
                newTrackFile.setLastOffset(0);
                // 文件重新开始，将旧的offset重置为0
                checkpoints.saveCheckpoint(newTrackFile, 0);
                LOGGER.info("Find [Re-New] file. \n\tThe Current  file [{}]\n\tThe Previous file [{}]",
                    newTrackFile,
                    oldTrackFile);
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("\nnewTrackFile size {}, header:[\n{}]\noldTrackFile size {}, header:[\n{}]",
                        Files.size(newTrackFile.getPath()),
                        newTrackFile.getHeaderBytes() == null ? "" : new String(newTrackFile.getHeaderBytes()),
                        Files.size(oldTrackFile.getPath()),
                        oldTrackFile.getHeaderBytes() == null ? "" : new String(oldTrackFile.getHeaderBytes()));
                }
                needUpdateFile.add(newTrackFile);
                continue;
            }
            
            // 文件内容变小
            if (newTrackFile.getSize() < oldTrackFile.getSize())
            {
                newTrackFile.setLastOffset(0);
                checkpoints.saveCheckpoint(newTrackFile, 0);
                LOGGER.info("Find [Shrink] file. \n\tThe Current  file [{}]\n\tThe Previous file [{}]",
                    newTrackFile,
                    oldTrackFile);
                needUpdateFile.add(newTrackFile);
                continue;
            }
            
            if (oldTrackFile.getIsDeleting())
            {
                LOGGER.info("Find [DeleteRecovery] file. \n\tThe Current  file [{}]\n\tThe Previous file [{}]",
                    newTrackFile,
                    oldTrackFile);
            }
            
            // 旧文件还没有解析完成 或 文件新增内容 (增加文件ID判断是因为文件频繁重命名时，可能导致trackFile前后不一致)
            if (oldTrackFile.getLastOffset() < oldTrackFile.getCurrentSize()
                && oldTrackFile.getId().equals(FileId.get(oldTrackFile.getPath()))
                || newTrackFile.size > oldTrackFile.size
                    && oldTrackFile.getId().equals(FileId.get(newTrackFile.getPath())))
            {
                newTrackFile.setLastOffset(oldTrackFile.getLastOffset());
                // LOGGER.info("Find [Expansion] file [{}], the Previous file [{}]", newTrackFile, oldTrackFile);
                needUpdateFile.add(newTrackFile);
                continue;
            }
            
            // 文件内容没有变化，保持不变
            newTrackFile.setLastOffset(oldTrackFile.getLastOffset());
        }

        // 连续两次采集都丢失，则从snapshot中删除(防止操作系统mv方式滚动日志导致偶现没获取文件然后又获取到的情况)
        List<TrackedFile> deleteTrackedFile = new ArrayList<>();
        for (TrackedFile currentDeleteFile : analyzer.getCurrentNoCounterparts())
        {
            if (!currentDeleteFile.getIsDeleting())
            {
                TrackedFile copiedTrackedFile = new TrackedFile(currentDeleteFile);
                copiedTrackedFile.setIsDeleting(true);
                // 添加到newSnapshot中
                newSnapshot.addSingle(copiedTrackedFile);
            }
            else
            {
                deleteTrackedFile.add(currentDeleteFile);
                LOGGER.info("Find [Delete] file {}", currentDeleteFile);
            }
        }
        // 如果文件删除，则清理元数据
        if (deleteTrackedFile.size() > 0)
        {
            checkpoints.deleteCheckpointByTrackedFileList(deleteTrackedFile);
        }

        this.currentSnapshot = newSnapshot;
        this.pendingFiles = new TrackedFileList(needUpdateFile);
        
        // 更新currentOpenFile
        if (currentOpenFile != null)
        {
            if (sameAsCurrentOpenFile == null
                    || sameAsCurrentOpenFile.getLastOffset() == 0 && currentOpenFile.getLastOffset() > 0)
            {
                // 当前文件丢失或变小，重置
                LOGGER.info("Close currentOpenFile [{}] due to [{}]",
                        currentOpenFile,
                        sameAsCurrentOpenFile == null ? "DELETE" : "Shrink");
                closeCurrentFileIfOpen();
                currentOpenFile = null;
                currentOpenFileIndex = -1;
            }
            else
            {
                continueTailingCurrentFile(sameAsCurrentOpenFile);
            }
        }
        
        lastRefreshTimestamp = System.currentTimeMillis();
        LOGGER.debug("End to refreshTrackFileList");
        return pendingFiles;
    }
    
    public boolean mustRefreshSnapshot()
    {
        try
        {
            // Some files appeared/disappeared
            if (currentSnapshot == null || sourceFile.countFiles() != currentSnapshot.size())
            {
                return true;
            }
            
            for (TrackedFile trackedFile : currentSnapshot)
            {
                if (!Files.exists(trackedFile.getPath()))
                {
                    LOGGER.debug("Current file {} does not exist anymore. Must refresh.", trackedFile.getPath());
                    return true;
                }
                
                FileId newId = FileId.get(trackedFile.getPath());
                
                if (!trackedFile.getId().equals(newId))
                {
                    LOGGER.debug("ID for trackedFile file ({}) changed from {} to {}. Must refresh.",
                        trackedFile.getPath(),
                        trackedFile.getId(),
                        newId);
                    return true;
                }
                
                long size = Files.size(trackedFile.getPath());
                if (size < trackedFile.getSize() || (size > trackedFile.getSize() && trackedFile != currentOpenFile))
                {
                    LOGGER.debug("TrackedFile ({}) size change from {} to {}. Must refresh.",
                        trackedFile.getPath(),
                        trackedFile.getSize(),
                        size);
                    return true;
                }
            }
        }
        catch (IOException e)
        {
            LOGGER.warn("Error while status of current file snapshot, must refresh. ErrorMsg [{}]", e.toString());
            return true;
        }
        return false;
    }
    
    public TrackedFileList refreshSmallFileTrackFileList()
        throws IOException
    {
        LOGGER.debug("Start to refreshTrackFileList");
        TrackedFileList newSnapshot = sourceFile.listFiles();
        // Preconditions.checkNotNull(currentSnapshot);
        
        // Initialize the analysis and log any anomalies
        TrackedFileRotationAnalyzer analyzer = new TrackedFileRotationAnalyzer(currentSnapshot, newSnapshot, null);
        
        List<TrackedFile> needUploadFile = new ArrayList<>();
        for (TrackedFile newTrackFile : newSnapshot)
        {
            TrackedFile oldTrackFile = analyzer.getCounterpart(newTrackFile);
            
            if ((oldTrackFile == null || oldTrackFile.getLastOffset() == -1)
                && isFileOpen(newTrackFile.getPath().toAbsolutePath()))
            {
                LOGGER.info("Find [Opened] file [{}], will ignore this file.", newTrackFile);
                if (oldTrackFile != null)
                {
                    newTrackFile.setLastOffset(oldTrackFile.getLastOffset());
                    newTrackFile.setLastCheckTime(oldTrackFile.getLastCheckTime());
                }
                else
                {
                    newTrackFile.setLastOffset(-1);
                    newTrackFile.setLastCheckTime(-1);
                }
                continue;
            }
            
            if (oldTrackFile == null || oldTrackFile.getLastCheckTime() == -1)
            {
                // 新增文件
                newTrackFile.setLastCheckTime(System.currentTimeMillis());
                newTrackFile.setLastOffset(-1);
                LOGGER.info("Find [New] file [{}].", newTrackFile);
                continue;
            }
            
            if (oldTrackFile.size == newTrackFile.size && oldTrackFile.getLastOffset() == -1
                && oldTrackFile.getLastModifiedTime() == newTrackFile.getLastModifiedTime()
                && (System.currentTimeMillis() - oldTrackFile.getLastCheckTime()) > flow.getMaxFileCheckingMillis())
            {
                // 文件小大在指定时间内不变，且没有更新，且文件没有上传过，则表示已经完成，准备上传
                newTrackFile.setLastOffset(0);
                newTrackFile.setLastCheckTime(oldTrackFile.getLastCheckTime());
                if (flow.isIgnoreEmptyData() && newTrackFile.size == 0)
                {
                    LOGGER.info("Find [Empty] file [{}], will not be uploaded.", newTrackFile);
                }
                else
                {
                    needUploadFile.add(newTrackFile);
                    LOGGER.info("File [{}] will be uploaded.", newTrackFile);
                }
            }
            else if (oldTrackFile.size != newTrackFile.size
                || oldTrackFile.getLastModifiedTime() != newTrackFile.getLastModifiedTime())
            {
                // 文件变动，重新计算时间
                newTrackFile.setLastCheckTime(System.currentTimeMillis());
                newTrackFile.setLastOffset(-1);
                LOGGER.info("Find [ReSize] file. \n\tThe Current  file [{}]\n\tThe Previous file [{}]",
                    newTrackFile,
                    oldTrackFile);
            }
            else
            {
                // 文件没有变动，继承以前上传的位置
                newTrackFile.setLastOffset(oldTrackFile.getLastOffset());
                newTrackFile.setLastCheckTime(oldTrackFile.getLastCheckTime());
            }
        }
        
        this.currentSnapshot = newSnapshot;
        this.pendingFiles = new TrackedFileList(needUploadFile);
        
        lastRefreshTimestamp = System.currentTimeMillis();
        LOGGER.debug("End to refreshTrackFileList");
        return pendingFiles;
    }
    
    private boolean isFileOpen(Path path)
        throws IOException
    {
        File file = new File(path.toString());
        if (SystemUtils.IS_OS_WINDOWS)
        {
            if (file.exists() && file.renameTo(file))
            {
                return false;
            }
        }
        else
        {
            // linux上调用lsof命令判断会影响性能，默认文件未打开
            return false;
        }
        return true;
    }
}