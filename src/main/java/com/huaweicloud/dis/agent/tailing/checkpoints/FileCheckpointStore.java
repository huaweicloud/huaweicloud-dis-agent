package com.huaweicloud.dis.agent.tailing.checkpoints;

import java.util.List;
import java.util.Map;

import com.huaweicloud.dis.agent.tailing.FileFlow;
import com.huaweicloud.dis.agent.tailing.TrackedFile;

/**
 * Base interface for a checkpoing store to track progress of file tailers.
 */
public interface FileCheckpointStore
{
    
    /**
     * 更新一个文件的checkpoint信息
     * 
     * @param file
     * @param offset
     * @return
     */
    public FileCheckpoint saveCheckpoint(TrackedFile file, long offset);
    
    /**
     * 发送之后批量更新文件的checkpoint(offset只增加)
     * 
     * @param fileCheckpoints
     * @return
     */
    public boolean saveCheckpointBatchAfterSend(List<FileCheckpoint> fileCheckpoints);
    
    /**
     * @param flow
     * @return The latest checkpoint for the given flow if any, or {@code null} if the flow has no checkpoints in the
     *         store.
     */
    public FileCheckpoint getCheckpointForFlow(FileFlow<?> flow);
    
    /**
     * Cleans up any resources used up by this store.
     */
    public void close();
    
    public List<Map<String, Object>> dumpCheckpoints();
    
    public List<TrackedFile> getAllCheckpointForFlow(FileFlow<?> flow);
    
    public void deleteCheckpointByTrackedFileList(List<TrackedFile> trackedFileList);
    
    public long getOffsetForFileID(FileFlow<?> flow, String fileID);
}
