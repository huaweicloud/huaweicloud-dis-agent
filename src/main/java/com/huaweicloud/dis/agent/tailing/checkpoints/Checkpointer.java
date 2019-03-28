package com.huaweicloud.dis.agent.tailing.checkpoints;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.huaweicloud.dis.agent.tailing.FileFlow;
import com.huaweicloud.dis.agent.tailing.IRecord;
import com.huaweicloud.dis.agent.tailing.RecordBuffer;

import lombok.Getter;

/**
 * Class that manages checkpoint updates per {@link FileFlow flow}. Current implementation only makes sure that for a
 * given {@link FileFlow flow} an older checkpoint arriving late does not overwrite a newer checkpoint.
 * <p>
 * This is not ideal because it could potentially create gaps and lead to data loss, but in normal cases this should
 * happen only rarely.
 * <p>
 * TODO: A future iteration could ensure that checkpoints are created in sequence such that no gaps are left. This
 * entails waiting for late-arriving buffers, and could lead to data duplication but guarantees no data loss.
 *
 * @param <R>
 */
public class Checkpointer<R extends IRecord>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Checkpointer.class);
    
    @Getter
    private final FileCheckpointStore store;
    
    @Getter
    private final FileFlow<R> flow;
    
    private long committed = -1;
    
    public Checkpointer(FileFlow<R> flow, FileCheckpointStore store)
    {
        this.flow = flow;
        this.store = store;
    }
    
    /**
     * @param buffer The buffer that was sent and that triggered the checkpoint update.
     * @return The checkpoint that was created, or {@code null} if the buffer would overwrite a previous checkpoint.
     */
    public synchronized FileCheckpoint saveCheckpoint(RecordBuffer<?> buffer)
    {
        // SANITYCHECK: Can remove when done with debugging
        Preconditions.checkArgument(buffer.id() != committed);
        // Only store the checkpoint if it has an increasing sequence number
        if (buffer.id() > committed)
        {
            committed = buffer.id();
            return store.saveCheckpoint(buffer.checkpointFile(), buffer.checkpointOffset());
        }
        else
        {
            LOGGER.trace("Buffer {} has lower sequence number than the last committed value {}. "
                + "No checkpoints will be updated.", buffer, committed);
            // TODO: Add metrics?
            return null;
        }
    }
    
    public boolean saveCheckpoint(List<IRecord> records)
    {
        List<FileCheckpoint> fileCheckpoints = new ArrayList<>(records.size());
        for (IRecord record : records)
        {
            fileCheckpoints.add(new FileCheckpoint(record.file(), record.endOffset()));
        }
        return store.saveCheckpointBatchAfterSend(fileCheckpoints);
    }
}
