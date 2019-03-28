package com.huaweicloud.dis.agent.tailing.checkpoints;

import com.google.common.base.Preconditions;
import com.huaweicloud.dis.agent.tailing.FileId;
import com.huaweicloud.dis.agent.tailing.TrackedFile;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * A checkpoint in a file. When doing equality comparison and computing hashcode, only the file ID and flow ID are
 * considered, making this class work with files that change in size (and potentially path).
 */
@ToString
@EqualsAndHashCode(exclude = {"file"})
public class FileCheckpoint
{
    @Getter
    private final TrackedFile file;
    
    @Getter
    private final long offset;
    
    @Getter
    private final FileId fileId;
    
    @Getter
    private final String flowId;
    
    public FileCheckpoint(TrackedFile file, long offset)
    {
        Preconditions.checkNotNull(file);
        Preconditions.checkArgument(offset >= 0, "The offset (%s) must be a non-negative integer", offset);
        this.file = file;
        this.offset = offset;
        fileId = file.getId();
        flowId = file.getFlow().getId();
    }
}
