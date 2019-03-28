package com.huaweicloud.dis.agent.tailing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.UUID;

import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Abstraction of a file identifier (independent from path). Current implementation relies on
 * {@link BasicFileAttributes#fileKey()}.
 */
@EqualsAndHashCode
public class FileId
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileId.class);
    
    public static final String AGENT_FILE_ID = "agent_file_id";
    
    public static final String AGENT_FILE_ID_SPLIT_CHAR = "_";
    
    @Getter
    private final String id;
    
    /**
     * @see #get(BasicFileAttributes)
     */
    public static FileId get(Path file)
        throws IOException
    {
        if (!Files.exists(file))
        {
            return null;
        }
        Preconditions.checkArgument(Files.isRegularFile(file),
            "Can only get ID for real files (no directories and symlinks): " + file);
        
        // Windows下，生成一个iNode(UUID)写入到UserDefinedFileAttributeView中
        if (SystemUtils.IS_OS_WINDOWS)
        {
            // 文件创建时间
            String createTimeMillis = Long.toString(Files.getFileAttributeView(file, BasicFileAttributeView.class)
                .readAttributes()
                .creationTime()
                .toMillis());
            // 获取自定义属性
            UserDefinedFileAttributeView view = Files.getFileAttributeView(file, UserDefinedFileAttributeView.class);
            ByteBuffer buf = ByteBuffer.allocate(50);
            String fileId = null;
            String oldFileId = null;
            boolean needUpdateFileId = false;
            try
            {
                // 获取file_id
                view.read(AGENT_FILE_ID, buf);
                buf.flip();
                fileId = Charset.forName("UTF-8").decode(buf).toString();
                
                if (!fileId.substring(fileId.lastIndexOf(AGENT_FILE_ID_SPLIT_CHAR) + 1).equals(createTimeMillis))
                {
                    oldFileId = fileId;
                    needUpdateFileId = true;
                }
            }
            catch (NoSuchFileException ignored)
            {
                // No agent_file_id, need to create
                needUpdateFileId = true;
            }
            if (needUpdateFileId)
            {
                // 文件修改时间
                FileTime lastModifiedTime = Files.getLastModifiedTime(file);
                // 写入file_id
                fileId = UUID.randomUUID().toString() + AGENT_FILE_ID_SPLIT_CHAR + createTimeMillis;
                view.write(AGENT_FILE_ID, Charset.forName("UTF-8").encode(fileId));
                Files.setLastModifiedTime(file, lastModifiedTime);
                LOGGER.info("File [{}] id changes from [{}] to [{}].", file, oldFileId, fileId);
            }
            
            return new FileId(fileId);
        }
        
        BasicFileAttributes attr = Files.readAttributes(file, BasicFileAttributes.class);
        if (attr == null || attr.fileKey() == null)
        {
            LOGGER.error(
                "Cloud not readAttributes of File [{}]! Use filename as FileId, if the file name changes, "
                    + "the file will be treated as a new file, so suggest run this on linux or windows platform.",
                file.toAbsolutePath().toString());
            return new FileId(file.getFileName().toString());
        }
        return get(attr);
    }
    
    /**
     * TODO: this might not be portable as we rely on inner representation of the {@link BasicFileAttributes#fileKey()}
     * ({@linkplain UnixFileKey
     * http://grepcode.com/file/repository.grepcode.com/java/root/jdk/openjdk/7u40-b43/sun/nio/fs/UnixFileKey.java} on
     * Linux), which is not defined canonically anywhere.
     *
     * @param attr
     * @return
     * @throws IOException
     */
    public static FileId get(BasicFileAttributes attr)
        throws IOException
    {
        return new FileId(attr.fileKey().toString());
    }
    
    public FileId(String id)
    {
        Preconditions.checkNotNull(id);
        this.id = id;
    }
    
    @Override
    public String toString()
    {
        return this.id;
    }
}
