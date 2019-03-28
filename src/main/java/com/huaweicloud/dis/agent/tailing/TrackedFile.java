package com.huaweicloud.dis.agent.tailing;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * A class that encapsulates a snapshot of the state of a file at a point in time, along with an optional channel that
 * can be used to read from the file.
 */
@NotThreadSafe
@ToString(exclude = {"channel", "flow", "headerBytes", "isSameAsCurrentOpenFile", "sha256HeaderStr", "lastCheckTime"})
public class TrackedFile
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TrackedFile.class);
    
    @Getter
    protected final FileFlow<?> flow;
    
    @Getter
    protected final FileId id;
    
    @Getter
    protected final Path path;
    
    @Getter
    protected final long lastModifiedTime;
    
    @Getter
    protected final long size;
    
    @Getter
    protected FileChannel channel;
    
    @Getter
    @Setter
    protected long lastOffset = 0;
    
    @Getter
    @Setter
    protected byte[] headerBytes;
    
    @Getter
    @Setter
    protected int headerBytesLength;
    
    @Getter
    @Setter
    protected String sha256HeaderStr;
    
    @Getter
    @Setter
    protected boolean isSameAsCurrentOpenFile = false;
    
    @Getter
    @Setter
    protected Boolean isDeleting = false;
    
    @Getter
    @Setter
    protected long lastCheckTime;
    
    public TrackedFile(FileFlow<?> flow, Path path, FileId id, long lastModifiedTime, long size)
        throws IOException
    {
        this.flow = flow;
        this.path = path;
        this.id = id;
        this.channel = null;
        this.lastModifiedTime = lastModifiedTime;
        this.size = size;
        if (Files.exists(path))
        {
            this.headerBytes = new byte[flow.getHeaderBytesLength()];
            
            try (InputStream fi = Files.newInputStream(path))
            {
                this.headerBytesLength = fi.read(headerBytes);
            }
            
            if (headerBytesLength > 0)
            {
                byte[] headerBytesTemp = new byte[headerBytesLength];
                System.arraycopy(headerBytes, 0, headerBytesTemp, 0, headerBytesLength);
                this.headerBytes = headerBytesTemp;
            }
        }
        else
        {
            throw new FileNotFoundException("File " + path + " not found.");
        }
        FileId newId = FileId.get(path);
        if (!id.equals(newId))
        {
            throw new FileSystemException(
                path.toString() + " file id changed, new id is " + newId + ", old id is " + id);
        }
    }
    
    public TrackedFile(FileFlow<?> flow, Path path, FileId id, long lastModifiedTime, long size, long offset,
        int headerBytesLength, byte[] headerBytes, String sha256HeaderStr)
    {
        this.flow = flow;
        this.path = path;
        this.id = id;
        this.channel = null;
        this.lastModifiedTime = lastModifiedTime;
        this.size = size;
        this.lastOffset = offset;
        this.headerBytesLength = headerBytesLength;
        this.headerBytes = headerBytes;
        this.sha256HeaderStr = sha256HeaderStr;
    }
    
    public TrackedFile(FileFlow<?> flow, Path path)
        throws IOException
    {
        this(flow, path, FileId.get(path), Files.getLastModifiedTime(path).toMillis(), Files.size(path));
    }
    
    protected TrackedFile(TrackedFile original)
    {
        this(original.flow, original.path, original.id, original.lastModifiedTime, original.size, original.lastOffset,
            original.headerBytesLength, original.headerBytes, original.sha256HeaderStr);
    }
    
    public void inheritChannel(TrackedFile oldOpenFile)
        throws IOException
    {
        Preconditions.checkState(oldOpenFile.channel != null, "Existing file does not have an open channel.");
        Preconditions.checkState(channel == null, "This file has an open channel already.");
        Preconditions.checkState(oldOpenFile.id.equals(id),
            "File ID differ (old: %s, new: %s)",
            id.toString(),
            oldOpenFile.id.toString());
        channel = oldOpenFile.channel;
    }
    
    public long getCurrentOffset()
        throws IOException
    {
        // return channel == null ? 0 : channel.position();
        if (channel != null)
        {
            lastOffset = channel.position();
        }
        return lastOffset;
    }
    
    public long getCurrentSize()
        throws IOException
    {
        return channel == null ? (Files.exists(path) ? Files.size(path) : 0) : channel.size();
    }
    
    public long getRemainingBytes()
        throws IOException
    {
        return getSize() - getCurrentOffset();
    }
    
    public void open(long offset)
        throws IOException
    {
        Preconditions.checkState(channel == null, "File already open.");
        Preconditions.checkArgument(offset >= 0, "Offset must be a non-negative number.");
        channel = FileChannel.open(path, StandardOpenOption.READ);
        if (offset > 0)
        {
            channel.position(offset);
        }
    }
    
    /**
     * Closes the channel that's open to this file.
     */
    public void close()
    {
        Preconditions.checkState(channel != null, "File not open.");
        try
        {
            // 保存offset
            lastOffset = channel.position();
            if (channel.isOpen())
                channel.close();
        }
        catch (IOException e)
        {
            LOGGER.error("Failed to closed file channel for {}", path, e);
        }
        finally
        {
            channel = null;
        }
    }
    
    /**
     * @return {@code true} if there's an open channel to this file, {@code false} otherwise.
     */
    public boolean isOpen()
    {
        return channel != null && channel.isOpen();
    }
    
    /**
     * @param f
     * @return {@code true} if the two files have the same ID, and {@code false} otherwise.
     */
    public boolean isSameAs(TrackedFile f)
    {
        return id.equals(f.id);
    }
    
    /**
     * @param other The file to compare to.
     * @return @see #isNewer(long, long, long, long)
     */
    public boolean isNewer(TrackedFile other)
    {
        return isNewer(this, other);
    }
    
    /**
     * @param tsOther The timestamp of the file to compare to.
     * @param sizeOther The size of the file to compare to.
     * @return @see #isNewer(long, long, long, long)
     */
    public boolean isNewer(long tsOther, long sizeOther)
    {
        return isNewer(this, tsOther, sizeOther);
    }
    
    /**
     * @return A shorter representation than {@link #toString()}
     */
    public String toShortString()
    {
        return path + ":" + id + ":" + size + ":" + lastModifiedTime;
    }
    
    /**
     * @param f1 The first file to compare.
     * @param f2 The second file to compare.
     * @return @see #isNewer(long, long, long, long)
     * @see #isNewer(long, long, long, long)
     */
    public static boolean isNewer(TrackedFile f1, TrackedFile f2)
    {
        // return isNewer(f1.lastModifiedTime, f1.size, f2.lastModifiedTime, f2.size);
        return isNewer(f1.lastModifiedTime,
            f1.getPath().getFileName().toString(),
            f2.lastModifiedTime,
            f2.getPath().getFileName().toString());
    }
    
    /**
     * @param f1 The first file to compare.
     * @param ts2 The timestamp of the second file.
     * @param size2 The size of the second file.
     * @return @see #isNewer(long, long, long, long)
     * @see #isNewer(long, long, long, long)
     */
    public static boolean isNewer(TrackedFile f1, long ts2, long size2)
    {
        return isNewer(f1.lastModifiedTime, f1.size, ts2, size2);
    }
    
    /**
     * @param p1 The first file to compare.
     * @param p2 The second file to compare.
     * @return @see #isNewer(long, long, long, long)
     * @throws IOException
     * @see #isNewer(long, long, long, long)
     */
    public static boolean isNewer(Path p1, Path p2)
        throws IOException
    {
        return isNewer(p1, Files.getLastModifiedTime(p2).toMillis(), Files.size(p2));
    }
    
    /**
     * @param p1 The path of the file to compare.
     * @param ts2 The timestamp of the second file.
     * @param size2 The size of the second file.
     * @return @see #isNewer(long, long, long, long)
     * @throws IOException
     * @see #isNewer(long, long, long, long)
     */
    public static boolean isNewer(Path p1, long ts2, long size2)
        throws IOException
    {
        return isNewer(Files.getLastModifiedTime(p1).toMillis(), Files.size(p1), ts2, size2);
    }
    
    /**
     * Determines if a file is newer than another by comparing the timestamps and sizes. This heuristic is consisten
     * with the behavior of rotating log files: at the instant the file is rotated there will likely exist two files
     * with the same timestamp; in that case, knowing nothing else about the files, the old file is almost certainly the
     * one which has more data, since the newer file has just been created (or truncated) and would not have enough data
     * yet. TODO: The edge case where the new file fills so quickly (within 1 second) to become as large as the old file
     * will escape this heuristic. This is *exceedingly rare* to happen in reality however, and will soon rectify itself
     * (within 1 second) since the timestamp of the file that's being written to will change, and the sizes won't matter
     * anymore.
     *
     * @param ts1 Timestamp of first file.
     * @param size1 Size of first file.
     * @param ts2 Timestamp of second file.
     * @param size2 Size of second file.
     * @return {@code true} if the first file is newer than the second file ({@code ts1 &gt; ts2}); if the two files
     *         have the same timestamp, then {@code true} is returned if the first file's size is smaller than the
     *         second's; otherwise, {@code false} is returned.
     */
    public static boolean isNewer(long ts1, long size1, long ts2, long size2)
    {
        return ts1 > ts2 || (ts1 == ts2 && size1 < size2);
    }
    
    public static boolean isNewer(long ts1, String name1, long ts2, String name2)
    {
        return ts1 > ts2 || (ts1 == ts2 && compareFileName(name1, name2) > 0);
    }
    
    /**
     * A comparison operator that uses the heuristic defined in {@link TrackedFile#isNewer(long, long, long, long)} to
     * comapre two {@link TrackedFile} instances and order them in descending order of recency (oldest first).
     */
    public static class NewestFirstComparator implements Comparator<TrackedFile>
    {
        @Override
        public int compare(TrackedFile f1, TrackedFile f2)
        {
            return f1.isSameAs(f2) ? 0 : (isNewer(f1, f2) ? -1 : 1);
        }
    }

    public static class OldestFirstComparator implements Comparator<TrackedFile>
    {
        @Override
        public int compare(TrackedFile f1, TrackedFile f2)
        {
            return f1.isSameAs(f2) ? 0 : (isNewer(f1, f2) ? 1 : -1);
        }
    }
    
    public static int compareFileName(String s1, String s2)
    {
        // 将相同的字符过滤
        int len1 = s1.length();
        int len2 = s2.length();
        int i;
        char c1, c2;
        for (i = 0, c1 = 0, c2 = 0; (i < len1) && (i < len2) && (c1 = s1.charAt(i)) == (c2 = s2.charAt(i)); i++)
            ;
        
        // 已经到字符串结尾
        if (c1 == c2)
            return (len1 - len2);
        
        // 从s1的第一个数字开始验证
        if (Character.isDigit(c1))
        {
            if (!Character.isDigit(c2))
                return (1);
            
            // 从s2的第一个数字开始，扫描所有的数字
            int x1, x2;
            for (x1 = i + 1; (x1 < len1) && Character.isDigit(s1.charAt(x1)); x1++)
                ;
            for (x2 = i + 1; (x2 < len2) && Character.isDigit(s2.charAt(x2)); x2++)
                ;
            
            // 比较数字
            return (x2 == x1 ? c1 - c2 : x1 - x2);
        }
        
        // 过滤后，s1没有找到第一个数字，如果s2第一个是数字则认为s2大
        if (Character.isDigit(c2))
            return (-1);
        
        // 没有数字直接比较字符
        return (c1 - c2);
    }
    
    /**
     * 比较两个文件的开头部分是否一致(采取比较两个文件的头部字节，或者比较头部字节的sha256码)
     * 
     * @param f
     * @return
     */
    public boolean isStartingSameAs(TrackedFile f)
    {
        int length1 = this.headerBytesLength;
        int length2 = f.headerBytesLength;
        byte[] bytes1 = this.headerBytes;
        byte[] bytes2 = f.headerBytes;
        String sha256HeaderStr1 = this.sha256HeaderStr;
        String sha256HeaderStr2 = f.sha256HeaderStr;
        
        if (length1 > 0 && length2 > 0)
        {
            int minLength = Math.min(length1, length2);
            if (bytes1 != null && bytes2 != null)
            {
                for (int i = 0; i < minLength; i++)
                {
                    if (bytes1[i] != bytes2[i])
                    {
                        return false;
                    }
                }
            }
            else if (bytes2 == null && sha256HeaderStr2 != null && bytes1 != null && length2 <= length1)
            {
                // file2是从数据库恢复的记录，使用sha256码比较
                byte[] file1HeaderBytes = new byte[length2];
                System.arraycopy(bytes1, 0, file1HeaderBytes, 0, length2);
                if (!sha256HeaderStr2.equals(DigestUtils.sha256Hex(file1HeaderBytes)))
                {
                    return false;
                }
            }
            else if (bytes1 == null && sha256HeaderStr1 != null && bytes2 != null && length1 <= length2)
            {
                // file1是从数据库恢复的记录，使用sha256码比较
                byte[] file2HeaderBytes = new byte[length1];
                System.arraycopy(bytes2, 0, file2HeaderBytes, 0, length1);
                if (!sha256HeaderStr1.equals(DigestUtils.sha256Hex(file2HeaderBytes)))
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }
        return true;
    }
}
