package com.huaweicloud.dis.agent.tailing;

import java.nio.file.Path;
import java.util.*;

import com.google.common.base.Preconditions;

import lombok.Getter;

/**
 * A snapshot of a a collection of files on the filesystem, ordered by last modified time (newer-first) as would be
 * retrieved by {@link SourceFile#listFiles().
 */
public class TrackedFileList extends AbstractList<TrackedFile>
{
    
    public static TrackedFileList emptyList()
    {
        // return new TrackedFileList(Collections.<TrackedFile> emptyList());
        return new TrackedFileList(new ArrayList<>(0));
    }
    
    @Getter
    private final List<TrackedFile> snapshot;
    
    public TrackedFileList(List<TrackedFile> snapshot)
    {
        this.snapshot = new ArrayList<>(validate(snapshot));
    }
    
    @Override
    public TrackedFile get(int index)
    {
        return snapshot.get(index);
    }
    
    @Override
    public int size()
    {
        return snapshot.size();
    }
    
    @Override
    public TrackedFileList subList(int fromIndex, int toIndex)
    {
        return new TrackedFileList(snapshot.subList(fromIndex, toIndex));
    }
    
    public boolean addAll(List<TrackedFile> trackedFiles)
    {
        snapshot.addAll(trackedFiles);
        return true;
    }
    
    public boolean addSingle(TrackedFile trackedFile)
    {
        snapshot.add(trackedFile);
        return true;
    }
    
    public int indexOfPath(Path path)
    {
        int i = 0;
        for (TrackedFile f : snapshot)
        {
            if (path.equals(f.getPath()))
                return i;
            ++i;
        }
        return -1;
    }
    
    public int indexOfFileId(FileId id)
    {
        int i = 0;
        for (TrackedFile f : snapshot)
        {
            if (id.equals(f.getId()))
                return i;
            ++i;
        }
        return -1;
    }
    
    /**
     * Performs following validations:
     * <ol>
     * <li>{@code files} is not {@code null}</li>
     * <li>No two files have the same path</li>
     * <li>No two files have the same {@code FileId}</li>
     * <li>Files are ordered newer-first</li>
     * </ol>
     *
     * @param files
     * @return The input parameter, if all is valid.
     * @throws NullPointerException if {@code files} is {@code null}.
     * @throws IllegalArgumentException if any of the other conditions are found.
     */
    private List<TrackedFile> validate(List<TrackedFile> files)
    {
        Preconditions.checkNotNull(files);
        // Unique Path
        // Unique FileId
        Map<FileId, TrackedFile> seenIds = new HashMap<>();
        for (TrackedFile f : files)
        {
            Preconditions.checkArgument(!seenIds.containsKey(f.getId()),
                "File with path '" + f.getPath() + "' has an ID " + f.getId()
                    + " that shows up multiple times! the previous File " + seenIds.get(f.getId()));
            seenIds.put(f.getId(), f);
        }
        // Order by lastModifiedTime descending
        //long previousLastModifiedTime = -1;
        //for (TrackedFile f : files)
        //{
        //    if (previousLastModifiedTime >= 0)
        //    {
        //        Preconditions.checkArgument(f.getLastModifiedTime() <= previousLastModifiedTime,
        //            "File with path '" + f.getPath() + "' is older than previous file in the list.");
        //    }
        //    previousLastModifiedTime = f.getLastModifiedTime();
        //}
        
        // All good
        return files;
    }
    
}
