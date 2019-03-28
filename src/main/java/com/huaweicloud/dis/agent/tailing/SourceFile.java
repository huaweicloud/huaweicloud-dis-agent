package com.huaweicloud.dis.agent.tailing;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.huaweicloud.dis.core.util.StringUtils;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Specification of the file(s) to be tailed.
 */
@EqualsAndHashCode(exclude = {"pathMatcher"})
public class SourceFile
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceFile.class);
    
    @Getter
    private final FileFlow<?> flow;
    
    @Getter
    private final Path directory;
    
    @Getter
    private final Path filePattern;
    
    private final PathMatcher pathMatcher;
    
    @Getter
    private final String filePatternStr;
    
    private final List<String> ignoredExtensions;
    
    private final Comparator<TrackedFile> comparator;
    
    public SourceFile(FileFlow<?> flow, String filePattern)
    {
        if (filePattern.endsWith(File.separator))
        {
            filePattern += "*";
        }
        
        if (flow instanceof DISFileFlow)
        {
            ignoredExtensions = ImmutableList.of(".gz", ".bz2", ".zip", ".tar");
        }
        else
        {
            ignoredExtensions = Collections.emptyList();
        }
        this.flow = flow;
        File file = new File(filePattern);
        Preconditions.checkArgument(file.getParent() != null, "Please check FilePattern " + filePattern);
        this.directory = FileSystems.getDefault().getPath(file.getParent());
        validateDirectory(this.directory);
        this.filePattern = directory;
        this.filePatternStr = file.getName();
        this.pathMatcher = FileSystems.getDefault().getPathMatcher("glob:" + filePatternStr);
        
        if (flow.getFileComparator() == FileFlow.FileComparatorEnum.NEWEST_FIRST)
        {
            // sort the files by decsending last modified time and return
            this.comparator = new TrackedFile.NewestFirstComparator();
        }
        else
        {
            this.comparator = new TrackedFile.OldestFirstComparator();
        }
    }
    
    /**
     * @return List of {@link Path} objects contained in the given directory and that match the file name pattern,
     *         sorted by {@code lastModifiedTime} descending (newest at the top). An empty list is returned if
     *         {@link #directory} does not exist, or if there are no files that match the pattern.
     * @throws IOException If there was an error reading the directory or getting the {@code lastModifiedTime} of a
     *             directory. Note that if the {@link #directory} doesn't exist no exception will be thrown but an empty
     *             list is returned instead.
     */
    public TrackedFileList listFiles()
        throws IOException
    {
        if (!Files.exists(this.directory))
            return TrackedFileList.emptyList();
        
        List<TrackedFile> files = new ArrayList<>();
        
        if (flow.isDirectoryRecursionEnabled())
        {
            // 递归目录
            Files.walkFileTree(this.directory, new FindJavaVisitor(files));
        }
        else
        {
            // 只查找当前目录
            try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(this.directory))
            {
                for (Path p : directoryStream)
                {
                    addTrackedFile(p, files);
                }
            }
        }
        
        files.sort(this.comparator);
        
        return new TrackedFileList(files);
    }

    private class FindJavaVisitor extends SimpleFileVisitor<Path>
    {
        private List<TrackedFile> files;
        
        public FindJavaVisitor(List<TrackedFile> files)
        {
            this.files = files;
        }
        
        @Override
        public FileVisitResult visitFile(Path p, BasicFileAttributes attrs)
        {
            addTrackedFile(p, files);
            return FileVisitResult.CONTINUE;
        }
    }
    
    /**
     * @return The number of files on the file system that match the given input pattern. More lightweight than
     *         {@link #listFiles()}.
     * @throws IOException If there was an error reading the directory or getting the {@code lastModifiedTime} of a
     *             directory. Note that if the {@link #directory} doesn't exist no exception will be thrown but
     *             {@code 0} is returned instead.
     */
    public int countFiles()
        throws IOException
    {
        int count = 0;
        if (Files.exists(this.directory))
        {
            try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(this.directory))
            {
                for (Path p : directoryStream)
                {
                    if (validateFile(p))
                    {
                        ++count;
                    }
                }
            }
        }
        return count;
    }
    
    @Override
    public String toString()
    {
        // return this.directory + "/" + this.filePattern;
        // Add by liuhao
        return this.directory + File.separator + this.filePatternStr;
    }
    
    /**
     * Performs basic validation on the directory parameter making sure it fits within the supported functionality of
     * this class.
     *
     * @param dir
     */
    private void validateDirectory(Path dir)
    {
        Preconditions.checkArgument(dir != null, "Directory component is empty!");
        // TODO: validate that the directory component has no glob characters
    }
    
    /**
     * Make sure to ignore invalid file for streaming e.g. well known compressed file extensions
     *
     * @param file
     */
    private boolean validateFile(Path file)
    {
        String fileName = file.getFileName().toString();
        
        // 不采集目录，名称不匹配的，已重命名的，隐藏的文件
        if (Files.isRegularFile(file) && pathMatcher.matches(file.getFileName())
            && (StringUtils.isNullOrEmpty(flow.getFileSuffix()) || !fileName.endsWith(flow.getFileSuffix()))
            && !fileName.startsWith("."))
        {
            // 去除不合法的文件
            for (String extension : ignoredExtensions)
            {
                if (fileName.toLowerCase().endsWith(extension))
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    
    private boolean addTrackedFile(Path file, List<TrackedFile> trackedFiles)
    {
        if (validateFile(file))
        {
            try
            {
                trackedFiles.add(new TrackedFile(flow, file));
                return true;
            }
            catch (Exception e)
            {
                LOGGER.warn("Skip to track file [{}], error [{}], may be it should be remove.",
                    file.toAbsolutePath().toString(),
                    e.toString());
            }
        }
        return false;
    }
}
