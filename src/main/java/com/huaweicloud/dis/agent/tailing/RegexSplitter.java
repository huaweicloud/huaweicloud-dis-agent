package com.huaweicloud.dis.agent.tailing;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.huaweicloud.dis.agent.Constants;

/**
 * Returns one record that splits records based on a regex that matches the beginning of a record
 */
public class RegexSplitter implements ISplitter
{
    public final Pattern startingPattern;
    
    public final Charset charset;
    
    public RegexSplitter(String startingPattern)
    {
        this(startingPattern, StandardCharsets.UTF_8);
    }
    
    public RegexSplitter(String startingPattern, Charset fileEncoding)
    {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(startingPattern));
        this.startingPattern = Pattern.compile(startingPattern);
        this.charset = fileEncoding;
    }
    
    public String getPattern()
    {
        return startingPattern.pattern();
    }
    
    @Override
    public int locateNextRecord(ByteBuffer buffer)
    {
        return advanceBufferToNextPattern(buffer);
    }
    
    /**
     * Advances the buffer current position to be at the index at the beginning of the next pattern, or else the end of
     * the buffer if there is no final pattern.
     *
     * @return {@code position} of the buffer at the starting index of the next new pattern; {@code -1} if the end of
     *         the buffer was reached.
     */
    private int advanceBufferToNextPattern(ByteBuffer buffer)
    {
        Matcher matcher;
        // this marks the position before which we already attempted to match the pattern
        int currentLookedPosition = buffer.position();
        boolean firstLine = true;
        while (buffer.hasRemaining())
        {
            // start matching from the current position on a line by line basis
            if (buffer.get() == Constants.NEW_LINE)
            {
                // Skip the first line as it must be part of the current record
                if (!firstLine)
                {
                    String line = new String(buffer.array(), currentLookedPosition,
                        buffer.position() - currentLookedPosition, StandardCharsets.UTF_8);
                    matcher = startingPattern.matcher(line);
                    if (matcher.lookingAt())
                    {
                        buffer.position(currentLookedPosition);
                        return currentLookedPosition;
                    }
                }
                firstLine = false;
                // update the position that we already looked at
                currentLookedPosition = buffer.position();
            }
        }
        
        // We've scanned to the end and there is only one complete record in the buffer, set the position to the end
        if (!firstLine && buffer.limit() < buffer.capacity())
        {
            return buffer.position();
        }
        
        return -1;
    }
}
