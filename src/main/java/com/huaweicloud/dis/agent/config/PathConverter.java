package com.huaweicloud.dis.agent.config;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.Nullable;

import com.google.common.base.Function;

/**
 * Function that converts an object into a {@link Path}.
 */
class PathConverter implements Function<Object, Path>
{
    
    @Override
    @Nullable
    public Path apply(@Nullable Object input)
    {
        if (input != null && !(input instanceof Path))
        {
            if (input instanceof URI)
                return Paths.get((URI)input);
            else
                return Paths.get(input.toString());
        }
        else
            return (Path)input;
    }
}