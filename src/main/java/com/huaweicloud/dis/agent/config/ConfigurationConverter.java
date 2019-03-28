package com.huaweicloud.dis.agent.config;

import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Function;

/**
 * Function that converts an object into a {@link Configuration}.
 */
class ConfigurationConverter implements Function<Object, Configuration>
{
    
    @SuppressWarnings("unchecked")
    @Override
    @Nullable
    public Configuration apply(@Nullable Object input)
    {
        if (input != null && !(input instanceof Configuration))
        {
            if (input instanceof Map)
                return new Configuration((Map<String, Object>)input);
            else
                throw new ConfigurationException("Value is not a valid map: " + input.toString());
        }
        else
            return (Configuration)input;
    }
}