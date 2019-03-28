package com.huaweicloud.dis.agent.config;

import javax.annotation.Nullable;

import com.google.common.base.Function;

/**
 * Wraps a <code>String</code> -> <code>T</code> function into an <code>Object</code> -> T</code> function by calling
 * <code>toString</code> on the value.
 *
 * @param <T> the destination type.
 */
class StringConverterWrapper<T> implements Function<Object, T>
{
    private final Function<String, T> delegate;
    
    public StringConverterWrapper(Function<String, T> delegate)
    {
        this.delegate = delegate;
    }
    
    @Override
    @Nullable
    public T apply(@Nullable Object input)
    {
        return input != null ? this.delegate.apply(input.toString()) : null;
    }
}