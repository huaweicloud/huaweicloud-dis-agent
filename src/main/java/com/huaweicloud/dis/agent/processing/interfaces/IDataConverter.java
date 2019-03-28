package com.huaweicloud.dis.agent.processing.interfaces;

import java.nio.ByteBuffer;

import com.huaweicloud.dis.agent.processing.exceptions.DataConversionException;

/**
 * Created by myltik on 12/01/2016.
 */
public interface IDataConverter
{
    
    public static final String NEW_LINE = "\n";
    
    /**
     * Convert data from source to any other format
     *
     * @param data Source data
     * @return byte array of processed data
     * @throws DataConversionException
     */
    public ByteBuffer convert(ByteBuffer data)
        throws DataConversionException;
}
