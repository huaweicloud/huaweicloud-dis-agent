package com.huaweicloud.dis.agent.processing.processors;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.huaweicloud.dis.agent.ByteBuffers;
import com.huaweicloud.dis.agent.config.Configuration;
import com.huaweicloud.dis.agent.processing.exceptions.DataConversionException;
import com.huaweicloud.dis.agent.processing.interfaces.IJSONPrinter;
import com.huaweicloud.dis.agent.processing.utils.ProcessingUtilsFactory;

/**
 * Convert a CSV record into JSON record.
 * <p>
 * customFieldNames is required. Optional delimiter other than comma can be configured. Optional jsonFormat can be used
 * for pretty printed json.
 * <p>
 * Configuration looks like: { "optionName": "CSVTOJSON", "customFieldNames": [ "field1", "field2", ... ], "delimiter":
 * "\\t" }
 */
public class CSVToJSONDataConverter extends BaseDataConverter
{
    
    private static String FIELDS_KEY = "customFieldNames";
    
    private static String DELIMITER_KEY = "delimiter";
    
    private final List<String> fieldNames;
    
    private final String delimiter;
    
    private final IJSONPrinter jsonProducer;
    
    public CSVToJSONDataConverter(Configuration config)
    {
        super(config);
        fieldNames = config.readList(FIELDS_KEY, String.class);
        delimiter = config.readString(DELIMITER_KEY, ",");
        jsonProducer = ProcessingUtilsFactory.getPrinter(config);
    }
    
    @Override
    public ByteBuffer convert(ByteBuffer data)
        throws DataConversionException
    {
        final Map<String, Object> recordMap = new LinkedHashMap<String, Object>();
        String dataStr = ByteBuffers.toString(data, charset);
        boolean hasLineBreak = false;
        // Preserve the NEW_LINE at the end of the JSON record
        if (dataStr.endsWith(NEW_LINE))
        {
            hasLineBreak = true;
            dataStr = dataStr.substring(0, (dataStr.length() - NEW_LINE.length()));
        }
        
        // String[] columns = dataStr.split(delimiter);
        // 拆分时保留空值情况
        String[] columns = StringUtils.splitPreserveAllTokens(dataStr, delimiter);
        for (int i = 0; i < fieldNames.size(); i++)
        {
            try
            {
                recordMap.put(fieldNames.get(i), columns[i]);
            }
            catch (ArrayIndexOutOfBoundsException e)
            {
                recordMap.put(fieldNames.get(i), null);
            }
            catch (Exception e)
            {
                throw new DataConversionException("Unable to create the column map", e);
            }
        }
        
        String dataJson = jsonProducer.writeAsString(recordMap);
        if (hasLineBreak)
        {
            dataJson += NEW_LINE;
        }
        return ByteBuffer.wrap(dataJson.getBytes(charset));
    }
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{ delimiter: [" + delimiter + "], " + "fields: " + fieldNames.toString()
            + "}";
    }
}
