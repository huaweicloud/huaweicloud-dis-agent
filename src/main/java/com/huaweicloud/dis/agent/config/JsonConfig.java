package com.huaweicloud.dis.agent.config;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonConfig implements Config
{
    private static Logger logger = LoggerFactory.getLogger(JsonConfig.class);
    
    @Override
    public Map parse(String filename)
    {
        try (InputStream is = new FileInputStream(filename))
        {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
            return mapper.readValue(is, new TypeReference<HashMap<String, Object>>()
            {
            });
        }
        catch (Exception e)
        {
            logger.error("load json config error", e);
            System.exit(1);
        }
        return null;
    }
}
