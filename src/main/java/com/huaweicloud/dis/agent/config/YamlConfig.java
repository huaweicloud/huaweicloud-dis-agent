package com.huaweicloud.dis.agent.config;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.huaweicloud.dis.agent.Constants;

public class YamlConfig implements Config
{
    private static Logger logger = LoggerFactory.getLogger(YamlConfig.class);
    
    @Override
    public Map parse(String filename)
    {
        try
        {
            Yaml yaml = new Yaml();
            if (filename.startsWith(Constants.HTTP_PREFIX) || filename.startsWith(Constants.HTTPS_PREFIX))
            {
                URL httpUrl;
                URLConnection connection;
                httpUrl = new URL(filename);
                connection = httpUrl.openConnection();
                connection.connect();
                return (Map)yaml.load(connection.getInputStream());
            }
            else
            {
                FileInputStream input = new FileInputStream(new File(filename));
                return (Map)yaml.load(input);
            }
        }
        catch (Exception e)
        {
            logger.error("load yaml config error", e);
            System.exit(1);
        }
        return null;
    }
}
