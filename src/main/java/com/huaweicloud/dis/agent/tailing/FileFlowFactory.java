package com.huaweicloud.dis.agent.tailing;

import com.huaweicloud.dis.agent.AgentContext;
import com.huaweicloud.dis.agent.config.Configuration;
import com.huaweicloud.dis.agent.config.ConfigurationException;

public class FileFlowFactory
{
    /**
     * @param config
     * @return
     * @throws ConfigurationException If the configuration does not correspond to a known {@link FileFlow} type.
     */
    public FileFlow<?> getFileFlow(AgentContext context, Configuration config)
        throws ConfigurationException
    {
        if (config.containsKey(DISConstants.DESTINATION_KEY))
        {
            return getDISFileflow(context, config);
        }
        else if (config.containsKey(SmallFileConstants.DESTINATION_KEY))
        {
            return getSmallFileflow(context, config);
        }
        else if (config.containsKey(OBSConstants.DESTINATION_KEY))
        {
            return getOBSFileflow(context, config);
        }
        throw new ConfigurationException(
            "Could not create flow from the given configuration. Could not recognize flow type.");
    }
    
    protected DISFileFlow getDISFileflow(AgentContext context, Configuration config)
    {
        return new DISFileFlow(context, config);
    }
    
    protected SmallFileFlow getSmallFileflow(AgentContext context, Configuration config)
    {
        return new SmallFileFlow(context, config);
    }
    
    protected OBSFileFlow getOBSFileflow(AgentContext context, Configuration config)
    {
        return new OBSFileFlow(context, config);
    }
    
}
