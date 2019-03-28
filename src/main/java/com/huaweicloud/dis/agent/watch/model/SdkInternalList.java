package com.huaweicloud.dis.agent.watch.model;

import java.util.ArrayList;
import java.util.Collection;

/**
 * List with auto construct flag to indicate whether it is auto constructed by Java SDK.
 */
public class SdkInternalList<T> extends ArrayList<T>
{
    
    private static final long serialVersionUID = 1L;
    
    /**
     * Auto construct flag to indicate whether the list is auto constructed by Java SDK.
     */
    private final boolean autoConstruct;
    
    public SdkInternalList()
    {
        super();
        autoConstruct = true;
    }
    
    public SdkInternalList(Collection<? extends T> c)
    {
        super(c);
        autoConstruct = false;
    }
    
    public SdkInternalList(int initialCapacity)
    {
        super(initialCapacity);
        autoConstruct = false;
    }
    
    /**
     * Return true if the list is auto constructed by Java SDK
     */
    public boolean isAutoConstruct()
    {
        return autoConstruct;
    }
    
}