package com.huaweicloud.dis.agent;

import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.AbstractScheduledService;

import lombok.Getter;

public abstract class HeartbeatService extends AbstractScheduledService
{
    private final long period;
    
    private final TimeUnit periodUnit;
    
    private final AgentContext agent;
    
    @Getter
    private Object lastResult;
    
    public HeartbeatService(AgentContext agent, long period, TimeUnit periodUnit)
    {
        super();
        this.period = period;
        this.periodUnit = periodUnit;
        this.agent = agent;
    }
    
    @Override
    protected void runOneIteration()
        throws Exception
    {
        lastResult = heartbeat(agent);
    }
    
    @Override
    protected Scheduler scheduler()
    {
        return Scheduler.newFixedRateSchedule(period, period, periodUnit);
    }
    
    protected abstract Object heartbeat(AgentContext agent);
}
