package org.iq80.twoLayerLog.impl;

public interface LogMonitor
{
    void corruption(long bytes, String reason);

    void corruption(long bytes, Throwable reason);
}
