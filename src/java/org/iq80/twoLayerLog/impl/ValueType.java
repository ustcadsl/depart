package org.iq80.twoLayerLog.impl;

public enum ValueType
{
    DELETION(0x00),
    VALUE(0x01);

    public static ValueType getValueTypeByPersistentId(int persistentId)
    {
        switch (persistentId) {
            case 0:
                return DELETION;
            case 1:
                return VALUE;
            default:
                throw new IllegalArgumentException("Unknown persistentId " + persistentId);
        }
    }

    private final int persistentId;

    ValueType(int persistentId)
    {
        this.persistentId = persistentId;
    }

    public int getPersistentId()
    {
        return persistentId;
    }
}
