package org.iq80.twoLayerLog.impl;

import com.google.common.base.Throwables;
import org.iq80.twoLayerLog.util.Closeables;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DbLock
{
    private final File lockFile;
    private final FileChannel channel;
    private final FileLock lock;

    public DbLock(File lockFile)
            throws IOException
    {
        requireNonNull(lockFile, "lockFile is null");
        this.lockFile = lockFile;

        // open and lock the file
        channel = new RandomAccessFile(lockFile, "rw").getChannel();
        try {
            lock = channel.tryLock();
        }
        catch (IOException e) {
            Closeables.closeQuietly(channel);
            throw e;
        }

        if (lock == null) {
            throw new IOException(format("Unable to acquire lock on '%s'", lockFile.getAbsolutePath()));
        }
    }

    public boolean isValid()
    {
        return lock.isValid();
    }

    public void release()
    {
        try {
            lock.release();
        }
        catch (IOException e) {
            Throwables.propagate(e);
        }
        finally {
            Closeables.closeQuietly(channel);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("DbLock");
        sb.append("{lockFile=").append(lockFile);
        sb.append(", lock=").append(lock);
        sb.append('}');
        return sb.toString();
    }
}
