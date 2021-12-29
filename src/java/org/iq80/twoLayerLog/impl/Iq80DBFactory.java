package org.iq80.twoLayerLog.impl;

import org.iq80.twoLayerLog.DB;
import org.iq80.twoLayerLog.DBFactory;
import org.iq80.twoLayerLog.Options;
import org.iq80.twoLayerLog.util.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Iq80DBFactory
        implements DBFactory
{
    public static final int CPU_DATA_MODEL;

    static {
        boolean is64bit;
        if (System.getProperty("os.name").contains("Windows")) {
            is64bit = System.getenv("ProgramFiles(x86)") != null;
        }
        else {
            is64bit = System.getProperty("os.arch").contains("64");
        }
        CPU_DATA_MODEL = is64bit ? 64 : 32;
    }

    // We only use MMAP on 64 bit systems since it's really easy to run out of
    // virtual address space on a 32 bit system when all the data is getting mapped
    // into memory.  If you really want to use MMAP anyways, use -Dleveldb.mmap=true
    //public static final boolean USE_MMAP = Boolean.parseBoolean(System.getProperty("leveldb.mmap", "" + (CPU_DATA_MODEL > 32)));
    public static final boolean USE_MMAP = false; //false

    public static final String VERSION;

    static {
        String v = "unknown";
        InputStream is = Iq80DBFactory.class.getResourceAsStream("version.txt");
        try {
            v = new BufferedReader(new InputStreamReader(is, UTF_8)).readLine();
        }
        catch (Throwable e) {
        }
        finally {
            try {
                is.close();
            }
            catch (Throwable e) {
            }
        }
        VERSION = v;
    }

    public static final Iq80DBFactory factory = new Iq80DBFactory();

    @Override
    public DB open(File path, Options options)
            throws IOException
    {
        return new DbImpl(options, path);
    }

    @Override
    public void destroy(File path, Options options)
            throws IOException
    {
        // TODO: This should really only delete leveldb-created files.
        FileUtils.deleteRecursively(path);
    }

    @Override
    public void repair(File path, Options options)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return String.format("iq80 leveldb version %s", VERSION);
    }

    public static byte[] bytes(String value)
    {
        return (value == null) ? null : value.getBytes(UTF_8);
    }

    public static String asString(byte[] value)
    {
        return (value == null) ? null : new String(value, UTF_8);
    }
}
