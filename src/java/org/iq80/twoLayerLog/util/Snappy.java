package org.iq80.twoLayerLog.util;

import java.io.IOException;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class Snappy
{
    private Snappy()
    {
    }

    public interface SPI
    {
        int uncompress(ByteBuffer compressed, ByteBuffer uncompressed)
                throws IOException;

        int uncompress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
                throws IOException;

        int compress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
                throws IOException;

        byte[] compress(String text)
                throws IOException;

        int maxCompressedLength(int length);
    }

    public static class XerialSnappy
            implements SPI
    {

        @Override
        public int uncompress(ByteBuffer compressed, ByteBuffer uncompressed)
                throws IOException
        {
        	return 0;
            //return org.xerial.snappy.Snappy.uncompress(compressed, uncompressed);
        }

        @Override
        public int uncompress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
                throws IOException
        {
        	return 0;
            //return org.xerial.snappy.Snappy.uncompress(input, inputOffset, length, output, outputOffset);
        }

        @Override
        public int compress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
                throws IOException
        {
        	return 0;
            //return org.xerial.snappy.Snappy.compress(input, inputOffset, length, output, outputOffset);
        }

        @Override
        public byte[] compress(String text)
                throws IOException
        {
        	return null;
            //return org.xerial.snappy.Snappy.compress(text);
        }

        @Override
        public int maxCompressedLength(int length)
        {
        	return 0;
            //return org.xerial.snappy.Snappy.maxCompressedLength(length);
        }
    }

    public static class IQ80Snappy
            implements SPI
    {
        static {
            // Make sure that the library can fully load.
            try {
                new IQ80Snappy().compress("test");
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int uncompress(ByteBuffer compressed, ByteBuffer uncompressed)
                throws IOException
        {
            byte[] input;
            int inputOffset;
            int length;
            byte[] output;
            int outputOffset;
            if (compressed.hasArray()) {
                input = compressed.array();
                inputOffset = compressed.arrayOffset() + compressed.position();
                length = compressed.remaining();
            }
            else {
                input = new byte[compressed.remaining()];
                inputOffset = 0;
                length = input.length;
                compressed.mark();
                compressed.get(input);
                compressed.reset();
            }
            if (uncompressed.hasArray()) {
                output = uncompressed.array();
                outputOffset = uncompressed.arrayOffset() + uncompressed.position();
            }
            else {
                int t = org.iq80.snappy.Snappy.getUncompressedLength(input, inputOffset);
                output = new byte[t];
                outputOffset = 0;
            }

            int count = org.iq80.snappy.Snappy.uncompress(input, inputOffset, length, output, outputOffset);
            if (uncompressed.hasArray()) {
                uncompressed.limit(uncompressed.position() + count);
            }
            else {
                int p = uncompressed.position();
                uncompressed.limit(uncompressed.capacity());
                uncompressed.put(output, 0, count);
                uncompressed.flip().position(p);
            }
            return count;
        }

        @Override
        public int uncompress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
                throws IOException
        {
            return org.iq80.snappy.Snappy.uncompress(input, inputOffset, length, output, outputOffset);
        }

        @Override
        public int compress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
                throws IOException
        {
            return org.iq80.snappy.Snappy.compress(input, inputOffset, length, output, outputOffset);
        }

        @Override
        public byte[] compress(String text)
                throws IOException
        {
            byte[] uncomressed = text.getBytes(UTF_8);
            byte[] compressedOut = new byte[maxCompressedLength(uncomressed.length)];
            int compressedSize = compress(uncomressed, 0, uncomressed.length, compressedOut, 0);
            byte[] trimmedBuffer = new byte[compressedSize];
            System.arraycopy(compressedOut, 0, trimmedBuffer, 0, compressedSize);
            return trimmedBuffer;
        }

        @Override
        public int maxCompressedLength(int length)
        {
            return org.iq80.snappy.Snappy.maxCompressedLength(length);
        }
    }

    private static final SPI SNAPPY;

    static {
        SPI attempt = null;
        String[] factories = System.getProperty("leveldb.snappy", "iq80,xerial").split(",");
        for (int i = 0; i < factories.length && attempt == null; i++) {
            String name = factories[i];
            try {
                name = name.trim();
                if ("xerial".equals(name.toLowerCase())) {
                    name = "org.iq80.twoLayerLog.util.Snappy$XerialSnappy";
                }
                else if ("iq80".equals(name.toLowerCase())) {
                    name = "org.iq80.twoLayerLog.util.Snappy$IQ80Snappy";
                }
                attempt = (SPI) Thread.currentThread().getContextClassLoader().loadClass(name).newInstance();
            }
            catch (Throwable e) {
            }
        }
        SNAPPY = attempt;
    }

    public static boolean available()
    {
        return SNAPPY != null;
    }

    public static void uncompress(ByteBuffer compressed, ByteBuffer uncompressed)
            throws IOException
    {
        SNAPPY.uncompress(compressed, uncompressed);
    }

    public static void uncompress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
            throws IOException
    {
        SNAPPY.uncompress(input, inputOffset, length, output, outputOffset);
    }

    public static int compress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
            throws IOException
    {
        return SNAPPY.compress(input, inputOffset, length, output, outputOffset);
    }

    public static byte[] compress(String text)
            throws IOException
    {
        return SNAPPY.compress(text);
    }

    public static int maxCompressedLength(int length)
    {
        return SNAPPY.maxCompressedLength(length);
    }
}
