package com.huaweicloud.dis.agent.processing.utils;

import com.huaweicloud.dis.util.encrypt.EncryptUtils;

public class EncryptTool
{
    private static final String DEFAULT_KEY = "9535995d-191e-4d12-a2c5-b8406f05e531";

    private EncryptTool()
    {
    }

    public static String encrypt(String data)
    {
        return encrypt(data, DEFAULT_KEY);
    }

    public static String encrypt(String data, String key)
    {
        try
        {
            if (key == null)
            {
                key = DEFAULT_KEY;
            }
            return EncryptUtils.gen(new String[]{key}, data);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static String decrypt(String data)
    {
        return decrypt(data, DEFAULT_KEY);
    }

    public static String decrypt(String data, String key)
    {
        try
        {
            if (key == null)
            {
                key = DEFAULT_KEY;
            }
            return EncryptUtils.dec(new String[]{key}, data);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args)
    {
        if (args.length < 1)
        {
            doUsage();
            System.exit(-1);
        }
        if (args.length > 1)
        {
            System.out.println(encrypt(args[0], args[1]));
        }
        else
        {
            System.out.println(encrypt(args[0]));
        }
    }

    private static void doUsage()
    {
        System.out.println("Please input password.");
    }
}
