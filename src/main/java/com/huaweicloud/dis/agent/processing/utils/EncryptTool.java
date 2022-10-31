package com.huaweicloud.dis.agent.processing.utils;

import com.huaweicloud.dis.util.encrypt.EncryptUtils;

public class EncryptTool
{


    private EncryptTool()
    {
    }



    public static String encrypt(String data, String key)
    {
        try
        {
            if (key == null)
            {
               throw new RuntimeException("key is null");
            }
            return EncryptUtils.gen(new String[]{key}, data);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }


    public static String decrypt(String data, String key)
    {
        try
        {
            if (key == null)
            {
                throw new RuntimeException("key is null");
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

        }
    }

    private static void doUsage()
    {
        System.out.println("Please input password.");
    }
}
