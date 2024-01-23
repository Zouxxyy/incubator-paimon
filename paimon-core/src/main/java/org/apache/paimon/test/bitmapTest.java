package org.apache.paimon.test;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class bitmapTest {

    public static void main(String[] args) {
        // testRoaring64NavigableMap(5000_0000L, 10_0000_0000L);
        // testRoaring64NavigableMap(100_0000L, 1_0000_0000L);
        // testRoaring64NavigableMap(10_0000, 200_0000);
        // testRoaring64NavigableMap(100_0000, 200_0000);
        // testRoaring64NavigableMap(1000_0000, 200_0000);

        System.out.println();

        int max = 20_0000_0000;
        testRoaringMap((int) (max * 0.2), max);
        System.out.println();
        testRoaringMap((int) (max * 0.5), max);
        System.out.println();
        testRoaringMap((int) (max * 0.8), max);
        System.out.println();
    }

    public static void testRoaring64NavigableMap (long numberOfRandoms, long max) {
        Roaring64NavigableMap roaring64Map = new Roaring64NavigableMap();
        Random random = new Random();

        long cur = System.currentTimeMillis();
        for (long i = 0; i < numberOfRandoms; ++i) {
            long randomNumber = 1L + (Math.abs(random.nextLong()) % (max - 1));
            roaring64Map.add(randomNumber);
        }
        System.out.println("插入数据耗时：" + (System.currentTimeMillis() - cur) + "ms");

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(bos)) {

            cur = System.currentTimeMillis();
            roaring64Map.runOptimize();
            roaring64Map.serializePortable(dos);

            String filePath = "/Users/zxy/project/open-source/incubator-paimon/paimon-core/src/main/resources/file.bin";
            try (FileOutputStream fos = new FileOutputStream(filePath)) {
                bos.writeTo(fos);
            }
            System.out.println("序列化耗时：" + (System.currentTimeMillis() - cur) + "ms");

            LocalFileIO fileIO = LocalFileIO.create();
            System.out.println("文件大小：" + fileIO.getFileStatus(new Path(filePath)).getLen() / 1024F / 1024 + "MB");

            cur = System.currentTimeMillis();
            try (FileInputStream fis = new FileInputStream(filePath);
                 DataInputStream dis = new DataInputStream(fis)) {
                Roaring64NavigableMap newRoaring64Map = new Roaring64NavigableMap();
                newRoaring64Map.deserializePortable(dis);
                System.out.println("反序列化耗时：" + (System.currentTimeMillis() - cur) + "ms");

                cur = System.currentTimeMillis();
                for (int i = 0; i < numberOfRandoms; i++) {
                    newRoaring64Map.contains(i);
                }
                System.out.println("filter 耗时：" + (System.currentTimeMillis() - cur) + "ms");
            }
        } catch (IOException e) {
            //
        }
    }

    public static void testRoaringMap (int numberOfRandoms, int max) {
        RoaringBitmap roaringMap = new RoaringBitmap();
        Random random = new Random();

        long cur = System.currentTimeMillis();
        for (long i = 0; i < numberOfRandoms; ++i) {
            int randomNumber = (int) (1L + (Math.abs(random.nextInt()) % (max - 1)));
            roaringMap.add(randomNumber);
        }
        System.out.println("插入数据耗时：" + (System.currentTimeMillis() - cur) + "ms");

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(bos)) {

            cur = System.currentTimeMillis();
            roaringMap.runOptimize();
            roaringMap.serialize(dos);

            String filePath = "/Users/zxy/project/open-source/incubator-paimon/paimon-core/src/main/resources/file.bin";
            try (FileOutputStream fos = new FileOutputStream(filePath)) {
                bos.writeTo(fos);
            }
            System.out.println("序列化耗时：" + (System.currentTimeMillis() - cur) + "ms");

            LocalFileIO fileIO = LocalFileIO.create();
            System.out.println("文件大小：" + fileIO.getFileStatus(new Path(filePath)).getLen() / 1024F / 1024 + "MB");

            cur = System.currentTimeMillis();
            try (FileInputStream fis = new FileInputStream(filePath);
                 DataInputStream dis = new DataInputStream(fis)) {
                RoaringBitmap newRoaringMap = new RoaringBitmap();
                newRoaringMap.deserialize(dis);
                System.out.println("反序列化耗时：" + (System.currentTimeMillis() - cur) + "ms");

                cur = System.currentTimeMillis();
                for (int i = 0; i < numberOfRandoms; i++) {
                    newRoaringMap.contains(i);
                }
                System.out.println("filter 耗时：" + (System.currentTimeMillis() - cur) + "ms");
            }
        } catch (IOException e) {
            //
        }
    }
}
