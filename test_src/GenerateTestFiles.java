/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package scratchpad;

import org.voltdb.*;
import org.voltdb.client.*;

import org.voltdb.VoltTable.ColumnInfo;
import java.nio.*;
import java.nio.channels.*;
import java.net.*;
import java.io.*;
import org.voltdb.types.*;
import java.math.*;
import org.voltdb.messaging.*;

public class GenerateTestFiles {
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.socket().bind(new InetSocketAddress("localhost", 21212));
        ClientConfig config = new ClientConfig("hello", "world", (ClientStatusListenerExt )null, ClientAuthHashScheme.HASH_SHA256);
        final org.voltdb.client.Client client = ClientFactory.createClient(config);
        Thread clientThread = new Thread() {
            @Override
            public void run() {
                try {
                    client.createConnection( "localhost", 21212);
                    client.callProcedure("Insert", "Hello", "World", "English");
                    try {
                        client.callProcedure("Insert", "Hello", "World", "English");
                    } catch (Exception e) {

                    }
                    client.callProcedure("Select", "English");
                    client.callProcedure(new NullCallback(), "@Shutdown");
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        };
        clientThread.setDaemon(true);
        clientThread.start();
        SocketChannel sc = ssc.accept();
        sc.socket().setTcpNoDelay(true);
        ByteBuffer message = ByteBuffer.allocate(8096);
        sc.configureBlocking(true);
        int read = sc.read(message);
        message.flip();
        FileOutputStream fos = new FileOutputStream("authentication_request_sha256.msg");
        fos.write(message.array(), 0, message.remaining());
        fos.flush();
        fos.close();

        ssc.close();

        ssc = ServerSocketChannel.open();
        ssc.socket().bind(new InetSocketAddress("localhost", 21212));
        config = new ClientConfig("hello", "world", (ClientStatusListenerExt )null, ClientAuthHashScheme.HASH_SHA1);
        final org.voltdb.client.Client oclient = ClientFactory.createClient(config);
        Thread oclientThread = new Thread() {
            @Override
            public void run() {
                try {
                    oclient.createConnection( "localhost", 21212);
                    oclient.callProcedure("Insert", "Hello", "World", "English");
                    try {
                        oclient.callProcedure("Insert", "Hello", "World", "English");
                    } catch (Exception e) {

                    }
                    oclient.callProcedure("Select", "English");
                    oclient.callProcedure(new NullCallback(), "@Shutdown");
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        };
        oclientThread.setDaemon(true);
        oclientThread.start();
        sc = ssc.accept();
        sc.socket().setTcpNoDelay(true);
        ByteBuffer omessage = ByteBuffer.allocate(8096);
        sc.configureBlocking(true);
        read = sc.read(omessage);
        omessage.flip();
        fos = new FileOutputStream("authentication_request.msg");
        fos.write(omessage.array(), 0, omessage.remaining());
        fos.flush();
        fos.close();

        ssc.close();

        System.out.print("Start a fresh VoltDB server with the HelloWorld example and authentication disabled so that an authentication request can be generated and the response read back as well as a few procedure invocations and then press enter");
        System.in.read();
        final SocketChannel voltsc = SocketChannel.open(new InetSocketAddress("localhost", 21212));
        voltsc.socket().setTcpNoDelay(true);
        voltsc.configureBlocking(true);
        voltsc.write(message);
        message.clear();
        voltsc.read(message);
        message.flip();
        ByteBuffer authenticationResponse = ByteBuffer.allocate(message.remaining());
        authenticationResponse.put(message);
        message.flip();
        authenticationResponse.flip();
        fos = new FileOutputStream("authentication_response.msg");
        fos.write(message.array(), 0, message.remaining());
        fos.flush();
        fos.close();
        System.exit(0);

        sc.write(message);
        message.clear();
        sc.read(message);
        message.flip();

        fos = new FileOutputStream("invocation_request_success.msg");
        fos.write(message.array(), 0, message.remaining());
        fos.flush();
        fos.close();

        voltsc.write(message);
        message.clear();
        voltsc.read(message);
        message.flip();

        fos = new FileOutputStream("invocation_response_success.msg");
        fos.write(message.array(), 0, message.remaining());
        fos.flush();
        fos.close();
        sc.write(message);

        message.clear();
        sc.read(message);
        message.flip();

        fos = new FileOutputStream("invocation_request_fail_cv.msg");
        fos.write(message.array(), 0, message.remaining());
        fos.flush();
        fos.close();

        voltsc.write(message);
        message.clear();
        voltsc.read(message);
        message.flip();

        fos = new FileOutputStream("invocation_response_fail_cv.msg");
        fos.write(message.array(), 0, message.remaining());
        fos.flush();
        fos.close();
        sc.write(message);

        message.clear();
        sc.read(message);
        message.flip();

        fos = new FileOutputStream("invocation_request_select.msg");
        fos.write(message.array(), 0, message.remaining());
        fos.flush();
        fos.close();

        voltsc.write(message);
        message.clear();
        voltsc.read(message);
        message.flip();

        fos = new FileOutputStream("invocation_response_select.msg");
        fos.write(message.array(), 0, message.remaining());
        fos.flush();
        fos.close();
        sc.write(message);

        message.clear();
        sc.read(message);
        message.flip();
        voltsc.write(message);

        voltsc.close();
        sc.close();
        clientThread.join();
        client.close();

        Thread.sleep(3000);
        ssc = ServerSocketChannel.open();
        ssc.socket().bind(new InetSocketAddress("localhost", 21212));

        clientThread = new Thread() {
            @Override
            public void run() {
                try {
                    org.voltdb.client.Client newClient = ClientFactory.createClient();
                    newClient.createConnection( "localhost", 21212);
                    String strings[] = new String[] { "oh", "noes" };
                    byte bytes[] = new byte[] { 22, 33, 44 };
                    short shorts[] = new short[] { 22, 33, 44 };
                    int ints[] = new int[] { 22, 33, 44 };
                    long longs[] = new long[] { 22, 33, 44 };
                    double doubles[] = new double[] { 3, 3.1, 3.14, 3.1459 };
                    TimestampType timestamps[] = new TimestampType[] { new TimestampType(33), new TimestampType(44) };
                    BigDecimal bds[] = new BigDecimal[] { new BigDecimal( "3" ), new BigDecimal( "3.14" ), new BigDecimal( "3.1459" ) };
                    try {
                        newClient.callProcedure("foo", strings, bytes, shorts, ints, longs, doubles, timestamps, bds, null, "ohnoes!", (byte)22, (short)22, 22, (long)22, 3.1459, new TimestampType(33), new BigDecimal("3.1459"));
                    } catch (Exception e) {}
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        clientThread.setDaemon(true);
        clientThread.start();

        sc = ssc.accept();
        message.clear();
        sc.read(message);
        sc.write(authenticationResponse);

        message.clear();
        read = sc.read(message);
        sc.close();

        message.flip();
        fos = new FileOutputStream("invocation_request_all_params.msg");
        fos.write(message.array(), 0, message.remaining());
        fos.flush();
        fos.close();

        ColumnInfo columns[] = new ColumnInfo[] {
                new ColumnInfo("column1", VoltType.TINYINT),
                new ColumnInfo("column2", VoltType.STRING),
                new ColumnInfo("column3", VoltType.SMALLINT),
                new ColumnInfo("column4", VoltType.INTEGER),
                new ColumnInfo("column5", VoltType.BIGINT),
                new ColumnInfo("column6", VoltType.TIMESTAMP),
                new ColumnInfo("column7", VoltType.DECIMAL)
        };
        VoltTable vt = new VoltTable(columns);
        vt.addRow( null, null, null, null, null, null, null);
        vt.addRow( 0, "", 2, 4, 5, new TimestampType(44), new BigDecimal("3.1459"));
        vt.addRow( 0, null, 2, 4, 5, null, null);
        vt.addRow( null, "woobie", null, null, null, new TimestampType(44), new BigDecimal("3.1459"));
        ByteBuffer bb = ByteBuffer.allocate(vt.getSerializedSize());
        vt.flattenToBuffer(bb);
        FastSerializer fs = new FastSerializer(vt.getSerializedSize());
        fs.write(bb);
        fos = new FileOutputStream("serialized_table.bin");
        fos.write(fs.getBytes());
        fos.flush();
        fos.close();

        clientThread.join();
    }

}
