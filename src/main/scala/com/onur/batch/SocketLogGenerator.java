package com.onur.batch;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by Onur_Dincol on 10/13/2017.
 */

public class SocketLogGenerator {
    private static final String FILENAME = "./src/main/resources/CarDealerMock.txt";

    public static void main(String args[])  {
        final int portNumber = 9999;
        System.out.println("Creating server socket on port " + portNumber);
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(portNumber);
            while (true) {
                Socket socket = serverSocket.accept();
                OutputStream os = socket.getOutputStream();
                PrintWriter pw = new PrintWriter(os, true);
                BufferedReader br = new BufferedReader(new FileReader(FILENAME));
                String sCurrentLine;
                while ((sCurrentLine = br.readLine()) != null) {
                    System.out.println(sCurrentLine);
                    pw.println(sCurrentLine);
                    Thread.sleep(100);
                }
                pw.close();
                socket.close();
                break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
