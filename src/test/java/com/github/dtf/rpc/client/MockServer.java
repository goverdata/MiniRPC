package com.github.dtf.rpc.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

public class MockServer {
	public static void main(String[] args) {
		try {
			ServerSocket server = new ServerSocket();
			SocketAddress add = new InetSocketAddress("localhost", 8181);
			server.bind(add);
			while (true) {
				Socket clientSocket = null;
				try {
					clientSocket = server.accept();
					BufferedReader reader = new BufferedReader(
							new InputStreamReader(clientSocket.getInputStream()));
					System.out.println("From client:"
							+ clientSocket.getRemoteSocketAddress()
							+ " Information is:" + reader.readLine());
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (clientSocket != null) {
						try {
							clientSocket.close();
						} catch (Exception e1) {
							e1.printStackTrace();
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

		}
	}
}
