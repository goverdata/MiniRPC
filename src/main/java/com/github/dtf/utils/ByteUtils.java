package com.github.dtf.utils;

public class ByteUtils {

	public static void printBytes(byte[] bytes){
		if(bytes == null || bytes.length <= 0){
			System.out.println("Empty byte array.");
			return;
		}
		StringBuilder sb = new StringBuilder();
		sb.append("Total length:");
		sb.append(bytes.length);
		sb.append(",[");
		for(byte b : bytes){
			sb.append(b);
			sb.append(",");
		}
		sb.append("]");
		System.out.println(sb.toString());
	}
}
