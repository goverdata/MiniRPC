package com.github.dtf.rpc.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.github.dtf.rpc.Writable;

public class TestParam implements Writable{

	public void write(DataOutput out) throws IOException {
		out.writeBytes("name=johnny");
	}

	public void readFields(DataInput in) throws IOException {
		System.out.println(in.readLine());
	}

}
