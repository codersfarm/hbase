package com.hbase;

import com.hbase.api.ClientAPI;
import com.hbase.rokey.RowKeyIncrTime;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.Test;

import java.util.Iterator;

/**
 * @Author: wang luqi
 * @Date: 2020/7/16 16:16
 **/
public class TestClientAPI {



	@Test
	public void putRowData(){
		String rowKey = RowKeyIncrTime.generateRowKey("371","371");
		boolean b = ClientAPI.putRow("student",rowKey,"info","name","ls");
		System.out.println(b);
	}

	@Test
	public void getRowData(){
		ResultScanner results = ClientAPI.getScanner("student");
		Iterator<Result> iterable = results.iterator();
		while (iterable.hasNext()){
			System.out.println(iterable.next());
		}
	}
}