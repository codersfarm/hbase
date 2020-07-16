package com.hbase;

import com.hbase.AdminAPI;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: wang luqi
 * @Date: 2020/7/15 17:57
 **/
public class TestAdminAPI {

	@Test
	public void createTable(){
		List<String> stringList = new ArrayList<>();
		stringList.add("a");
		stringList.add("b");
		stringList.add("c");
		boolean create = AdminAPI.createTable("student", stringList);
		System.out.println(create);
	}

	@Test
	public void existsTable(){
		boolean exist = false;
		try {
			exist = AdminAPI.exists("student");
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(exist);
	}
}