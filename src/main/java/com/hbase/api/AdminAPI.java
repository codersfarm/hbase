package com.hbase.api;

import com.hbase.HbaseConn;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @Author: wang luqi
 * @Date: 2020/7/15 15:38
 **/
public class AdminAPI {

	private static Connection connection = HbaseConn.getConnection();


	/**
	 * 创建HBase表
	 *
	 * @param tableName      表名
	 * @param columnFamilies 列族的数组
	 */
	public static boolean createTable(String tableName, List<String> columnFamilies) {
		try {
			HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
			if (admin.tableExists(TableName.valueOf(tableName))) {
				return false;
			}
			TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
			columnFamilies.forEach(columnFamily -> {
				ColumnFamilyDescriptorBuilder cfDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
				cfDescriptorBuilder.setMaxVersions(1);
				ColumnFamilyDescriptor familyDescriptor = cfDescriptorBuilder.build();
				tableDescriptor.setColumnFamily(familyDescriptor);
			});
			admin.createTable(tableDescriptor.build());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * 判断表是否存在
	 *
	 * @param name
	 * @return
	 * @throws IOException
	 */
	public static boolean exists(String name) throws IOException {
		HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
		boolean result = admin.tableExists(TableName.valueOf(name));
		return result;
	}

	/**
	 * 删除hBase表
	 *
	 * @param tableName 表名
	 */
	public static boolean deleteTable(String tableName) {
		try {
			HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
			// 删除表前需要先禁用表
			admin.disableTable(TableName.valueOf(tableName));
			admin.deleteTable(TableName.valueOf(tableName));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * 禁用表
	 *
	 * @param name
	 * @throws IOException
	 */
	public static void disable(String name) throws IOException {
		Admin admin = connection.getAdmin();

		TableName tableName = TableName.valueOf(name);

		if (!admin.tableExists(tableName)) {
			System.out.println("table " + tableName + " not exists");
			return;
		}

		boolean isDisabled = admin.isTableDisabled(tableName);
		if (!isDisabled) {
			System.out.println("disable table " + name);
			admin.disableTable(tableName);
		}
	}

	/**
	 * 启用表
	 *
	 * @param name
	 * @throws IOException
	 */
	public static void enable(String name) throws IOException {
		Admin admin = connection.getAdmin();

		TableName tableName = TableName.valueOf(name);

		if (!admin.tableExists(tableName)) {
			System.out.println("table " + tableName + " not exists");
			return;
		}

		boolean isEnabled = admin.isTableEnabled(tableName);
		if (!isEnabled) {
			System.out.println("enable table " + name);
			admin.enableTable(tableName);
		}
	}

	/**
	 * 增加列簇
	 *
	 * @param name
	 * @param cf
	 * @throws IOException
	 */
	public static void addColumnFamily(String name, String cf) throws IOException {
		Admin admin = connection.getAdmin();

		TableName tableName = TableName.valueOf(name);

		if (!admin.tableExists(tableName)) {
			System.out.println("table " + tableName + " not exists");
			return;
		}

		ColumnFamilyDescriptor desc = ColumnFamilyDescriptorBuilder.of(cf);
		admin.addColumnFamily(TableName.valueOf(name), desc);
		System.out.println("add column family " + cf);

	}

	/**
	 * 更新列簇
	 *
	 * @param name
	 * @param cf
	 * @throws IOException
	 */
	public static void updateColumnFamily(String name, String cf) throws IOException {
		Admin admin = connection.getAdmin();

		TableName tableName = TableName.valueOf(name);

		if (!admin.tableExists(tableName)) {
			System.out.println("table " + tableName + " not exists");
			return;
		}
		ColumnFamilyDescriptor desc = ColumnFamilyDescriptorBuilder.of(cf);
		admin.modifyColumnFamily(tableName, desc);
		System.out.println("add column family " + cf);

	}

	/**
	 * 删除
	 *
	 * @param name
	 * @param cf
	 * @throws IOException
	 */
	public static void deleteColumnFamily(String name, String cf) throws IOException {
		Admin admin = connection.getAdmin();

		TableName tableName = TableName.valueOf(name);

		if (!admin.tableExists(tableName)) {
			System.out.println("table " + tableName + " not exists");
			return;
		}

		admin.deleteColumn(tableName, cf.getBytes());
		System.out.println("delete column family " + cf);
	}
}
