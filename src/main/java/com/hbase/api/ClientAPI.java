package com.hbase.api;

import com.hbase.HbaseConn;
import javafx.util.Pair;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @Author: wang luqi
 * @Date: 2020/7/15 15:39
 **/
public class ClientAPI {

	private static Connection connection = HbaseConn.getConnection();

	/**
	 * 插入数据
	 *
	 * @param tableName        表名
	 * @param rowKey           唯一标识
	 * @param columnFamilyName 列族名
	 * @param qualifier        列标识
	 * @param value            数据
	 */
	public static boolean putRow(String tableName, String rowKey, String columnFamilyName, String qualifier,
	                             String value) {
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			table.put(put);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * 插入数据
	 *
	 * @param tableName        表名
	 * @param rowKey           唯一标识
	 * @param columnFamilyName 列族名
	 * @param pairList         列标识和值的集合
	 */
	public static boolean putRow(String tableName, String rowKey, String columnFamilyName, List<Pair<String, String>> pairList) {
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Put put = new Put(Bytes.toBytes(rowKey));
			pairList.forEach(pair -> put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(pair.getKey()), Bytes.toBytes(pair.getValue())));
			table.put(put);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}


	/**
	 * 根据rowKey获取指定行的数据
	 *
	 * @param tableName 表名
	 * @param rowKey    唯一标识
	 */
	public static Result getRow(String tableName, String rowKey) {
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));
			return table.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}


	/**
	 * 获取指定行指定列(cell)的最新版本的数据
	 *
	 * @param tableName    表名
	 * @param rowKey       唯一标识
	 * @param columnFamily 列族
	 * @param qualifier    列标识
	 */
	public static String getCell(String tableName, String rowKey, String columnFamily, String qualifier) {
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));
			if (!get.isCheckExistenceOnly()) {
				get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
				Result result = table.get(get);
				byte[] resultValue = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
				return Bytes.toString(resultValue);
			} else {
				return null;
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}


	/**
	 * 检索全表
	 *
	 * @param tableName 表名
	 */
	public static ResultScanner getScanner(String tableName) {
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			return table.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}


	/**
	 * 检索表中指定数据
	 *
	 * @param tableName  表名
	 * @param filterList 过滤器
	 */

	public static ResultScanner getScanner(String tableName, FilterList filterList) {
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			scan.setFilter(filterList);
			return table.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 检索表中指定数据
	 *
	 * @param tableName   表名
	 * @param startRowKey 起始RowKey
	 * @param endRowKey   终止RowKey
	 * @param filterList  过滤器
	 */

	public static ResultScanner getScanner(String tableName, String startRowKey, String endRowKey,
	                                       FilterList filterList) {
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			scan.withStartRow(Bytes.toBytes(startRowKey));
			scan.withStopRow(Bytes.toBytes(endRowKey));
			scan.setFilter(filterList);
			return table.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 删除指定行记录
	 *
	 * @param tableName 表名
	 * @param rowKey    唯一标识
	 */
	public static boolean deleteRow(String tableName, String rowKey) {
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			table.delete(delete);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}


	/**
	 * 删除指定行指定列
	 *
	 * @param tableName  表名
	 * @param rowKey     唯一标识
	 * @param familyName 列族
	 * @param qualifier  列标识
	 */
	public static boolean deleteColumn(String tableName, String rowKey, String familyName,
	                                   String qualifier) {
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			delete.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifier));
			table.delete(delete);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
}