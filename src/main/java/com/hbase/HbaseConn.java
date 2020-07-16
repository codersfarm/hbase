package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @Author: wang luqi
 * @Date: 2020/7/14 14:44
 **/
public class HbaseConn {

	private static Connection connection;
	private static Configuration conf;

	/**
	 * 获取连接
	 *
	 * @return
	 */
	static {
		// 新建一个Configuration
		conf = HBaseConfiguration.create();
		// 集群的连接地址(公网地址)在控制台页面的数据库连接界面获得
		conf.set("hbase.client.endpoint", "https://sh-bp14g4jkecj5q57w7-hbase-serverless.hbase.rds.aliyuncs.com:443");
		// xml_template.comment.hbaseue.username_password.default
		conf.set("hbase.client.username", "LTAI4GE8y8SRKrfVTvTkHvbB");
		conf.set("hbase.client.password", "uO10z7IYyasHKeDYPLrbgbtOziJLjZ");
		// 如果您直接依赖了阿里云hbase客户端，则无需配置connection.impl参数，如果您依赖了alihbase-connector，则需要配置此参数
		//conf.set("hbase.client.connection.impl", AliHBaseUEClusterConnection.class.getName());
	}

	public static Connection getConnection() {
		if (connection == null || connection.isClosed()) {
			try {
				// 创建 HBase连接，在程序生命周期内只需创建一次，该连接线程安全，可以共享给所有线程使用。
				// 在程序结束后，需要将Connection对象关闭，否则会造成连接泄露。
				// 也可以采用try finally方式防止泄露
				connection = ConnectionFactory.createConnection(conf);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return connection;
	}

	/**
	 * 关闭连接
	 */
	public static void closeConnection() {
		if (connection != null) {
			try {
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}