package com.hbase.rokey;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * @Author: 基于时间序列
 * @Date: 2020/7/21 15:37
 **/
public class RowKeyIncrTime {

	private static int addPart = 1;
	private static String result = "";
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
	private static String lastDate = "";
	/**
	 * 获取主键
	 * @param length 长度
	 * @return 返回17位时间戳+3位递增数
	 */
	public static String getId(int length) {
		//获取时间部分字符串
		Date now = new Date();
		String nowStr = sdf.format(now);

		//获取数字后缀值部分
		if (RowKeyIncrTime.lastDate.equals(nowStr)) {
			addPart += 1;
		} else {
			addPart = 1;
			lastDate = nowStr;
		}

		if (length > 17) {
			length -= 17;
			for (int i = 0; i < length - ((addPart + "").length()); i++) {
				nowStr += "0";
			}
			nowStr += addPart;
			result = nowStr;
		} else {
			result = nowStr;
		}
		//20171127092455109003
		return result;
	}


	public static String generateRowKey(String provCode,String cityCode){
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(provCode).append(cityCode).append(UUID.randomUUID()).append(getId(20));
		return stringBuilder.toString();
	}

	public static void main(String[] args) {
		System.out.println(generateRowKey("371","371"));
	}
}
