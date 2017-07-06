package org.demo.redisDemo.SellerBuyer;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.demo.redisDemo.lock.LockUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class SimpleMarketDemo {


	public static void main(String[] args) {

		SimpleDateFormat format = new SimpleDateFormat("yyyy:MM:dd-hh:mm:ss");

		initData(format);
//		initBatchData(format);
		
		runSellerAndBuy1();
	}

	
	/**
	 * 初始化单个卖家商品数据
	 */
	public static void initData(SimpleDateFormat format) {
		
		Date initStart = new Date(System.currentTimeMillis());
		System.out.println("init start: \t" + format.format(initStart));
		
		Jedis conn = null;
		String seller = "inventory:37";
		int initCount = 300000;

		try{
			conn = new Jedis("localhost");
			Pipeline pipe = conn.pipelined();

				pipe.multi();
				for(int itemid = 0;itemid<initCount; itemid++) {
					pipe.sadd(seller, String.valueOf(itemid));
				}
				pipe.exec();
				pipe.sync();
				System.out.println(seller + " init count: \t" + conn.scard(seller));
		
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			conn.disconnect();
		}
		
		Date initEnd = new Date(System.currentTimeMillis());
		System.out.println("init end: \t" + format.format(initEnd));
		System.out.println("init cost: \t" + (initEnd.getTime()-initStart.getTime()) + "ms");
		System.out.println("");
	}
	
	/**
	 * 初始化多个卖家商品数据
	 */
	public static void initBatchData(SimpleDateFormat format) {
		
		Date initStart = new Date(System.currentTimeMillis());
		System.out.println("init start: \t" + format.format(initStart));
		
		Jedis conn = null;
		int initCount = 50000;

		try{
			conn = new Jedis("localhost");
			Pipeline pipe = conn.pipelined();

			for(int i=5; i<=9; i++){
				String seller = "inventory:3" + String.valueOf(i); 
				pipe.multi();
				for(int itemid = 0;itemid<initCount; itemid++) {
					pipe.sadd(seller, String.valueOf(itemid));
				}
				pipe.exec();
				pipe.sync();
				System.out.println(seller + " init count: \t" + conn.scard(seller));
			}
		
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			conn.disconnect();
		}
		
		Date initEnd = new Date(System.currentTimeMillis());
		System.out.println("init end: \t" + format.format(initEnd));
		System.out.println("init cost: \t" + (initEnd.getTime()-initStart.getTime()) + "ms");
		System.out.println("");
		
	}


	/**
	 * 1个卖家，1个买家
	 */
	protected static void runSellerAndBuy1() {
		new Thread(new Seller("37",true)).start();
		try {
			Thread.sleep(10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		new Thread(new Buyer("47", "37",true)).start();
	}
	

	/**
	 * 5个卖家，1个买家
	 */
	protected static void runSellerAndBuy2() {

		new Thread(new Seller("35"), "Seller-1").start();
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		new Thread(new Seller("36")).start();
		new Thread(new Seller("37")).start();
		new Thread(new Seller("38")).start();
		new Thread(new Seller("39")).start();

		new Thread(new Buyer("47", "35",true)).start();
	}
	
	/**
	 * 5个卖家，5个买家(全部购买第一个买家的商品)
	 */
	protected static void runSellerAndBuy3() {
		
		new Thread(new Seller("35"), "Seller-1").start();
//		try {
//			Thread.sleep(100);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		new Thread(new Seller("36")).start();
		new Thread(new Seller("37")).start();
		new Thread(new Seller("38")).start();
		new Thread(new Seller("39")).start();

		new Thread(new Buyer("45", "35")).start();
		new Thread(new Buyer("46", "35")).start();
		new Thread(new Buyer("47", "35")).start();
		new Thread(new Buyer("48", "35")).start();
		new Thread(new Buyer("49", "35")).start();
	}
	
}
