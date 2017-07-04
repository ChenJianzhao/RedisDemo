package org.demo.redisDemo;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class SimpleMarketDemo {


	public static void main(String[] args) {

		SimpleDateFormat format = new SimpleDateFormat("yyyy:MM:dd-hh:mm:ss");

		Date initStart = new Date(System.currentTimeMillis());
		System.out.println("init start: \t" + format.format(initStart));
		initData();
		Date initEnd = new Date(System.currentTimeMillis());
		System.out.println("init end: \t" + format.format(initEnd));
		System.out.println("init cost: \t" + (initEnd.getTime()-initStart.getTime()) + "ms");
		System.out.println("");
		
		new Thread(new Seller()).start();
//		try {
//			Thread.sleep(100);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		new Thread(new Buyer()).start();
	}
	
	/**
	 * 初始化商品数据
	 */
	public static void initData() {
		
		Jedis conn = null;
		String seller = "inventory:37";
		try{
			conn = new Jedis("localhost");
			Pipeline pipe = conn.pipelined();

			pipe.multi();
			for(int itemid = 0;itemid<50; itemid++) {
				pipe.sadd(seller, String.valueOf(itemid));
			}
			pipe.exec();
			pipe.sync();
			System.out.println("init count: \t" + conn.scard(seller));
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			conn.disconnect();
		}
	}
	
	/**
	 * 
	 * @author pc
	 *
	 */
	static class Buyer implements Runnable {

		protected int retryCounter = 0;
		SimpleDateFormat format = new SimpleDateFormat("yyyy:MM:dd-hh:mm:ss");
		
		public void run() {
			Jedis conn = new Jedis("localhost");
			
			try{
				Date buyStart = new Date(System.currentTimeMillis());
				System.out.println("buy start: \t" + format.format(buyStart));
				
				for(int itemid = 0;itemid<50; itemid++) {
					purchaseItem(conn,"47",itemid+"","37",1);
				}
				
				Date buyEnd = new Date(System.currentTimeMillis());
				System.out.println("buy end: \t" + format.format(buyEnd));
				System.out.println("buy cost: \t" + (buyEnd.getTime()-buyStart.getTime()) + "ms");
				System.out.println("buy Count: \t" +  conn.scard("inventory:47"));
				System.out.println("buy retryCounter: \t" +  retryCounter);
				System.out.println("");
			}finally{
				conn.disconnect();
			}
			
			
		}
		
		
		public boolean purchaseItem(Jedis conn, String buyerid, String itemid, String sellerid, int lprice) {
			String inventory ="inventory:" + buyerid;
			String market = "market:";
			String item = itemid + "." + sellerid;
			String buyer = "users:" + buyerid;
			String seller = "users:" + sellerid;
			
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.SECOND, 5);
			Date timeout = cal.getTime();
			
			Pipeline pipe = conn.pipelined();
			
			while(System.currentTimeMillis() < timeout.getTime()){
				
				try{
//					pipe.watch(market, inventory, seller, buyer);
					pipe.zscore(market, item);
					pipe.hget(buyer,"funds");
					List<Object> result = pipe.syncAndReturnAll();
					Double price = (Double)result.get(0);
					int funds = Integer.parseInt((String)result.get(1));
					
					if( price == null || 
							price != null && (lprice!=price || price>funds)) {
						retryCounter++;
						System.out.println("retry do not have item: \t" + itemid);
//						continue;
					}else {
//						pipe.watch(market, inventory, seller, buyer);
						pipe.multi();
						pipe.hincrBy(seller, "funds", price.longValue());
						pipe.hincrBy(buyer, "funds", -price.longValue());
						pipe.sadd(inventory,itemid);
						pipe.zrem(market,item);
						pipe.exec();
						
						System.out.println(pipe.syncAndReturnAll());
						return true;
					}
					
				}catch(Exception e) {
					// retry
					retryCounter++;
					System.out.println("retry buy item: \t" + itemid);
//					continue;
				}finally{
//					conn.disconnect();
				}
			}
			
			return false;
		}
	}
	
	static class Seller implements Runnable {
		
		
		protected int retryCounter = 0;
		SimpleDateFormat format = new SimpleDateFormat("yyyy:MM:dd-hh:mm:ss");
		
		public void run() {
			Jedis conn = new Jedis("localhost");
			
			try{
				Date sellStart = new Date(System.currentTimeMillis());
				System.out.println("sell start: \t" + format.format(sellStart));
				
				for(int itemid = 0;itemid<50; itemid++) {
					listItem(conn,itemid+"","37",1);
				}
				
				Date sellEnd = new Date(System.currentTimeMillis());
				System.out.println("sell end: \t" + format.format(sellEnd));
				System.out.println("sell cost: \t" + (sellEnd.getTime()-sellStart.getTime()) + "ms");
//			System.out.println("sell Count: \t" +  conn.zcard("inventory:37"));
				System.out.println("sell retryCounter: \t" +  retryCounter);
				System.out.println("");
			}finally{
				conn.disconnect();
			}
			
		}
		
		public  boolean  listItem(Jedis conn, String itemid, String sellerid, int price) {
			String inventory ="inventory:" + sellerid;
			String item = itemid + "." + sellerid;
			
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.SECOND, 5);
			Date timeout = cal.getTime();
			
			Pipeline pipe = conn.pipelined();
			
			while(System.currentTimeMillis() < timeout.getTime()){
				
				try{
					pipe.watch(inventory);
					pipe.sismember(inventory, itemid);
					List<Object> result = pipe.syncAndReturnAll();
					if(!(Boolean)result.get(1)) {
						retryCounter++;
						continue;
					}else {
						pipe.multi();
						pipe.zadd("market:", price, item);
						pipe.srem(inventory, itemid);
						pipe.exec();
						pipe.syncAndReturnAll();
						return true;
					}
					
				}catch(Exception e) {
					// retry
					retryCounter++;
					continue;
				}finally{
//					conn.disconnect();
				}
			}
			
			return false;
		}

	}
	
	
}
