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

		Date initStart = new Date(System.currentTimeMillis());
		System.out.println("init start: \t" + format.format(initStart));
		initData();
		Date initEnd = new Date(System.currentTimeMillis());
		System.out.println("init end: \t" + format.format(initEnd));
		System.out.println("init cost: \t" + (initEnd.getTime()-initStart.getTime()) + "ms");
		System.out.println("");
		
//		new Thread(new Seller("35"), "Seller-1").start();
//		try {
//			Thread.sleep(100);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		new Thread(new Seller("36")).start();
		new Thread(new Seller("37")).start();
//		new Thread(new Seller("38")).start();
//		new Thread(new Seller("39")).start();

		new Thread(new Buyer("47", "37")).start();
	}

	/**
	 * 初始化单个卖家商品数据
	 */
	public static void initData() {
		
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
	}
	
	/**
	 * 初始化多个卖家商品数据
	 */
	public static void initBatchData() {
		
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
	}
	
	/**
	 * 卖家
	 * @author pc
	 *
	 */
	static class Buyer implements Runnable {

		protected int retryCounter = 0;
		int buyCount = 2000000;
		String buyerid = "";
		String sellerid = "";
		
		SimpleDateFormat format = new SimpleDateFormat("yyyy:MM:dd-hh:mm:ss");
		
		public  Buyer(String buyerid , String sellerid) {
			this.buyerid = buyerid;
			this.sellerid = sellerid;
		}
		
		public void run() {
			Jedis conn = new Jedis("localhost");
			
			try{
				Date buyStart = new Date(System.currentTimeMillis());
				System.out.println("buyer:" + buyerid + " buy start: \t" + format.format(buyStart));

				Calendar cal = Calendar.getInstance();
				cal.add(Calendar.SECOND, 30);
				
				// 测试30秒内购买的次数
				for(int itemid = 0;itemid<buyCount; itemid++) {
//					purchaseItem(conn,buyerid,itemid+"",sellerid,1);
					purchaseItemWithLock(conn,buyerid,itemid+"",sellerid,1);
					if( System.currentTimeMillis() >= cal.getTimeInMillis() )
						break;
				}
				
				synchronized (Buyer.class) {
					Date buyEnd = new Date(System.currentTimeMillis());
					System.out.println("buyer:" + buyerid + " buy end: \t" + format.format(buyEnd));
					System.out.println("buyer:" + buyerid + " buy cost: \t" + (buyEnd.getTime()-buyStart.getTime()) + "ms");
					long count  = conn.scard("inventory:" + buyerid);
					System.out.println("buyer:" + buyerid + " buy Count: \t" +  count);
					System.out.println("buyer:" + buyerid + " avg buy cost: \t" + (buyEnd.getTime()-buyStart.getTime())/count + "ms");
					System.out.println("buyer:" + buyerid + " buy retryCounter: \t" +  retryCounter);
					System.out.println("");
				}
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
			
			// 设置重试超时时间为5s
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.SECOND, 5);
			Date timeout = cal.getTime();
			
			Pipeline pipe = conn.pipelined();
			
			while(System.currentTimeMillis() < timeout.getTime()){
				
				try{
					pipe.watch(market, buyer);
					pipe.zscore(market, item);
					pipe.hget(buyer,"funds");
					List<Object> check = pipe.syncAndReturnAll();
					Double price = (Double)check.get(1);
					int funds = Integer.parseInt((String)check.get(2));
					
					// 商品还未放入市场
					if( price == null || 
							price != null && (lprice!=price || price>funds)) {
						continue;
					}else {
						pipe.multi();
						pipe.hincrBy(seller, "funds", price.longValue());
						pipe.hincrBy(buyer, "funds", -price.longValue());
						pipe.sadd(inventory,itemid);
						pipe.zrem(market,item);
						pipe.exec();

						List<Object> result= pipe.syncAndReturnAll();
						// Watch 的键发生变化，同步返回值为null
						if(result.get(5)==null) {
							retryCounter++;
							continue;
						}
						else {
							return true;
						}
					}
					
				}catch(Exception e) {
					// retry
					retryCounter++;
				}finally{
				}
			}
			
			return false;
		}
		
		public boolean purchaseItemWithLock(Jedis conn, String buyerid, String itemid, String sellerid, int lprice) {
			String inventory ="inventory:" + buyerid;
			String market = "market:";
			String item = itemid + "." + sellerid;
			String buyer = "users:" + buyerid;
			String seller = "users:" + sellerid;
			
			// 设置重试超时时间为5s
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.SECOND, 5);
			Date timeout = cal.getTime();
			
			Pipeline pipe = conn.pipelined();
			
			while(System.currentTimeMillis() < timeout.getTime()){
				
				String lock = LockUtil.acquireLock(conn, market, 3, 5);
				if(lock==null)
					continue;
				try{
					
//					pipe.watch(market, buyer);
					pipe.zscore(market, item);
					pipe.hget(buyer,"funds");
					List<Object> check = pipe.syncAndReturnAll();
					Double price = (Double)check.get(1);
					int funds = Integer.parseInt((String)check.get(2));
					
					// 商品还未放入市场
					if( price == null || 
							price != null && (lprice!=price || price>funds)) {
						continue;
					}else {
						pipe.multi();
						pipe.hincrBy(seller, "funds", price.longValue());
						pipe.hincrBy(buyer, "funds", -price.longValue());
						pipe.sadd(inventory,itemid);
						pipe.zrem(market,item);
						pipe.exec();

						List<Object> result= pipe.syncAndReturnAll();
						// Watch 的键发生变化，同步返回值为null
						if(result.get(5)==null) {
							retryCounter++;
							continue;
						}
						else {
							return true;
						}
					}
					
				}catch(Exception e) {
					// retry
					retryCounter++;
				}finally{
					LockUtil.releaseLock(conn, market, lock);
				}
			}
			
			return false;
		}
	}
	
	/**
	 * 卖家
	 * @author pc
	 *
	 */
	static class Seller implements Runnable {
		
		protected int retryCounter = 0;
		int buyCount = 2000000;
		String  sellerid = "";

		SimpleDateFormat format = new SimpleDateFormat("yyyy:MM:dd-hh:mm:ss");

		public Seller(String sellerid) {
			this.sellerid = sellerid;
		}
		
		public void run() {
			
			Jedis conn = new Jedis("localhost");
			
			try{
				Date sellStart = new Date(System.currentTimeMillis());
				System.out.println("seller:" + sellerid + " sell start: \t" + format.format(sellStart));
				
				Calendar cal = Calendar.getInstance();
				cal.add(Calendar.SECOND, 30);
				
				// 测试30秒内卖出的次数
				for(int itemid = 0; itemid<buyCount; itemid++) {
					listItem(conn,itemid+"",sellerid,1);
					if( System.currentTimeMillis() >= cal.getTimeInMillis() )
						break;
				}
				
				Date sellEnd = new Date(System.currentTimeMillis());
				synchronized (Seller.class) {
					System.out.println("seller:" + sellerid + " sell end: \t" + format.format(sellEnd));
					System.out.println("seller:" + sellerid + " sell cost: \t" + (sellEnd.getTime()-sellStart.getTime()) + "ms");
					long count = conn.scard("inventory:" + sellerid);
					System.out.println("seller:" + sellerid + " sell Count: \t" +  count);
					System.out.println("seller:" + sellerid + " avg sell cost: \t" + (sellEnd.getTime()-sellStart.getTime())/count + "ms");
					System.out.println("seller:" + sellerid + " sell retryCounter: \t" +  retryCounter);
					System.out.println("");
				}
			}finally{
				conn.disconnect();
			}
			
		}
		
		public  boolean  listItem(Jedis conn, String itemid, String sellerid, int price) {
			String inventory ="inventory:" + sellerid;
			String item = itemid + "." + sellerid;
			
			// 设置重试超时时间为5s
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
//					retryCounter++;
					continue;
				}finally{
				}
			}
			
			return false;
		}
	}
	
	
}
