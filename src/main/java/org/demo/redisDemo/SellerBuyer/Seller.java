package org.demo.redisDemo.SellerBuyer;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.demo.redisDemo.lock.LockUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * 卖家
 * @author pc
 *
 */
public class Seller implements Runnable {
	
	protected int retryCounter = 0;
	int buyCount = 2000000;
	String  sellerid = "";
	boolean useLock = false;
	
	SimpleDateFormat format = new SimpleDateFormat("yyyy:MM:dd-hh:mm:ss");

	public Seller(String sellerid) {
		this.sellerid = sellerid;
	}
	
	public Seller(String sellerid, boolean useLock) {
		this.sellerid = sellerid;
		this.useLock = useLock;
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
				if(!useLock)
					listItem(conn,itemid+"",sellerid,1);
				else
					listItemWithLock(conn, itemid+"", sellerid, 1);
				
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
//				retryCounter++;
				continue;
			}finally{
			}
		}
		
		return false;
	}
	
	public  boolean  listItemWithLock(Jedis conn, String itemid, String sellerid, int price) {
		String inventory ="inventory:" + sellerid;
		String item = itemid + "." + sellerid;
		
		// 设置重试超时时间为5s
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.SECOND, 5);
		Date timeout = cal.getTime();
		
		Pipeline pipe = conn.pipelined();
		
		while(System.currentTimeMillis() < timeout.getTime()){
			
			/*
			 * 获得锁
			 */
			String lock = LockUtil.acquireLock(conn, "market:", 3, 5);
			if(lock==null)
				continue;
			
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
//				retryCounter++;
				continue;
			}finally{
				
				LockUtil.releaseLock(conn, "market:", lock);
			}
		}
		
		return false;
	}
}
