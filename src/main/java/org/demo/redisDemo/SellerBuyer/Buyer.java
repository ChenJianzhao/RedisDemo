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
public class Buyer implements Runnable {

	protected int retryCounter = 0;
	int buyCount = 2000000;
	String buyerid = "";
	String sellerid = "";
	boolean useLock = false;
	
	SimpleDateFormat format = new SimpleDateFormat("yyyy:MM:dd-hh:mm:ss");
	
	public  Buyer(String buyerid , String sellerid) {
		this.buyerid = buyerid;
		this.sellerid = sellerid;
	}
	
	public  Buyer(String buyerid , String sellerid, boolean useLock) {
		this.buyerid = buyerid;
		this.sellerid = sellerid;
		this.useLock = useLock;
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
				
				if(!useLock)
					purchaseItem(conn,buyerid,itemid+"",sellerid,1);
				else
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
				if(count!=0)
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
			
			/*
			 * 获得锁
			 */
			String lock = LockUtil.acquireLock(conn, market, 3, 5);
			if(lock==null)
				continue;
			
			try{
				
//				pipe.watch(market, buyer);
				pipe.zscore(market, item);
				pipe.hget(buyer,"funds");
				List<Object> check = pipe.syncAndReturnAll();
				Double price = (Double)check.get(0);
				int funds = Integer.parseInt((String)check.get(1));
				
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
