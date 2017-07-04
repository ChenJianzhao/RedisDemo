package org.demo.redisDemo.SellerBuyer;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class Inventory {

	public static void main(String[] args) {
		
		Jedis conn = new Jedis("localhost");
		
		listItem(conn,"ItemL","17",97);
		System.out.println("market:" +"\t"+		conn.zrange("market:", 0,-1));
		System.out.println("users:17"  +"\t"+	conn.hgetAll("users:17"));
		System.out.println("inventory:17"+"\t"+ conn.smembers("inventory:17"));
		System.out.println("users:27"  +"\t"+	conn.hgetAll("users:27"));
		System.out.println("inventory:27"+"\t"+ conn.smembers("inventory:27"));
		
		purchaseItem(conn,"27","ItemL","17",97);
		System.out.println("market:" + "\t"+	conn.zrange("market:", 0,-1));
		System.out.println("users:17"+ "\t"+	conn.hgetAll("users:17"));
		System.out.println("inventory:17"+"\t"+	conn.smembers("inventory:17"));
		System.out.println("users:27" + "\t"+	conn.hgetAll("users:27"));
		System.out.println("inventory:27"+"\t"+ conn.smembers("inventory:27"));
	}
	
	public static boolean  listItem(Jedis conn, String itemid, String sellerid, int price) {
		String inventory ="inventory:" + sellerid;
		String item = itemid + "." + sellerid;
		
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.SECOND, 5);
		Date timeout = cal.getTime();
		
		Pipeline pipe = conn.pipelined();
		
		while(Calendar.getInstance().getTime().compareTo(timeout) < 0){
			
			try{
				pipe.watch(inventory);
				pipe.sismember(inventory, itemid);
				List<Object> result = pipe.syncAndReturnAll();
				if(!(Boolean)result.get(1)) {
					return false;
				}else {
					pipe.multi();
					pipe.zadd("market:", price, item);
					pipe.srem(inventory, itemid);
					pipe.exec();
					pipe.sync();
					return true;
				}
				
			}catch(Exception e) {
				// retry
				continue;
			}finally{
				conn.disconnect();
			}
		}
		
		return false;
	}
	
	public static boolean purchaseItem(Jedis conn, String buyerid, String itemid, String sellerid, int lprice) {
		String inventory ="inventory:" + buyerid;
		String market = "market:";
		String item = itemid + "." + sellerid;
		String buyer = "users:" + buyerid;
		String seller = "users:" + sellerid;
		
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.SECOND, 5);
		Date timeout = cal.getTime();
		
		Pipeline pipe = conn.pipelined();
		
		while(Calendar.getInstance().getTime().compareTo(timeout) < 0){
			
			try{
				pipe.watch(market, buyer);
				pipe.zscore(market, item);
				pipe.hget(buyer,"funds");
				List<Object> result = pipe.syncAndReturnAll();
				Double price = (Double)result.get(1);
				int funds = Integer.parseInt((String)result.get(2));
				
				if(lprice!=price || price>funds) {
					return false;
				}else {
					pipe.multi();
					pipe.hincrBy(seller, "funds", price.longValue());
					pipe.hincrBy(buyer, "funds", -price.longValue());
					pipe.sadd(inventory,itemid);
					pipe.zrem(market,item);
					pipe.exec();
					pipe.sync();
					return true;
				}
				
			}catch(Exception e) {
				// retry
				continue;
			}finally{
				conn.disconnect();
			}
		}
		
		return false;
	}
}
