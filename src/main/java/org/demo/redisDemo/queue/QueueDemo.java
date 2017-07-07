package org.demo.redisDemo.queue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import redis.clients.jedis.Jedis;

public class QueueDemo {
	
	public static void main(String[] args) {
	
		Jedis conn = new Jedis("localhost");
		
		sendSolEmailViaQueue(conn,"seller","item",(double)188,"buyer");
		
		processSoldEmailQueue(conn);
	}

	public static void sendSolEmailViaQueue(Jedis conn, String seller, String item, Double price, String buyer) {
		
		String queueKey = "queue:email";
		
		Map<String,Object> data = new HashMap<String,Object>();
		data.put("sellerid", seller);
		data.put("itemid", item);
		data.put("price", price);
		data.put("buyerid", buyer);
		data.put("time", System.currentTimeMillis());
		

		String jsonObj = JSONObject.valueToString(data);
		conn.rpush(queueKey, jsonObj);
	}
	
	public static void processSoldEmailQueue(Jedis conn) {
		
		String queueKey = "queue:email";
		
		while(true) {
			List<String > packed = conn.blpop(3,queueKey);
			
			if(packed.size() == 0)
				continue;
			else {
				JSONObject jsonObj = new JSONObject(packed.get(1));
				Map<String,Object> email = jsonObj.toMap();
				//
				System.out.println(email);
				break;
			}
		}
	}
	
}
