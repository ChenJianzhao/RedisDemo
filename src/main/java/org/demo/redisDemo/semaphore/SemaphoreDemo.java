package org.demo.redisDemo.semaphore;

import java.util.Calendar;
import java.util.List;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ZParams;

public class SemaphoreDemo {

	public static void main(String[] args) {
		
		Jedis conn = new Jedis("localhost");
		
		String semname = "semaphore:remote"; 
		String id = acquireFairSemaphore(conn, semname, 5, 10);
		System.out.println(id);
		
		System.out.println(releaseFairSemaphore(conn, semname,id));
		
		System.out.println(refreshFairSemaphore(conn,semname,"cd72c69e-ff95-4af5-b5b6-1064045bc42c"));
		
	}
	
	
	public static String acquireFairSemaphore(Jedis conn, String semname, Integer limit, Integer timeout) {
		
		if( timeout == null ) 
			timeout = 10;
		if(limit==null)
			limit = 5;
			
		String identifier = UUID.randomUUID().toString();
		String czset = semname + ":owner";
		String ctr = semname + ":counter";
		
		long now = System.currentTimeMillis();
		Pipeline pipe = conn.pipelined();
		pipe.multi();
		
		// 移除过期信号量
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.SECOND, -timeout);
		pipe.zremrangeByScore(semname, 0, (double)cal.getTimeInMillis());
		ZParams param = new ZParams();
		param.weightsByDouble(1,0); 	// 信号量拥有者 权重1，过期集合 0
		pipe.zinterstore(czset,param,czset,semname); // 同步更新信号量拥有者 
		
		// 计数器自增
		pipe.incr(ctr);
		
		pipe.exec();
		List<Object> counterResult = pipe.syncAndReturnAll();
		long counter = (Long)((List)counterResult.get(4)).get(2);

		pipe.multi();
		// 尝试获取信号量
		pipe.zadd(semname, System.currentTimeMillis(), identifier);
		pipe.zadd(czset, counter, identifier);
		
		// 检查排名来判断客户端是否取得信号量
		pipe.zrank(czset, identifier);
		pipe.exec();
		
		List<Object> result = pipe.syncAndReturnAll();
		long rank = (Long)((List)result.get(4)).get(2);
		
		if(rank<limit){
			return identifier;
		}else{
			pipe.multi();
			pipe.zrem(semname, identifier);
			pipe.zrem(czset, identifier);
			pipe.exec();
			pipe.sync();
			return null;
		}
	}
	
	public static boolean releaseFairSemaphore(Jedis conn, String semname, String identifier) {
		Pipeline pipe = conn.pipelined();
		pipe.multi();
		pipe.zrem(semname, identifier);
		pipe.zrem(semname + ":owner", identifier);
		pipe.exec();
		List<Object> result = pipe.syncAndReturnAll();
		if(result.get(3)!=null)
			return true;
		else return false;
	}
	
	public static boolean refreshFairSemaphore(Jedis conn, String semname, String identifier) {
		// 添加成功，客户端已失去信号量
		if( conn.zadd(semname, (double)System.currentTimeMillis(), identifier) == 1) {
			releaseFairSemaphore(conn, semname, identifier);
			return false;
		}else{
			// 添加失败，更新了 键的分值
			return true;
		}
	}
}