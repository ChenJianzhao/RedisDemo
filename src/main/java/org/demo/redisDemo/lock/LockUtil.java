package org.demo.redisDemo.lock;

import java.util.Calendar;
import java.util.List;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * Created by cjz on 2017/7/4.
 */
public class LockUtil {


	public static String acquireLock(Jedis conn, String lockname, int acquireTimeout) {

        // 标志本客户端唯一锁标志
        String identifier = UUID.randomUUID().toString();

        String lockkey = "lock:" + lockname;

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.SECOND, acquireTimeout);

        while( System.currentTimeMillis() <  cal.getTime().getTime()) {

            if(conn.setnx(lockkey, identifier) == 1) {
                return identifier;
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return null;
    }
    
	public static String acquireLock(Jedis conn, String lockname, int acquireTimeout, int lockTimeout) {
		
		// 标志本客户端唯一锁标志
        String identifier = UUID.randomUUID().toString();

        String lockkey = "lock:" + lockname;

        Calendar acquireCal = Calendar.getInstance();
        acquireCal.add(Calendar.SECOND, acquireTimeout);
        
        Calendar lockCal = Calendar.getInstance();
        lockCal.add(Calendar.SECOND, lockTimeout);

        while( System.currentTimeMillis() < acquireCal.getTime().getTime()) {

            if(conn.setnx(lockkey, identifier) == 1) {
            	// 设置过期时间
            	conn.expire(lockkey, lockTimeout);
                return identifier;
            }else if ( conn.ttl(lockkey) == null ){
            	conn.expire(lockkey, lockTimeout);
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return null;
	}
	
    public static boolean releaseLock(Jedis conn, String lockname, String identifier) {
    	
    	Pipeline pipe = conn.pipelined();
    	String lockkey = "lock:" + lockname;
    	
    	while(true) {
    		try{
    			pipe.watch(lockkey);
    			pipe.get(lockkey);
    			List<Object> result = pipe.syncAndReturnAll();
    			if( result.get(0).equals(identifier)) {
    				pipe.multi();
    				pipe.del(lockkey);
    				pipe.exec();
    				List<Object> delResl = pipe.syncAndReturnAll();
    				if(delResl.get(2) == null)
    					continue;
    				else return true;
    			}
    		}catch(Exception e) {
    			continue;
    		}
    	}
    }
}
