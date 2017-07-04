package org.demo.redisDemo.lock;

import redis.clients.jedis.Jedis;

import java.util.Calendar;
import java.util.UUID;

/**
 * Created by cjz on 2017/7/4.
 */
public class LockDemo {


    protected String acquireLock(Jedis conn, String lockname, int timeout) {

        // 标志本客户端唯一锁标志
        String identifier = UUID.randomUUID().toString();

        String lockkey = "lock:" + lockname;

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.SECOND, timeout);

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
}
