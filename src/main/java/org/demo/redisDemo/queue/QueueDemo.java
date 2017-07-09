package org.demo.redisDemo.queue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import org.demo.redisDemo.lock.LockUtil;
import org.json.JSONObject;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

public class QueueDemo {

    protected static final String queueHigh = "queue:high";
    protected static final String queueMiddle = "queue:middle";
    protected static final String queueLow = "queue:low";

    public static String[] QUEQUS = new String[]{queueHigh, queueMiddle, queueLow};
    public static String[] CALLBACKS = new String[]{"sendEmail"};

	public static void main(String[] args) {

		Jedis conn = new Jedis("localhost");
		
		sendSolEmailViaQueue(conn,"seller","item",(double)188,"buyer");
		
//		workerWatchQueue(conn, new String[]{queueKey}, callbacks);

        new Thread(new QueueWorker()).start();
        new Thread(new PollWorker()).start();
	}

	/**
     * 任务执行线程
	 */
	static class QueueWorker implements Runnable{

        public void run() {
            Jedis conn = new Jedis("localhost");
            workerWatchQueue(conn, QueueDemo.QUEQUS ,QueueDemo.CALLBACKS);
        }
    }

    /**
     * 延迟任务拉取线程
     */
	static class PollWorker implements Runnable{

        public void run() {
            Jedis conn = new Jedis("localhost");
            poll_queue(conn);
        }
    }


	/**
	 * 待发送邮件入队
	 * @param conn
	 * @param seller
	 * @param item
	 * @param price
	 * @param buyer
	 */
	public static void sendSolEmailViaQueue(Jedis conn, String seller, String item, Double price, String buyer) {
		
		String callback = "sendEmail";

		Map<String,Object> data = new HashMap<String,Object>();
		data.put("callback", callback);

		Map<String,Object> args = new HashMap<String,Object>();
		args.put("sellerid", seller);
		args.put("itemid", item);
		args.put("price", price);
		args.put("buyerid", buyer);
		args.put("time", System.currentTimeMillis());
		data.put("args", args);

		String jsonObj = JSONObject.valueToString(data);
//		conn.rpush(queueKey, jsonObj);

		String queue = "queue:high";
		executeLater(conn, queue, callback, args, 5l) ;

	}

	/**
	 * 从队列中取出邮件发送
	 * @param conn
	 */
	public static void processSoldEmailQueue(Jedis conn) {
		
		String queueKey = "queue:email";
		
		while(true) {
			List<String > packed = conn.blpop(3,queueKey);
			
			if(packed.size() == 0) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }
			else {
				JSONObject jsonObj = new JSONObject(packed.get(1));
				Map<String,Object> email = jsonObj.toMap();

				// 发送邮件
				sendEmail(email);
				break;
			}
		}
	}

	public static void  sendEmail(Map<String,Object> email) {
		System.out.println(email);
	}

	/**
	 * 任务优先级队列，根据优先级取出队列中任务执行相应的回调函数
	 * @param conn
     * @param queues 传入多个队列，实现优先级
     * @param callbacks	提供的毁掉函数列表
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException
	 * @throws IllegalAccessException
	 */
	public static void workerWatchQueue(Jedis conn, String[] queues, String[] callbacks){

		while(true) {
		    List<String> packed = null;
			// 传入多个队列，实现优先级
			Object result = conn.blpop(3,queues);
			if(result instanceof List)
                packed = (List)result;

			if(packed!=null && packed.size() == 0) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				continue;
			}

			JSONObject jsonObj = new JSONObject(packed.get(1));
			Map<String,Object> data = jsonObj.toMap();

			String callback = (String)data.get("callback");
			Object email = data.get("args");

			try {
				Method method = QueueDemo.class.getMethod(callback, new Class[]{Map.class});
				method.invoke(QueueDemo.class, email);
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}

		}
	}

    /**
     * 将需要延迟执行的任务添加到任务延迟有序集合
     * @param conn
     * @param queue
     * @param callback
     * @param args
     * @param delay
     * @return
     */
	public static String executeLater(Jedis conn, String queue, String callback, Map args, Long delay) {
		if (delay==null) delay = 0l;

		String delayed = "delayed:";
		String identifier = UUID.randomUUID().toString();
		Map<String, Object> data = new HashMap<String, Object>();
		data.put("identifier", identifier);
		data.put("queue", queue);
		data.put("callback", callback);
		data.put("args", args);

		JSONObject jsonObj = new JSONObject(data);
		jsonObj.toString();

		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.SECOND, delay.intValue());
		if(delay>0)
			conn.zadd(delayed, cal.getTimeInMillis(), jsonObj.toString());
		else
			conn.rpush(queue, jsonObj.toString());

		return identifier;

	}

	/**
	 * 从延迟执行队列拉取任务执行
	 * @param conn
	 */
	public static void poll_queue(Jedis conn) {

		String delayed = "delayed:";
		while(true) {
			Set<Tuple> result = conn.zrangeWithScores(delayed, 0,0);
			if(result == null || result!=null && result.size()==0) {

			    // 队列中暂时没有任务，短暂休眠后继续读取
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }
			else{
			    // 任务还未达到执行的时间
                Tuple tuple = (Tuple)new ArrayList(result).get(0);
			    if(tuple.getScore() > System.currentTimeMillis())
			        continue;

				String item = tuple.getElement();
				JSONObject jsonObj = new JSONObject(item);
				Map<String,Object> data = (Map<String,Object>)jsonObj.toMap();
				String identifier = (String)data.get("identifier");
				String queue = (String)data.get("queue");
				String name = (String)data.get("name");
				Object args = data.get("args");

				String lock = LockUtil.acquireLock(conn, identifier, 3, 5);

				try {
					if(lock == null)
						continue;
					else {
						conn.zrem(delayed, item);
						conn.rpush(queue, item);
					}
				}finally {
					LockUtil.releaseLock(conn, identifier, lock);
				}

			}
		}
	}
}
