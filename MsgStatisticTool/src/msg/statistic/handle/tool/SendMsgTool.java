package msg.statistic.handle.tool;

import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.msg.MSGType;
import org.apache.hadoop.hive.metastore.msg.MSGFactory.DDLMsg;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

public class SendMsgTool {
	private static HiveConf conf = new HiveConf();
	private static final Log LOG = LogFactory.getLog(SendMsgTool.class);
	static Producer producer = null;
	private static boolean initalized = false;
	private static SendThread send = new SendThread();
	private static boolean zkfailed = false;
	private static ConcurrentLinkedQueue<DDLMsg> queue = new ConcurrentLinkedQueue<DDLMsg>();
	private static ConcurrentLinkedQueue<DDLMsg> failed_queue = new ConcurrentLinkedQueue<DDLMsg>();
	static int times = 3;
	
	public static void startProducer() throws MetaClientException {
		if (!initalized) {
			initalize();
			new Thread(send).start();
		}
	}
	
	private static void initalize() throws MetaClientException {
//		Producer.config(zkAddr);
		producer = Producer.getInstance();
		initalized = true;
		zkfailed = false;

	}
	
	private static void reconnect() throws MetaClientException {
//		Producer.config(zkAddr);
		producer = Producer.getInstance();
		initalized = true;
		zkfailed = false;
	}
	
	static class SendThread implements Runnable{
		Semaphore sem  = new Semaphore(0);
		public SendThread() {
		}
		
		public void release(){
      sem.release();
    }
		@Override
		public void run() {
			 while(true ){
	        try{
	          if(queue.isEmpty()){
	            sem.acquire();
	            if(queue.isEmpty()){
	              continue;
	            }
	          }

	          if(zkfailed)
	          {
	            try{
	              Thread.sleep(1*1000l);
	              reconnect();
	            }catch(InterruptedException e)
	            {
	            }catch(MetaClientException e){
	              zkfailed = true;
	            }

	          }
	          DDLMsg msg = queue.peek();
	          boolean succ = sendDDLMsg(msg);
	          if(!succ){
	            if(!failed_queue.contains(msg)) {
	              failed_queue.add(msg);
	            }
	          }else{

	            failed_queue.remove(queue.poll());

	            if(!failed_queue.isEmpty()){
	              while( !failed_queue.isEmpty()){//retry send faild msg,old msg should send as soon as possible.
	                DDLMsg retry_msg =failed_queue.peek();
	                if(!sendDDLMsg(retry_msg)){
	                  break;
	                }else{
	                  failed_queue.poll();
	                }
	              }
	            }
	          }
	        } catch (Exception e) {
	        	LOG.error(e,e);
	        }
	      }

		}
		
	}
	private static boolean sendDDLMsg(DDLMsg msg) {
		String jsonMsg = "";
		jsonMsg =msg.toJson();
		LOG.info("---ZQH---- send ddl msg:" + jsonMsg);
		boolean success = false;

		success = retrySendMsg(jsonMsg, times);
		return success;
	}

  private static boolean retrySendMsg(String jsonMsg,int times){
    // FIXME: if server not initialized, just return true;
    if (!initalized) {
      return true;
    }
    if(times <= 0){
      zkfailed = true;
      return false;
    }

    boolean success = false;
    try{
      success = producer.sendMsg(jsonMsg);
    }catch(InterruptedException ie){
      return retrySendMsg(jsonMsg,times-1);
    } catch (MetaClientException e) {
      LOG.error(e,e);
      return retrySendMsg(jsonMsg,times-1);
    }
    return success;
  }
  
	public static class Producer
	{
		private static Producer instance= null;
		private final MetaClientConfig metaClientConfig = new MetaClientConfig();
		private final ZKConfig zkConfig = new ZKConfig();
		private MessageSessionFactory sessionFactory = null;
		// create producer,强烈建议使用单例
		private MessageProducer producer = null;
		// publish topic
		private static String topic = "test";
		private static String zkAddr = conf.getVar(ConfVars.ZOOKEEPERADDRESS);

		private Producer() {

			// 设置zookeeper地址
			zkConfig.zkConnect = "127.0.0.1:4181" ;//zkAddr;
			metaClientConfig.setZkConfig(zkConfig);
			// New session factory,强烈建议使用单例
			connect();
		}

		private void connect() {
			try {
				sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
				producer = sessionFactory.createProducer();
				producer.publish(topic);
			} catch (MetaClientException e) {
				// LOG.error(e.getMessage());
				LOG.error(e, e);
			}
			LOG.info("Topic '" + topic + "' has been published.");
		}

		public static Producer getInstance() throws MetaClientException {
			if (instance == null) {
				instance = new Producer();
			}
			return instance;
		}

		boolean sendMsg(String msg) throws MetaClientException,
				InterruptedException {
			// LOG.debug("in send msg:"+msg);

			if (producer == null) {
				connect();
				if (producer == null) {
					return false;
				}
			}
			SendResult sendResult = producer.sendMessage(new Message(topic, msg
					.getBytes()));
			LOG.info("SendResult" + sendResult);
			LOG.info("Queue" + queue.size());
			// check result

			boolean success = sendResult.isSuccess();
			LOG.info("SendResult" + success);
			if (!success) {
				LOG.debug("Send message failed,error message:"
						+ sendResult.getErrorMessage());
			} else {
				LOG.debug("Send message successfully,sent to "
						+ sendResult.getPartition());
			}
			return success;
		}
	}

	public static void main(String[] args) {
		HashMap<String,Object> params = new HashMap<String,Object>();
		params.put("db_name","yunnan");
        params.put("table_name","t_cdr");
        long now = new Date().getTime()/1000;
		DDLMsg msg = new DDLMsg(MSGType.MSG_NEW_TALBE, -1, params, -1, 1, -1, -1, now, null, null);
		                       //event_id  object_id  msg_data  msg_id  db_id  node_id  event_time event_handler old_object_params
		queue.add(msg);
		try {
			startProducer();
		} catch (MetaClientException e) {
			e.printStackTrace();
		}
	}
}
