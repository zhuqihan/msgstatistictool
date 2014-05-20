package msg.statistic.handle.tool;

import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.apache.hadoop.hive.metastore.msg.MSGFactory.DDLMsg;
import org.mortbay.log.Log;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

public class ReceiveMsgTool {
  
  public static void main(String[] args) throws MetaClientException {
    
      HandleMsg hlm = new HandleMsg("127.0.0.1:4181", "test", "msgTool");
//	  if(args.length == 0)
//	  {
//		  System.out.println("please give zk address！");
//		  System.exit(0);
//	  }
//      HandleMsg hlm = new HandleMsg(args[0], "meta-test", "MsgToolzqhmm");
      hlm.time();
      hlm.handle();     
      System.out.println("Message handle is completing.");
  }

  
  public static class HandleMsg {
    final MetaClientConfig metaClientConfig = new MetaClientConfig();
    final ZKConfig zkConfig = new ZKConfig();
    private String zkaddr;
    private String topic;
    private String group;
    private MsgStatistics mst = new MsgStatistics();
    
    public HandleMsg(String zkaddr, String topic, String group) {
    	this.zkaddr = zkaddr;
    	this.topic = topic;
    	this.group = group;
    }
    
    public void time(){
    	Timer timer = new Timer(true);
    	timer.schedule(new java.util.TimerTask() { 
    		public void run(){
    			msgStatistics();
    		}},30*1000, 60*1000);
    }

    public void msgStatistics(){
    	System.out.println();
    	System.out.println("------------------------------------------------------------------------------------------------");
        System.out.println("This is the messages from 0 to " + mst.getMessageIndexAll() +  " 's statistics:");
        System.out.println("The first file has been created at " + mst.getFirstFileTime());
        System.out.println("------------------------------------------------------------------------------------------------");
        System.out.format("%10s%35s%15s%n","MsgID:1001","createDb MsgCounts:", mst.getCreateDbIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1002","alterDb MsgCounts:" , mst.getAlterDbIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1003","alterDbparam MsgCounts:" , mst.getAlterDbparamIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1004","dropDb MsgCounts:" , mst.getAlterDbparamIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1101","createTbl MsgCounts:" , mst.getCreateTblIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1102","alterTblName MsgCounts:" , mst.getAlterTblNameIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1103","alterTblDistri MsgCounts:" , mst.getAlterTblDistriIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1104","alterTblPart MsgCounts:" , mst.getAlterTblPartIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1105","alterTblSplit MsgCounts:" , mst.getAlterTblSplitIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1201","alterTblDelcol MsgCounts:" , mst.getAlterTblDelcolIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1202","alterTblAddcol MsgCounts:" , mst.getAlterTblAddcolIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1203","alterTblcolName MsgCounts:" , mst.getAlterTblcolNameIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1204","alterTblcolType MsgCounts:" , mst.getAlterTblcolTypeIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1205","alterTblcolLen MsgCounts:" , mst.getAlterTblcolLenIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1206","alterTblParam MsgCounts:" , mst.getAlterTblParamIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1207","dropTbl MsgCounts:" , mst.getDropTblIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1208","TblBusiChange MsgCounts:" , mst.getTblBusiChangeIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1301","createPart MsgCounts:" , mst.getCreatePartIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1302","alterPart MsgCounts:" , mst.getAlterPartIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1303","delPart MsgCounts:" , mst.getDelPartIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1304","addPartFile MsgCounts:" , mst.getAddPartFileIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1305","alterPartFile MsgCounts:" , mst.getAlterPartFileIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1306","repFileChange MsgCounts:" , mst.getRepFileChangeIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1307","staFileChange MsgCounts:" , mst.getStaFileChanfeIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1308","repFileOnOff MsgCounts:" , mst.getRepFileOnOffIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1309","delPartFile MsgCounts:" , mst.getDelPartFileIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1310","fileUsrSetRepChan MsgCounts:" , mst.getFileUsrSetRepChanIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1401","createInd MsgCounts:" , mst.getCreateIndIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1402","alterInd MsgCounts:" , mst.getAlterIndIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1403","alterIndParm MsgCounts:" , mst.getAlterDbparamIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1404","delInd MsgCounts:" , mst.getDelIndIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1405","createPartInd MsgCounts:" , mst.getCreatePartIndIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1406","alterPartInd MsgCounts:" , mst.getAlterPartIndIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1407","delPartInd MsgCounts:" , mst.getDelPartIndIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1408","createPartIndFile MsgCounts:" , mst.getCreatePartIndFileIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1409","alterPartIndFile MsgCounts:" , mst.getAlterPartIndFileIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1413","delPartIndFile MsgCounts:" , mst.getDelPartIndFileIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1501","createNode MsgCounts:" , mst.getCreateNodeIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1502","delNode MsgCounts:" , mst.getDelNodeIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1503","failNode MsgCounts:" , mst.getFailNodeIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1504","backNode MsgCounts:" , mst.getBackNodeIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1601","createSchema MsgCounts:" , mst.getCreateSchemaIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1602","modifyScheName MsgCounts:" , mst.getModifyScheNameIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1603","modifyScheDelcol MsgCounts:" , mst.getModifyScheDelcolIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1604","modifyScheAddcol MsgCounts:" , mst.getModifyScheAddcolIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1605","modifyScheAltcolName MsgCounts:" , mst.getModifyScheAltcolNameIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1606","modifyScheAltcolType MsgCounts:" , mst.getModifyScheAltcolTypeIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1607","modifySchemaParam MsgCounts:" , mst.getModifySchemaParamIndex());
        System.out.format("%10s%35s%15s%n","MsgID:1608","delSchema MsgCounts:" , mst.getDelSchemaIndex());
        System.out.format("%10s%35s%15s%n","MsgID:2001","DDLDirectDw1 MsgCounts:" , mst.getDDLDirectDw1Index());
        System.out.format("%10s%35s%15s%n","MsgID:2002","DDLDirectDw2 MsgCounts:" , mst.getDDLDirectDw2Index());
        System.out.format("%10s%35s%15s%n","MsgID:3001","createNg MsgCounts:" , mst.getCreateNgIndex());
        System.out.format("%10s%35s%15s%n","MsgID:3002","modifyNg MsgCounts:" , mst.getModifyNgIndex());
        System.out.format("%10s%35s%15s%n","MsgID:3003","delNg MsgCounts:" , mst.getDelNgIndex());
        System.out.format("%10s%35s%15s%n","MsgID:3004","alterNg MsgCounts:" , mst.getAlterNgIndex());
        System.out.format("%10s%35s%15s%n","MsgID:4001","createFile MsgCounts:" , mst.getCreateFileIndex());
        System.out.format("%10s%35s%15s%n","MsgID:4002","delFile MsgCounts:" , mst.getDelFileIndex());
        System.out.format("%10s%35s%15s%n","MsgID:5101","grantGlobal MsgCounts:" , mst.getGrantGlobalIndex());
        System.out.format("%10s%35s%15s%n","MsgID:5102","grantDB MsgCounts:" , mst.getGrantDBIndex());
        System.out.format("%10s%35s%15s%n","MsgID:5103","grantTbl MsgCounts:" , mst.getGrantTblIndex());
        System.out.format("%10s%35s%15s%n","MsgID:5104","grantSchema MsgCounts:" , mst.getGrantSchemaIndex());
        System.out.format("%10s%35s%15s%n","MsgID:5105","grantPart MsgCounts:" , mst.getGrantPartIndex());
        System.out.format("%10s%35s%15s%n","MsgID:5106","grantPartClo MsgCounts:" , mst.getGrantPartCloIndex());
        System.out.format("%10s%35s%15s%n","MsgID:5107","grantTblCol MsgCounts:" , mst.getGrantTblColIndex());
        System.out.format("%10s%35s%15s%n","MsgID:5201","revokeGlobal MsgCounts:" , mst.getRevokeGlobalIndex());
        System.out.format("%10s%35s%15s%n","MsgID:5202","revokeDB MsgCounts:" , mst.getRevokeDBIndex());
        System.out.format("%10s%35s%15s%n","MsgID:5203","revokeTbl MsgCounts:" , mst.getRevokeTblIndex());
        System.out.format("%10s%35s%15s%n","MsgID:5204","revokeSchema MsgCounts:" , mst.getRevokeSchemaIndex());
        System.out.format("%10s%35s%15s%n","MsgID:5205","revokePart MsgCounts:" , mst.getRevokePartIndex());
        System.out.format("%10s%35s%15s%n","MsgID:5206","revokePartClo MsgCounts:" , mst.getRevokePartColIndex());
        System.out.format("%10s%35s%15s%n","MsgID:5207","revokeTblCol MsgCounts:" , mst.getRevokeTblColIndex());
        System.out.println("------------------------------------------------------------------------------------------------");
        System.out.println("The average time of a file created to closed is :" + mst.avgFileCreCloTime());
        System.out.println("The average time of a file created to deleted is :" + mst.avgFileCreDelTime());
        System.out.println("The average time of a file closed to replicated is :" + mst.avgFileCloRepTime());
        System.out.println("------------------------------------------------------------------------------------------------");
        System.out.println("These are problem messages which the parameters are not match to the document:");
        System.out.println("");
        
        ConcurrentHashMap<Long,String> probMsg = mst.getProbMsg();
        for(Long msgkey : probMsg.keySet()){
          System.out.println(msgkey + "----" + probMsg.get(msgkey));
        }
        System.out.println("");
      }
    
    public void handle() throws MetaClientException {
      // 设置zookeeper地址
      zkConfig.zkConnect = zkaddr;
      metaClientConfig.setZkConfig(zkConfig);
      // New session factory,强烈建议使用单例
      MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
      // create consumer,强烈建议使用单例

      // 生成处理线程
      ConsumerConfig cc = new ConsumerConfig(group);
      MessageConsumer consumer = sessionFactory.createConsumer(cc);
    
      // 订阅事件，MessageListener是事件处理接口
      consumer.subscribe(topic, 1024, new MessageListener() {

        @Override
        public Executor getExecutor() {
          return null;
        }

        @Override
        public void recieveMessages(final Message message) {
          String data = new String(message.getData());
          DDLMsg msg = DDLMsg.fromJson(data);
          Log.info("DDLMSG" + msg.getEvent_id());
          mst.handleMsg(msg);
        }

      });
      consumer.completeSubscribe();
    }
    
  }
}
