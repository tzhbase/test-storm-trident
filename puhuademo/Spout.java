/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package puhuademo;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.List;
import java.util.Map;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

/**
 *
 * @author root
 */
public class Spout  implements IBatchSpout {

    Fields fields;
    List<Object>[] outputs;
    int maxBatchSize;
    int kk;
    
    public Spout(Fields fields, int maxBatchSize, List<Object>... outputs) {
        this.fields = fields;
        this.outputs = outputs;
        this.maxBatchSize = maxBatchSize;
    }
    
    int index = 0;
    boolean cycle = false;
    
    public void setCycle(boolean cycle) {
        this.cycle = cycle;
    }
    
    @Override
    public void open(Map conf, TopologyContext context) {
        index = 0;
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        //Utils.sleep(2000);
        //System.out.println("第" + ++kk + "次调用emit...................................");
        //System.out.println("time: " + System.currentTimeMillis());
        
        if(outputs == null)return;
        if(index>=outputs.length && cycle) {
            index = 0;
        }
        
//        for(int i=0; index < outputs.length && i < maxBatchSize; index++, i++) {
//            collector.emit(outputs[index]);
//        }
        for(int i=0; i < 100; i++) {
            collector.emit(new Values("xx"));
        }
        outputs = null;
//        try{
//            for(int i=0; i < 1000000; index++) {
//                collector.emit(outputs[index]);
//                if((index+1) == outputs.length)
//                    index = 0;
//            }
//        }catch(Exception e){
//            System.out.println(e + "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//        }
    }

    @Override
    public void ack(long batchId) {
        
    }

    @Override
    public void close() {
        System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
    }

    @Override
    public Map getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    @Override
    public Fields getOutputFields() {
        return fields;
    }
    
}
