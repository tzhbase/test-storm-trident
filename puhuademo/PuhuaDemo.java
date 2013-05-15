package puhuademo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import puhuademo.state.MysqlState;
import puhuademo.state.MysqlStateConfig;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateType;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

/**
 * demo
 *
 * @author root
 */
public class PuhuaDemo {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        new PuhuaDemo().execute(args);
    }

    public void execute(String[] args) {

        Spout spout = new Spout(new Fields("record"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));

        spout.setCycle(true);

//        final MysqlStateConfig config1 = new MysqlStateConfig();
//        config1.setUrl("jdbc:mysql://localhost:3306/test?user=root&password=ttt");
//        config1.setTable("storm_state_trans");
//        config1.setKeyColumns(new String[]{"pkey"});
//        config1.setValueColumns(new String[]{"sum"});
//        config1.setType(StateType.TRANSACTIONAL);
//        config1.setCacheSize(1);
        
        long start = System.currentTimeMillis();
        
        TridentTopology topology = new TridentTopology();
        TridentState state = topology.newStream("spout_1", spout).each(new Fields("record"), new Split(), new Fields("account_time", "amount"))
                .parallelismHint(1)
                .groupBy(new Fields("account_time")) //default key
                //.persistentAggregate(MysqlState.newFactory(config1), new Fields("amount"), new Sum(), new Fields("sum"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("amount"), new Sum(), new Fields("sum"))
                .parallelismHint(1);

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(5);

        try {
            
            if(args.length > 0){
                StormSubmitter.submitTopology(args[0], conf, topology.build());
            }
            else {
                
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("test", conf, topology.build());

                Thread.sleep(1000*30);

                System.out.println("topology stop: " + (System.currentTimeMillis() - start)/1000 + " 秒~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
                cluster.shutdown();
            }

        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println(e);
        }
    }
}
