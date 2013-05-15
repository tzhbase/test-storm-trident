/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package backtype.storm.spout;

import backtype.storm.Config;
import java.util.Map;

/**
 *
 * @author root
 */
public class DummySpoutWaitStrategy implements ISpoutWaitStrategy {

    long sleepMillis;
    
    @Override
    public void prepare(Map conf) {
        sleepMillis = ((Number) conf.get(Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS)).longValue();
    }

    @Override
    public void emptyEmit(long streak) {
        try {
            System.out.println(sleepMillis + "----------------------------------------------@@@@@@@@@@@@@@@@");
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}