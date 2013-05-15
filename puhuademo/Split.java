/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package puhuademo;

import backtype.storm.tuple.Values;
import java.io.Serializable;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author root
 */
public  class Split extends BaseFunction {
    
       public int i = 0;
        @Override
        public void execute(TridentTuple tt, TridentCollector tc) {
            i++;
           if(i % 10000 == 0)
                System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@" + i++);
            tc.emit(new Values("55", 1));  
        }
        
    }
