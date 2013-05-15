/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package puhuademo.test;

/**
 *
 * @author root
 */
public class TestStackOverflowError {
    
    public static void main(String args[]){
        new TestStackOverflowError().loop();
    }
    
    public void loop(){
         while(true){
             System.out.println(System.currentTimeMillis());
             loop();
         }
    }
}
