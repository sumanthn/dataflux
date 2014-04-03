package dataflux.common.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by sumanthn on 27/3/14.
 */
public class SimplePermitTest {
    ResizeableSemaphore permits = new ResizeableSemaphore(2);

    private ScheduledExecutorService monitorTask = Executors.newSingleThreadScheduledExecutor();

    class Oligarchy implements Runnable{

        @Override
        public void run() {

            while(true){
            try {

                permits.acquire();
                System.out.println("Oligarchy has taken away all permits:" + permits.toString()
                        + " will not release and lemme ");

                while(true){
                    //save some CPU but never ever release control
                    Thread.sleep(2*60*1000);
                }
                //Thread.sleep(2*60*1000);


            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            }

        }
    }


    class LateEntrant implements Runnable{

        @Override
        public void run() {
            while(true){
            try {

                permits.acquire();
                System.out.println("LateEntrant finally got one " + System.currentTimeMillis()
                        + "  total permits " + permits.toString());

                Thread.sleep(2*1000);
                 permits.release();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            }
        }
    }

    //the monitor
    class Government implements Runnable{

        int blockedCount ;
        long lastIncreased=System.currentTimeMillis();
        @Override
        public void run() {

            if (blockedCount > 5){

                System.out.println("releasing more permits " +  System.currentTimeMillis());
                int increaseCount = (int) Math.ceil(permits.initPermits * 0.2);
                System.out.println("Increase permit by " + increaseCount);
                //can optionally increase the permits based on time diff
                permits.increasePermits(increaseCount);
                blockedCount++;
                blockedCount=0;
            }

            if (permits.hasQueuedThreads()){
                System.out.println("Some players in market are blocked waiting for permits " );
                blockedCount++;

            }
        }
    }

    public void kickStartEconomyGame(){
        monitorTask.scheduleAtFixedRate(new Government(), 1l,2L, TimeUnit.SECONDS);
        //2 evil corps take over all the permits
        new Thread(new Oligarchy()).start();
        new Thread(new Oligarchy()).start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        new Thread(new LateEntrant()).start();



    }


    public static void main(String [] args){


        SimplePermitTest permitTest = new SimplePermitTest();
        permitTest.kickStartEconomyGame();


    }

}
