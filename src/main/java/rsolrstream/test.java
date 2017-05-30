package rsolrstream;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class test {
    private static final Logger log = LoggerFactory.getLogger(test.class);

    private final BlockingQueue<String> q;
    private final Thread consumer;

    public test() {
        q = new SynchronousQueue<>();
        consumer = new Thread(new OutputStuff(60000));
        consumer.start(); 
    }
    
    public void end() {
        consumer.interrupt();
    }

    public void testMe(String s) throws InterruptedException {
        log.debug("ADDING: " + s);
        try {
            q.put(s);
        } catch (InterruptedException ie) {
            throw ie;
        }    
    }

    class OutputStuff implements Runnable {
        final int millisTimeout;

        OutputStuff(int millisTimeout) {
            this.millisTimeout = millisTimeout;
        }

        @Override
        public void run() {
            log.debug("STARTING");
            String s = null;
            while (true) {
                try {
                    s = q.poll(millisTimeout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    log.debug("ENDING WITH INTERRUPT");
                    break;
                }
                if (s == null) {
                    log.debug("ENDING WITH NULL");
                    break;
                }
                log.debug("got: " + s);
            }
        }
    }
}
