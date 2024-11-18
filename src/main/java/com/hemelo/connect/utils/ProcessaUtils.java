package com.hemelo.connect.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;

public class ProcessaUtils {

    private static final Logger logger = LoggerFactory.getLogger(ProcessaUtils.class);

    /**
     * Cria um timer que executa um runnable a cada duration
     * @param name
     * @param runnable
     * @param duration
     * @return
     */
    public static Timer createTimer(String name, Duration duration, Runnable runnable) {

        name = "Timer - " + name;

        logger.info("Criando timer \"" + name + "\" com duração de " + duration.toMillis() + "ms");

        Timer timer = new Timer(name);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                runnable.run();
            }
        }, 1000, duration.toMillis());
        return timer;
    }

    /**
     * Cria e inicia uma thread
     * @param runnable
     * @return
     */
    public static Thread createThread(String threadName, Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setName("Thread - " + threadName);
        thread.start();
        return thread;
    }

    /**
     * Cria e inicia uma thread que executa um runnable em loop
     * @param runnable
     * @return
     */
    public static Thread createLoopedThread(String threadName, Runnable runnable) {
        Thread thread = new Thread(() -> {
            while (true) {
                runnable.run();
            }
        });

        thread.setName("Thread - " + threadName);
        thread.start();
        return thread;
    }

    public static Thread createAndJoinThread(String threadName, Runnable runnable) throws InterruptedException {
        Thread thread = new Thread(runnable);
        thread.setName("Thread - " + threadName);
        thread.join();
        return thread;
    }
}
