package com.lysoft.chapter05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 功能说明：自定义SourceFunction
 * author:liangyy
 * createtime：2022-12-28 09:45:48
 */
public class ClickSource implements SourceFunction<Event> {

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        while (isRunning) {
            ctx.collect(new Event(users[random.nextInt(users.length)], urls[random.nextInt(urls.length)], System.currentTimeMillis()));
            // 隔1秒生成一个点击事件，方便观测
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }

}
