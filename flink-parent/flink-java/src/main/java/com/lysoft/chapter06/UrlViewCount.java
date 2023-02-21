package com.lysoft.chapter06;

import java.sql.Timestamp;

public class UrlViewCount {
    // 访问的Url地址
    private String url;

    // Url访问次数
    private Long viewCount;

    // 窗口的开始时间
    private Long windowStart;

    // 窗口的结束时间
    private Long windowEnd;

    public UrlViewCount(String url, Long viewCount, Long windowStart, Long windowEnd) {
        this.url = url;
        this.viewCount = viewCount;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getViewCount() {
        return viewCount;
    }

    public void setViewCount(Long viewCount) {
        this.viewCount = viewCount;
    }

    public Long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(Long windowStart) {
        this.windowStart = windowStart;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", viewCount=" + viewCount +
                ", windowStart=" + new Timestamp(windowStart) +
                ", windowEnd=" + new Timestamp(windowEnd) +
                '}';
    }
}
