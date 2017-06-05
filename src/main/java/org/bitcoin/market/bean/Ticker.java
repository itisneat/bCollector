package org.bitcoin.market.bean;

/**
 * Created by leo on 2017/6/3.
 */
public class Ticker {
    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    private long time;

    public Double getLast() {
        return last;
    }

    public void setLast(Double last) {
        this.last = last;
    }

    private Double last;
}
