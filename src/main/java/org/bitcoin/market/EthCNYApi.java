package org.bitcoin.market;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.bitcoin.common.HttpUtils;
import org.bitcoin.market.bean.Ticker;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.itisneat.collector.model.TSDBDatapoint;
import com.itisneat.collector.model.Trade;

import redis.clients.jedis.Jedis;

/**
 * Created by leo on 2017/6/3.
 */
public class EthCNYApi {
    private static final String YUNBI_URL = "https://yunbi.com";
    
    private static int latest = 0;
    private static int start = 0;
    private static Jedis jedis;
    
    private static long historyTimestamp = 0l;
    
    private static final String ethHistoryKey = "eth.trade.history";
    private static final String ethCurrentKey = "eth.trade.current";
    
    private static final String btcCurrentKey = "btc.trade.current";
    
    

    public static void main(String[] args)
    {
    	jedis = new Jedis("120.55.171.131", 63791);
    	
//    	historyTimestamp = Long.valueOf(jedis.get(ethHistoryKey));
    	
//    	while(true) {
//    		try {
//    			int count = handleHistoryTrades(historyTimestamp);
//    			if (count == 0) {
//    				//break;
//    			}
//    			Thread.sleep(1000L);
//				
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//    	}
    	
    	while(true) {
    		try {
    			int count = handleCurrentTrades();
    			if(count == 0) {
    				Thread.sleep(5000);
    			} else {
    				Thread.sleep(2000);
    			}
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    	}
    	

    }
    
    public Ticker getCurrentTicker()
    {
        //https://yunbi.com//api/v2/tickers/ethcny.json
        String ticker_url = YUNBI_URL + "/api/v2/tickers/ethcny";
        String text = HttpUtils.getContentForGet(ticker_url, 5000);
        System.out.println(text);
        JSONObject jsonObject = JSONArray.parseObject(text);
        JSONObject ticker = jsonObject.getJSONObject("ticker");
        Ticker tickerObj = new Ticker();
        tickerObj.setTime(jsonObject.getLong("at"));
        tickerObj.setLast(ticker.getDouble("last"));
        return tickerObj;
    }
    
    public static List<Trade> getTrades(String paramString){
    	List<Trade> trades = new LinkedList<Trade>();
    	//String trades_url = ETH_URL + "/api/v2/trades.json?market=ethcny" + paramString + "&order_by=asc";
    	String trades_url = YUNBI_URL + "/api/v2/trades.json?market=btccny" + paramString + "&order_by=asc";
    	
    	String text = HttpUtils.getContentForGet(trades_url, 5000);
        JSONArray jsonArray = JSONArray.parseArray(text);
        long time = 0l;
        int ms = 0;
                
        for(Object obj : jsonArray) {       	
        	JSONObject jObj = (JSONObject) obj;
        	Trade trade = new Trade(jObj);
        	
        	if (time == trade.getAt()) {
    			ms += 10;
    			trade.setAt(time * 1000 + ms);
    		} else {
    			ms = 0;
    		}
        	time = trade.getAt();
        	trades.add(trade);
        }
    	return trades;
    }
    
    public static int handleCurrentTrades() {
    	
    	List<TSDBDatapoint> dps = new LinkedList<TSDBDatapoint>();
    	String paramterString = "";
    	if (latest > 0) {
    		paramterString = "&from=" + latest;
    	} 
    	List<Trade> trades = getTrades(paramterString);
    	
    	for(Trade trade : trades) {
    		dps.addAll(trade.toTSDPs());
    		if (start == 0) {
    			//only one time
    			start = trade.getId();
    		}
    	}
    	
    	if (dps.size() > 0) {
    		String openTSdbMsg = JSON.toJSONString(dps.toArray());
        	HttpUtils.appadd(openTSdbMsg);
        	
        	latest = ((LinkedList<Trade>) trades).getLast().getId();
        	Map<String, String> map = new HashMap<String, String>();
        	map.put(String.valueOf(start), String.valueOf(latest));
        	jedis.hmset(btcCurrentKey, map);
    	}
    	
    	System.out.println("handled [" + trades.size() + "] trades, start: [" + start + "], latest: [" + latest + "]");
    	return trades.size();
    }
    
    public static int handleHistoryTrades(long timestamp) {
    	
    	List<TSDBDatapoint> dps = new LinkedList<TSDBDatapoint>();
    	String paraString = "&timestamp=" + String.valueOf(timestamp);
    	List<Trade> trades = getTrades(paraString);
    	long firstTime = 0l;
    	for(Trade trade : trades) {
    		if (firstTime == 0l) {
    			//prevent have multiple recode at fist time
    			firstTime = trade.getAt() + 1;
    		}
    		dps.addAll(trade.toTSDPs());
    	}
    	
    	if (dps.size() > 0) {
    		String openTSdbMsg = JSON.toJSONString(dps.toArray());
        	HttpUtils.appadd(openTSdbMsg);
        	
        	Map<String, String> map = new HashMap<String, String>();
        	map.put(String.valueOf(start), String.valueOf(latest));
        	jedis.set(ethHistoryKey, String.valueOf(firstTime));
        	historyTimestamp = firstTime;
    	}
    	
    	System.out.println("handled [" + trades.size() + "] trades, time from: [" + timestamp + "] change to [" + firstTime + "]");
    	return trades.size();
    }

}
