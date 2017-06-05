package com.itisneat.collector.model;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

public class Trade {
	int id;
	String price;
	String volume;
	String funds;
	String market;
	String side;
	Long at;
	boolean up;
	
	public Trade() {
		
	}
	
	public Trade(JSONObject jsonObj) {
		this.setId(jsonObj.getInteger("id"));
		this.setAt(jsonObj.getLong("at"));
		this.setFunds(jsonObj.getString("funds"));
		this.setMarket(jsonObj.getString("market"));
		this.setPrice(jsonObj.getString("price"));
		this.setVolume(jsonObj.getString("volume"));
		
		if ("down".equalsIgnoreCase(jsonObj.getString("side"))) {
			this.up = false;
		} else {
			this.up = true;
		}
	}
	
	public List<TSDBDatapoint> toTSDPs() {
		Map<String, String> tags = new HashMap<String, String>();
		tags.put("bourse", "yunbi");
		
		List<TSDBDatapoint> tsdbs = new LinkedList<TSDBDatapoint>();
		tsdbs.add(new TSDBDatapoint("eth.trade.price", this.getPrice(), this.getAt(), new HashMap<String, String>(tags)));
		tsdbs.add(new TSDBDatapoint("eth.trade.volume", this.getVolume(), this.getAt(), new HashMap<String, String>(tags)));
		tsdbs.add(new TSDBDatapoint("eth.trade.funds", this.getFunds(), this.getAt(), new HashMap<String, String>(tags)));
		
		return tsdbs;
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getPrice() {
		return price;
	}
	public void setPrice(String price) {
		this.price = price;
	}
	public String getVolume() {
		return volume;
	}
	public void setVolume(String volume) {
		this.volume = volume;
	}
	public String getFunds() {
		return funds;
	}
	public void setFunds(String funds) {
		this.funds = funds;
	}
	public String getMarket() {
		return market;
	}
	public void setMarket(String market) {
		this.market = market;
	}
	public Long getAt() {
		return at;
	}
	public void setAt(Long at) {
		this.at = at;
	}

	public String getSide() {
		return side;
	}

	public void setSide(String side) {
		this.side = side;
	}

	public boolean isUp() {
		return up;
	}

	public void setUp(boolean up) {
		this.up = up;
	}


}
