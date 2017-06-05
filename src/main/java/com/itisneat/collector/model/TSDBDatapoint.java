package com.itisneat.collector.model;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSONArray;

public class TSDBDatapoint {
	String metric;
	String value;
	long timestamp;
	Map<String, String> tags;
	
	public TSDBDatapoint() {
		
	}
	
	public TSDBDatapoint(String metric, String value, long time, Map<String, String> tags) {
		this.setMetric(metric);
		if (tags != null) {
			this.setTags(tags);
		} else {
			this.setTags(new HashMap<String, String>());
		}
		this.setTimestamp(time);
		this.setValue(value);
		this.setTags(tags);
	}
	
	public String toJsonString() {
		return JSONArray.toJSONString(this);
	}
	
//	public static void main(String[] args) {
//		TSDBDatapoint dp = new TSDBDatapoint();
//		dp.setMetric("m");
//		dp.setTimestamp(123456l);
//		dp.setValue("999");
//		Map<String, String> tags = new HashMap<String, String>();
//		tags.put("t1", "t1v");
//		dp.setTags(tags);
//		
//		TSDBDatapoint dp2 = new TSDBDatapoint();
//		dp2.setMetric("m");
//		dp2.setTimestamp(123456l);
//		dp2.setValue("999");
//		dp2.setTags(tags);
//		
//		TSDBDatapoint[] dps = {dp, dp2};
//		
//		System.out.println(JSONArray.toJSON(dps));
//	}

	public String getMetric() {
		return metric;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}
	
	

}
