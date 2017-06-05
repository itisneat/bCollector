package com.itisneat.collector;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class Collector {

	private static String configPath = "config.properties";
	
	public static void main(String[] args) throws IOException {
		String configPaht = System.getProperty("config");
		Properties prop = getProperties(configPaht);
		
		
	}
	
	private static Properties getProperties(String path) throws IOException {
		Properties prop = new Properties();
		InputStream input = new FileInputStream(path);
		prop.load(input);
		
		return prop;
	}
	
	

}
