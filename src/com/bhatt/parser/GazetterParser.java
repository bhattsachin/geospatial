package com.bhatt.parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/**
 * Parses data from http://www.geonames.org/
 * US.txt
 * @author bhatt
 *
 */
public class GazetterParser {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		readData("/home/bhatt/Downloads/US.txt");

	}
	
	/**
	 * Add data to mongodb from text file
	 * @param filename
	 */
	private static void readData(String filename) {
		MongoClient mongoClient = null;
		try {
			File file = new File(filename);
			BufferedReader in = new BufferedReader(new FileReader(file));
			String line;
			String[] word;
			GeoLocation location;
			GeoData data;
			mongoClient = new MongoClient("localhost", 54321);
			DB geodb = mongoClient.getDB("usgeo");
			DBCollection collection = geodb.getCollection("postaloc");

			BasicDBObject document = null;
			while ((line = in.readLine()) != null) {
				document = new BasicDBObject();

				try {
					System.out.println(line);
					word = line.split("\t");
					data = new GeoData();
					data.id = word[0];
					location = new GeoLocation();
					location.geoid = word[0];
					location.name = word[1];
					location.asciiname = word[2];
					location.alternatenames = word[3];
					location.latitude = Double.parseDouble(word[4]);
					location.longitude = Double.parseDouble(word[5]);
					location.featureclass = word[6];
					location.feaaturecode = word[7];
					location.countrycode = word[8];
					location.cc2 = word[9];
					location.admin1code = word[10];
					location.admin2code = word[11];
					location.admin3code = word[12];
					location.admin4code = word[13];
					location.population = BigInteger.valueOf(Long
							.parseLong(word[14]));
					location.elevation = Integer.parseInt(word[15]);
					location.modificationdate = word[18];
					data.value = location;

					document = populateDocument(document, location);
					collection.insert(document);
					System.out.println(data);
				} catch (Exception ex) {
					System.out.println("error:");
					ex.printStackTrace();
				}

			}
		} catch (Exception ex) {
			ex.printStackTrace();

		} finally {
			mongoClient.close();
		}
	}

	/**
	 * 
	 * @param document
	 * @param location
	 * @return
	 */
	public static BasicDBObject populateDocument(BasicDBObject document,
			GeoLocation location) {
		List<Double> locData = new ArrayList<Double>();
		document.put("geoid", location.geoid);
		document.put("name", location.name);
		document.put("asciiname", location.asciiname);
		document.put("latitude", location.latitude);
		document.put("longitude", location.longitude);
		document.put("featureclass", location.featureclass);
		document.put("featurecode", location.feaaturecode);
		document.put("admin1code", location.admin1code);
		document.put("admin2code", location.admin2code);
		document.put("admin3code", location.admin3code);
		document.put("admin4code", location.admin4code);
		// document.put("population", location.population);
		locData.add(location.latitude);
		locData.add(location.longitude);
		document.put("elevation", location.elevation);
		document.put("loc", locData);

		return document;
	}

}
