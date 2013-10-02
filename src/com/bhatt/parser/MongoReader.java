package com.bhatt.parser;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

/**
 * Reads from given source
 * @author bhatt
 *
 */
public class MongoReader {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		//readFromDb();
		latlonquery();
		

	}
	
	/**
	 * Creates a geo spatial index 2D wgs84
	 * @throws Exception
	 */
	public static void readFromDb()throws Exception{
		MongoClient mongoClient = new MongoClient("localhost", 54321);
		DB geodb = mongoClient.getDB("usgeo");
		
		
		DBCollection collection = geodb.getCollection("postaloc");
		collection.ensureIndex(new BasicDBObject("loc", "2d"));
		
		BasicDBObject query = new BasicDBObject();
		query.put("name", "Lexington");
		DBCursor cursor = collection.find(query);
		while(cursor.hasNext()){
			System.out.println(cursor.next());
		}
		
	}
	
	/**
	 * Lat lon query
	 * @throws Exception
	 */
	public static void latlonquery()throws Exception{
		MongoClient mongoClient = new MongoClient("localhost", 54321);
		DB geodb = mongoClient.getDB("usgeo");
		
		
		DBCollection collection = geodb.getCollection("postaloc");
		//collection.ensureIndex(new BasicDBObject("loc", "2d"));50
		
		BasicDBObject query = new BasicDBObject();
		query.put("loc", new BasicDBObject("$near", new Double[]{(double)44.144, (double)-73.987}));
	
		DBCursor cursor = collection.find(query).limit(5);
		while(cursor.hasNext()){
			System.out.println(cursor.next());
		}
	}

}
