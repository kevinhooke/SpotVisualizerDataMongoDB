package kh.callsign.spotcollector.data;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.enterprise.context.Dependent;

import kh.mongo.MongoConnection;
import kh.radio.spotparser.domain.Spot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

@Dependent
public class CallsignProcessorMongoDBDaoImpl implements CallsignProcessorDao {

	private static Logger LOGGER = LogManager
			.getLogger("kh.callsign.spotcollector.data");
	
	/**
	 * Stores a new Spot document after processing. Example JSON document:
	 * <pre>
	 * {
	 *     "_id" : ObjectId("55209154e4b009a43fe1a166"),
	 *     "spotter" : "spotter_callsign",
	 *     "signalreport" : "-21",
	 *     "spotReceivedTimestamp" : ISODate("2014-07-07T19:53:00Z"),
	 *     "word1" : "callsign",
	 *     "timeDeviation" : "-0.2",
	 *     "word3" : "DM33",
	 *     "word2" : "othercall",
	 *     "frequencyOffset" : "1006",
	 *     "time" : "1953",
	 *     "rxFrequency" : "14.076",
	 *     "spotDetail" : {
	 *         "errorMessage" : "success",
	 *         "longitude" : "-112.18149499999998",
	 *         "latitude" : "33.59184",
	 *         "dateLastProcessed" : ISODate("2015-04-05T21:49:16.063Z")
	 *      },
     * }
	 * </pre>
	 */
	@Override
	public void create(Spot spot) {
		LOGGER.info("store(spot) called!");
		DB db;
		try {
			db = MongoConnection.getMongoDB();
			DBCollection col = db.getCollection("Spot");
			DBObject newSpot = new BasicDBObject();
			newSpot.put("spotter", spot.getSpotter());
			newSpot.put("signalreport", spot.getSignalreport());
			newSpot.put("spotReceivedTimestamp", spot.getSpotReceivedTimestamp());
			newSpot.put("word1", spot.getWord1());
			newSpot.put("timeDeviation", spot.getTimeDeviation());
			newSpot.put("word3", spot.getWord3());
			newSpot.put("word2", spot.getWord2());
			newSpot.put("frequencyOffset", spot.getFrequencyOffset());
			newSpot.put("time", spot.getTime());
			newSpot.put("rxFrequency", spot.getRxFrequency());		
			
			DBObject processingDetail = new BasicDBObject();
			processingDetail.put("errorMessage", spot.getSpotDetail().getErrorMessage());
			processingDetail.put("longitude", spot.getSpotDetail().getLongitude());
			processingDetail.put("latitude", spot.getSpotDetail().getLatitude());
			processingDetail.put("dateLastProcessed", new Date());
			newSpot.put("spotDetail", processingDetail);
			
			col.insert(newSpot);
		} catch (UnknownHostException e) {
			LOGGER.fatal("Failed to store Spot data", e);;
		}

	}

	
	@Override
	public boolean existsSpotBySpotterAndTimestamp(Spot spot) {

		DB db;
		boolean exists = false;
		
		try {
			db = MongoConnection.getMongoDB();
			DBCollection col = db.getCollection("Spot");
			BasicDBObject querySpotter = new BasicDBObject("spotter", spot.getSpotter());
			BasicDBObject queryTime = new BasicDBObject("spotter", spot.getTime());
			List<BasicDBObject> conditions = new ArrayList<>();
			conditions.add(querySpotter);
			conditions.add(queryTime);
			BasicDBObject findSpotBySpotterAndTimestamp 
				= new BasicDBObject("$and", conditions);
			int results = col.find(findSpotBySpotterAndTimestamp).count();
			if(results > 0){
				exists = true;
			}
		}  catch (UnknownHostException e) {
			LOGGER.fatal("Failed to execute query for findSpotBySpotterAndTimestamp()", e);
		}		
		//TOOD: return Spot
		return exists;
	}
	@Override
	public Spot findSpotBySpotterAndTimestamp(Spot spot) {

		DB db;
		DBCursor c;
		
		try {
			db = MongoConnection.getMongoDB();
			DBCollection col = db.getCollection("Spot");
			BasicDBObject querySpotter = new BasicDBObject("spotter", spot.getSpotter());
			BasicDBObject queryTime = new BasicDBObject("spotter", spot.getTime());
			List<BasicDBObject> conditions = new ArrayList<>();
			conditions.add(querySpotter);
			conditions.add(queryTime);
			BasicDBObject findSpotBySpotterAndTimestamp 
				= new BasicDBObject("$and", conditions);
			c = col.find(findSpotBySpotterAndTimestamp).limit(1);
			DBObject result = c.next();
			//TODO create Spot instance
			
		}  catch (UnknownHostException e) {
			LOGGER.fatal("Failed to execute query for findSpotBySpotterAndTimestamp()", e);
		}		
		//TOOD: return Spot
		return null;
	}
}
