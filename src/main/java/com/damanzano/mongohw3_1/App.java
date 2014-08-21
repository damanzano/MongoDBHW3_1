package com.damanzano.mongohw3_1;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Mongo Course, Homework 3.1
 *
 * <p>
 * Download the students.json file from
 * https://university.mongodb.com/static/10gen_2014_M101J_August/handouts/students.e7ed0a289cbe.json
 * and import it into your local Mongo instance with this command:</p>
 * <code>$ mongoimport -d school -c students < students.json</code>
 *
 * <p>
 * This dataset holds the same type of data as last week's grade collection, but
 * it's modeled differently. You might want to start by inspecting it in the
 * Mongo shell.</p>
 * <p>
 * Write a program in the language of your choice that will remove the lowest
 * homework score for each student. Since there is a single document for each
 * student containing an array of scores, you will need to update the scores
 * array and remove the homework.</p>
 * <p>
 * Remember, just remove a homework score. Don't remove a quiz or an exam!</p>
 * <p>
 * Hint/spoiler: With the new schema, this problem is a lot harder and that is
 * sort of the point. One way is to find the lowest homework in code and then
 * update the scores array with the low homework pruned.</p>
 */
public class App {

    public static void main(String[] args) throws UnknownHostException {
        MongoClient client = new MongoClient();
        DB db = client.getDB("school");
        DBCollection collection = db.getCollection("students");

        DBCursor students = collection.find();
        
        try {
            while (students.hasNext()) {
                DBObject student = students.next();
                System.out.println(student);
                
                // Find the lowest homework grade for each student and delete it.
                BasicDBList scores = (BasicDBList) student.get("scores");
                
                int lowestPosition=0;
                double lowestGrade=0;
                boolean firstHomework = false;
                for(int i=0; i<scores.size();i++){
                    BasicDBObject score = (BasicDBObject)scores.get(i);
                    if(score.getString("type").equalsIgnoreCase("homework")){
                        if(!firstHomework){
                            lowestGrade = score.getDouble("score");
                            lowestPosition = i;
                            firstHomework=true;
                        }else{
                            if(score.getDouble("score")<lowestGrade){
                                lowestGrade = score.getDouble("score");
                                lowestPosition = i;
                            }
                        }
                    }
                }
                
                scores.remove(lowestPosition);
                System.out.println(collection.update(new BasicDBObject("_id", ((Integer)student.get("_id"))), new BasicDBObject("$set", new BasicDBObject("scores",scores))));
                
            }
        } finally {
            students.close();
        }
    }
}
