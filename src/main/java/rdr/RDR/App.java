package RDR.RDR;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.spark.deploy.worker.Sleeper;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException, InterruptedException
    {
        
    	List<String> qidValues = new ArrayList<String>();
    	List<String> sValues = new ArrayList<String>();
    	
    	long startTime,endTime;
    	
    	int nbQidValues = 10;
    	
    	int nbSValues = 100;
    	
    	while(qidValues.size() < nbQidValues) {
    		Random rn = new Random();
    		int r = rn.nextInt();
    		if(r < 0)
    			r = r * -1;
    		//System.out.println(r);
    		if(!qidValues.contains("q" + r))
    			qidValues.add("q" + r);
    	}
    	
    	while(sValues.size() < nbSValues) {
    		Random rn = new Random();
    		int r = rn.nextInt();
    		if(r < 0)
    			r = r * -1;
    		//System.out.println(r);
    		if(!sValues.contains("s" + r))
    			sValues.add("s" + r);
    	}
    	
    	//BufferedWriter writer = new BufferedWriter(new FileWriter("data.txt"));

    	/*
    	int i  = 0;
    	
    	Random rn = new Random();
    	int indexQid, indexS;
    	
    	while(i < 1000000000) {
    		indexQid = rn.nextInt() % nbQidValues;
    		if(indexQid < 0)
    			indexQid = indexQid * -1;
    		indexS = rn.nextInt() % nbSValues;
    		if(indexS < 0)
    			indexS = indexS * -1;
    		
    		writer.write(qidValues.get(indexQid) + ";" + sValues.get(indexS) + "\n");
    		if(i % 10000000 == 0 && i / 10000000 != 0)
    			System.out.println(i);
    		i++;
    	}
    	writer.close();
    	*/
    	
    	Database db = new Database();
    	db.generateRandom(100000, 129, qidValues, sValues);
    	
    	
    	long max;
    	long min;
    	long sum;
    	
    	for(int i = 2; i < 129; i = i*2) {
    		max = Long.MIN_VALUE;
        	min = Long.MAX_VALUE;
        	sum = 0;
    		for(int j = 0; j < 100; j++) {
	    		startTime = System.nanoTime();
	    		db.compute_entropy(i);
	    		endTime = System.nanoTime();
	    		if(max < endTime - startTime)
	    			max = endTime - startTime;
	    		if(min > endTime - startTime)
	    			min = endTime - startTime;
	    		
	    		sum += endTime - startTime;
	    		//System.out.println(i + " " + (endTime-startTime));
    		}
    		System.out.println(min + " " + (sum/100) + " " + max);
    	}
    	//db.compute_entropy(1);
    	
    	
    	
    }
}
