package RDR.RDR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Database {
	
	private int nbQids;
	List<String[]> database;
	
	
	public int getNb_qids() {
		return nbQids;
	}
	public void setNb_qids(int nb_qids) {
		this.nbQids = nb_qids;
	}
	public List<String[]> getDatabase() {
		return database;
	}
	public void setDatabase(List<String[]> database) {
		this.database = database;
	}
	
	public void addRecord(String[] record) {
		database.add(record);
	}
	
	
	public void generateRandom(int nbRecord, int nbQids, List<String> qidValues, List<String> sValues){
		this.nbQids = nbQids;
		database = new ArrayList<String[]>();
		int i = 0;
		int indexQid;
		int indexS;
		
		Random rn = new Random();
		
		while(i < nbRecord) {
			String[] record = new String[nbQids+1];
			
			for(int j = 0; j < nbQids; j++) {
				indexQid = rn.nextInt() % qidValues.size();
	    		if(indexQid < 0)
	    			indexQid = indexQid * -1;
	    		record[j] = qidValues.get(indexQid) + "_" + Integer.toString(j);
			}
			
    		indexS = rn.nextInt() % sValues.size();
    		if(indexS < 0)
    			indexS = indexS * -1;
    		record[nbQids] = sValues.get(indexS);
    		
    		database.add(record);
    		
    		//if(i % 10000 == 0)
    			//System.out.println(i);
			i++;
		}
	}
	
	public void compute_entropy(int nbConsideredQid) {
		Map<String, Double> sOccurances = new HashMap<String, Double>();
		
		Map<Record, HashMap<String, Double>> qidsOcurances = new HashMap<Record, HashMap<String, Double>>();
		
		Map<Record, Double> qidOcurances = new HashMap<Record, Double>();

		
		for(String[] record : database) {
			if(sOccurances.containsKey(record[this.nbQids])) {
				sOccurances.put(record[this.nbQids], sOccurances.get(record[this.nbQids])+1);
			}
			else
				sOccurances.put(record[this.nbQids], 1D);
			
			String[] qids = Arrays.copyOfRange(record, 0, nbConsideredQid);
			Record rec = new Record();
			rec.setContent(qids);
			
			if(qidsOcurances.containsKey(rec)) {
				if(qidsOcurances.get(rec).containsKey(record[this.nbQids])){
					//System.out.println("b" + qidsOcurances.get(rec).get(record[this.nbQids]));
					qidsOcurances.get(rec).put(record[this.nbQids],qidsOcurances.get(rec).get(record[this.nbQids])+1);
					//System.out.println("a" + qidsOcurances.get(rec).get(record[this.nbQids]));
				}
				qidOcurances.put(rec,qidOcurances.get(rec)+1);
			}
			else {
				qidsOcurances.put(rec, new HashMap<String, Double>());
				qidsOcurances.get(rec).put(record[this.nbQids], 1D);
				qidOcurances.put(rec,1D);
			}
		}
		
		//System.out.println(qidOcurances.size());
		//System.out.println(qidsOcurances.size());
		
		//computing H(X)
		double HX = 0;
		int nbRecords = database.size();
		for(Map.Entry<String, Double> entry : sOccurances.entrySet()) {
			HX = HX + (-(entry.getValue()/nbRecords) * Util.log2(entry.getValue()/nbRecords));
		}
		//System.out.println("Hx = " + HX);
		
		//ComputingHXy
		double HXy = 0;
		for(Map.Entry<Record, HashMap<String, Double>> entry : qidsOcurances.entrySet()) {
			HXy = qidOcurances.get(entry.getKey()) / nbRecords;
			double temp = 0;
			for(Map.Entry<String, Double> entrys : entry.getValue().entrySet()) {
				//if(entrys.getValue()==null)
					//continue;
				//System.out.println(entrys.getValue());
				temp = temp + ((entrys.getValue() / nbRecords) * Util.log2(entrys.getValue() / nbRecords));
			}
			HXy = HXy * temp;
			//System.out.println("HXy = " + HXy);
		}
	}
}
