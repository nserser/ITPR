package RDR.RDR;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

public class Compute {

	
	static JavaSparkContext sc;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkConf sparkConf = new SparkConf().setAppName("RDR_Computer");

		sparkConf.set("spark.executor.instances", "60");
		//sparkConf.set("spark.executor.memory", "512M");

		String srcFile = args[0];
		
		sc = new JavaSparkContext(sparkConf);

		
		long startTime = System.currentTimeMillis();
				
		JavaRDD<String> lines = sc.textFile(srcFile);
		
		JavaPairRDD<Tuple2<String, String>, Long> paired = 
				lines.mapToPair(new PairFunction<String, Tuple2<String, String>, Long>() {

					private static final long serialVersionUID = -6757349307852373001L;

					public Tuple2<Tuple2<String, String>, Long> call(String t) throws Exception {
						// TODO Auto-generated method stub
						String[] elements = t.split(";");
						return new Tuple2<Tuple2<String,String>, Long>(new Tuple2<String, String>(elements[0], elements[1]), 1l);
					}
		});
		
		JavaPairRDD<Tuple2<String, String>, Long> nbOccurances = paired.reduceByKey(new Function2<Long, Long, Long>() {
			
			private static final long serialVersionUID = -2465397583997628957L;

			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
		});
		
		JavaPairRDD<String, Tuple2<String, Long>> nbOccurances_ = nbOccurances.mapToPair(new PairFunction<Tuple2<Tuple2<String,String>,Long>, String, Tuple2<String, Long>>() {

			private static final long serialVersionUID = -5196848758305999645L;

			public Tuple2<String, Tuple2<String, Long>> call(Tuple2<Tuple2<String, String>, Long> t)
					throws Exception {
				
				return new Tuple2<String, Tuple2<String,Long>>(t._1._1, 
						new Tuple2<String, Long>(t._1._2, t._2));
			}
		});
		
		
		JavaPairRDD<String, Iterable<Tuple2<String, Long>>> nbOccurances_grouped = nbOccurances_.groupByKey();
		
		JavaRDD<Tuple3<String, Iterable<Tuple2<String,Long>>, Long>> res = nbOccurances_grouped.map(new Function<Tuple2<String,Iterable<Tuple2<String,Long>>>, Tuple3<String, Iterable<Tuple2<String, Long>>, Long>>() {

			private static final long serialVersionUID = -6102757055239972714L;

			public Tuple3<String, Iterable<Tuple2<String, Long>>, Long> call(
					Tuple2<String, Iterable<Tuple2<String, Long>>> v1) throws Exception {
				Long count = 0l;
				for(Tuple2<String, Long> elem: v1._2) {
					count += elem._2();
				}
				return new Tuple3<String, Iterable<Tuple2<String,Long>>, Long>(v1._1, v1._2, count);
			}
		});
		
		
		List<Tuple3<String, Iterable<Tuple2<String,Long>>, Long>> rec = res.take(100);
		
		
		for(Tuple3<String, Iterable<Tuple2<String,Long>>, Long> elem :rec) {
			System.out.println(elem._1() + ":" + elem._3());
		}
		
		
		long endTime = System.currentTimeMillis();
		System.out.println("Execution Time:" + (endTime - startTime));
	}

}
