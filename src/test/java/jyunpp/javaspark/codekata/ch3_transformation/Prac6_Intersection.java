package jyunpp.javaspark.codekata.ch3_transformation;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class Prac6_Intersection {

	@Test
	public void intersect_two_data_set() {
		SparkConf sparkConf= new SparkConf().setMaster("local").setAppName("hello intersection");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<Integer> javaRDD1 = javaSparkContext.parallelize(Arrays.asList(1, 2, 3));
		JavaRDD<Integer> javaRDD2 = javaSparkContext.parallelize(Arrays.asList(3, 4, 5));
		JavaRDD<Integer> commonRDD;

		/**
		 * extract common data between two data set using javaRDD1.intersection(javaRDD2);
		 */
		// write your code here..
		commonRDD = javaRDD1.intersection(javaRDD2);

		// check
		assertThat(StringUtils.join(commonRDD.collect(), ","), is("3"));
		System.out.println("commonRDD : " + StringUtils.join(commonRDD.collect(), ","));
	}
}
