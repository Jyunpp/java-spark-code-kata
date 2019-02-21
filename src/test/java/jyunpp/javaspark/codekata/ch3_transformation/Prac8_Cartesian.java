package jyunpp.javaspark.codekata.ch3_transformation;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class Prac8_Cartesian {

	@Test
	public void cartesian_product() {
		SparkConf sparkConf= new SparkConf().setMaster("local").setAppName("hello cartesian");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<Integer> javaRDD1 = javaSparkContext.parallelize(Arrays.asList(1, 2, 3));
		JavaRDD<Integer> javaRDD2 = javaSparkContext.parallelize(Arrays.asList(3, 4, 5));
		JavaPairRDD<Integer, Integer> cartesianRDD;

		/**
		 * do cartesian product javaRDD1 x javaRDD2 using javaRDD1.cartesian(javaRDD2);
		 */
		// write your code here..
		cartesianRDD = javaRDD1.cartesian(javaRDD2);

		// check
		assertThat(StringUtils.join(cartesianRDD.collect(), ","), is("(1,3),(1,4),(1,5),(2,3),(2,4),(2,5),(3,3),(3,4),(3,5)"));
		System.out.println("cartesianRDD : " + StringUtils.join(cartesianRDD.collect(), ","));
	}
}
