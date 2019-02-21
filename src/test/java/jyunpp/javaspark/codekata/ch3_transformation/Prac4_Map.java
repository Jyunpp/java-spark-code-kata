package jyunpp.javaspark.codekata.ch3_transformation;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class Prac4_Map {

	@Test
	public void square_numbers() {
		SparkConf sparkConf= new SparkConf().setMaster("local").setAppName("hello map");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));
		JavaRDD<Integer> squareRDD;

		/**
		 * square numbers in javaRDD using map(Function<T, R>);
		 */
		// write your code here..
		squareRDD = javaRDD.map(x -> x * x);

		// check
		assertThat(StringUtils.join(squareRDD.collect(), ","), is("1,4,9,16,25"));
		System.out.println("squareRDD : " + StringUtils.join(squareRDD.collect(), ","));
	}
}
