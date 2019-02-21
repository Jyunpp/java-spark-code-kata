package jyunpp.javaspark.codekata.ch3_transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class Prac3_Count {

	@Test
	public void count_line_of_String() {
		SparkConf sparkConf= new SparkConf().setMaster("local").setAppName("hello count");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		long count;
		/**
		 * count String line of 'resouces/prac3_Count' file using javaRDD.count();
		 */
		// write your code here..
		JavaRDD<String> javaRDD = javaSparkContext.textFile(getClass().getClassLoader().getResource("prac3_csount").getFile());
		count = javaRDD.count();

		// check
		assertThat(count, is(10L));
		System.out.println("count : " + count);
	}

	@Test
	public void count_line_of_String_that_contains_error() {
		SparkConf sparkConf= new SparkConf().setMaster("local").setAppName("hello count and filter");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		long count;

		/**
		 * count String line that containing "error" at 'resouces/prac2_filter' file using javaRDD.count();
		 */
		// write your code here..
		JavaRDD<String> javaRDD = javaSparkContext.textFile(getClass().getClassLoader().getResource("prac2_filter").getFile());
		count = javaRDD.filter(s -> s.contains("error")).count();

		// check
		assertThat(count, is(5L));
		System.out.println("count : " + count);
	}
}
