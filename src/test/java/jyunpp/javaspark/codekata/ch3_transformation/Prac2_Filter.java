package jyunpp.javaspark.codekata.ch3_transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class Prac2_Filter {

	@Test
	public void filter_String_that_not_contain_error() {
		SparkConf sparkConf= new SparkConf().setMaster("local").setAppName("hello filter");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> javaRDD = javaSparkContext.textFile(getClass().getClassLoader().getResource("prac2_filter").getFile());

		/**
		 * filter JavaRDD<String> that is not containing "error" using javaRDD.filter(Function<T, R>);
		 */
		// write your code here..
		javaRDD = javaRDD.filter(s -> !s.contains("error"));

		// check
		int len = (int) javaRDD.count();
		for (String line : javaRDD.take(len))
			assertThat(line.contains("error"), is(false));
		for (String line : javaRDD.take(len))
			System.out.println(line);
	}
}
