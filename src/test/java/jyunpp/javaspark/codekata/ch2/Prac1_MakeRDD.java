package jyunpp.javaspark.codekata.ch2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class Prac1_MakeRDD {

	@Test
	public void make_JavaRDD_using_parallelize() {
		SparkConf sparkConf= new SparkConf().setMaster("local").setAppName("hello JavaSparkContext");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		JavaRDD<String> javaRDD;
		List<String> useThis = Arrays.asList("hello ", "spark");

		/**
		 * construct JavaRDD<String>, save at javaRDD using javaSparkContext.parallelize(useThis);
		 */
		// write your code here..
		javaRDD = javaSparkContext.parallelize(useThis);

		// check
		List<String> strings = javaRDD.collect();
		System.out.println(strings.get(0).concat(strings.get(1)));
		assertThat(strings.get(0).concat(strings.get(1)), is("hello spark"));
	}

	@Test
	public void hello_java_rdd() {
		SparkConf sparkConf= new SparkConf().setMaster("local").setAppName("hello JavaRDD");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		JavaRDD<String> javaRDD;
		String useThisFile = getClass().getClassLoader().getResource("prac1_hello").getFile();

		/**
		 * construct JavaRDD<String>, save at javaRDD using javaSparkContext.textFile(useThisFile);
		 */
		// write your code here..
		javaRDD = javaSparkContext.textFile(useThisFile);

		// check
		String hello = javaRDD.collect().get(0);
		System.out.println(hello);
		assertThat(hello, is("Hello Spark !!"));
	}
}
