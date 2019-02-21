package jyunpp.javaspark.codekata.ch2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * these tests are not problem. just.. hello world!
 */
public class Prac0_WarmingUp {

	@Test
	public void hello_java_spark_context() {
		SparkConf sparkConf= new SparkConf().setMaster("local").setAppName("hello JavaSparkContext");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
	}

	@Test
	public void hello_java_rdd() {
		SparkConf sparkConf= new SparkConf().setMaster("local").setAppName("hello JavaRDD");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		JavaRDD<String> inputRDD = javaSparkContext.textFile(getClass().getClassLoader().getResource("prac1_hello").getFile());
		String hello = inputRDD.collect().get(0);

		System.out.println(hello);
		assertThat(hello, is("Hello Spark !!"));
	}
}
