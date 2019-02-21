package jyunpp.javaspark.codekata.ch3_transformation;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class Prac7_Subtract {

	@Test
	public void square_numbers() {
		SparkConf sparkConf= new SparkConf().setMaster("local").setAppName("hello subtract");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<Integer> javaRDD1 = javaSparkContext.parallelize(Arrays.asList(1, 2, 3));
		JavaRDD<Integer> javaRDD2 = javaSparkContext.parallelize(Arrays.asList(3, 4, 5));
		JavaRDD<Integer> subtractedRDD;

		/**
		 * subtract common data at javaRDD1 using javaRDD1.subtract(javaRDD2);
		 */
		// write your code here..
		subtractedRDD = javaRDD1.subtract(javaRDD2);

		// check
		assertThat(StringUtils.join(subtractedRDD.collect(), ","), is("1,2"));
		System.out.println("subtractedRDD : " + StringUtils.join(subtractedRDD.collect(), ","));
	}
}
