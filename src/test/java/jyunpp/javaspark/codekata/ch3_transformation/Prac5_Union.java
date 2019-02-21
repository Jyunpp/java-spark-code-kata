package jyunpp.javaspark.codekata.ch3_transformation;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class Prac5_Union {

	@Test
	public void union_two_data_set() {
		SparkConf sparkConf= new SparkConf().setMaster("local").setAppName("hello union");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<Integer> javaRDD1 = javaSparkContext.parallelize(Arrays.asList(1, 2, 3));
		JavaRDD<Integer> javaRDD2 = javaSparkContext.parallelize(Arrays.asList(3, 4, 5));
		JavaRDD<Integer> unionRDD;

		/**
		 * union two data set using javaRDD1.union(javaRDD2);
		 */
		// write your code here..
		unionRDD = javaRDD1.union(javaRDD2);

		// check
		assertThat(StringUtils.join(unionRDD.collect(), ","), is("1,2,3,3,4,5"));
		System.out.println("unionRDD : " + StringUtils.join(unionRDD.collect(), ","));
	}
}
