package Assignmnt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

//import hadoopProject.WordCount;

public class RunnableMap {

	@SuppressWarnings( "deprecation" )
	public static void main( String[] args ) {
		HBaseAdmin admin = null;
		try {
			Configuration conf = HBaseConfiguration.create();

			List<Scan> list = new ArrayList<>();
			//JobConf conf = new JobConf(RunnableMap.class); 
			Job job = new Job( conf, "RunnableMap" );
			job.setJarByClass( RunnableMap.class );

			Scan scan = new Scan();
			scan.setAttribute( "scan.attributes.table.name", Bytes.toBytes( "employee" ) );
			list.add( scan );

			TableMapReduceUtil.initTableMapperJob( list, MyTableMapper.class, Text.class, IntWritable.class, job );
			TableMapReduceUtil.initTableReducerJob( "output", MyTableReducer.class, job );

			System.out.println( job.waitForCompletion( true ) ? 0 : 1 );
		}
		catch ( IOException | ClassNotFoundException
				| InterruptedException e ) {
			e.printStackTrace();
		}
		finally {
			try {
				if ( admin != null )
					admin.close();
			}
			catch ( Exception em ) {
				em.printStackTrace();
			}
		}

	}
}