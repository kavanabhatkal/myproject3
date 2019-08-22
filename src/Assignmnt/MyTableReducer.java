package Assignmnt;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class MyTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

	@Override
	public void reduce( Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
		int i = 0;
		for ( IntWritable val : values ) {
			i += val.get();
		}
		System.out.println( "For key ===> " + key.toString() + " Value ---> " + i );
		Put p = new Put( Bytes.toBytes( key.toString() ) );
		p.addColumn( "Total".getBytes(), "t".getBytes(), Bytes.toBytes( "" + i ) );
		context.write( null, p );
	}
}