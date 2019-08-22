package Assignmnt;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class MyTableMapper extends TableMapper<Text, IntWritable> {

	private static final Logger logger = Logger.getLogger( MyTableMapper.class );

	private static String type = "DOB";

	@SuppressWarnings( "deprecation" )
	@Override
	public void map( ImmutableBytesWritable key, Result value, Context context ) throws IOException, InterruptedException {
		Text text = new Text();
		List<KeyValue> l = value.list();
		logger.info( "Size ------> " + l.size() + " key ===> " + key.toString() );
		for ( KeyValue kv : l ) {
			String columnFamily = Bytes.toString( kv.getFamily() );
			String qualifier = Bytes.toString( kv.getQualifier() );
			String qualifiedValue = Bytes.toString( kv.getValue() );
			logger.info( "columnFamily ------> " + columnFamily + " type---> " + qualifier );
			if ( columnFamily.equals( "personal" ) && type.equals( qualifier ) ) {
				if ( qualifier.equals( "DOB" ) ) {
					String[] dob = qualifiedValue.split( Pattern.quote( "-" ) );
					text.set( dob[2] );
					context.write( text, new IntWritable( 1 ) );
				}
				else {
					text.set( qualifiedValue );
					context.write( text, new IntWritable( 1 ) );
				}
			}
			else {
				logger.info( "Iam not interested in the details" );
			}
		}
	}
}