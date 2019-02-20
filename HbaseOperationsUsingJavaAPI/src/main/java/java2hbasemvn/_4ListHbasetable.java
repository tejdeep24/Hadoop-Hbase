package java2hbasemvn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class _4ListHbasetable {

	public static void main(String[] args) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();
		TableName tablename[] = admin.listTableNames();
		for(TableName t:tablename)
		{
			System.out.println(t);
		}

	}

}
