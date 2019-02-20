package java2hbasemvn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class _5DisableHbaseTable {

	public static void main(String[] args) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		boolean value = admin.isTableDisabled(TableName.valueOf("employee"));
		if(!value)
		{
			System.out.println("Table is enabled");
		}
		else
		{
			System.out.println("Table is disabled");
		}

	}

}
