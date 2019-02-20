package java2hbasemvn;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class _3CreateHbasetable {

	public static void main(String[] args) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();
		Scanner sc = new Scanner(System.in);
		System.out.println("Choose TableName and Column Familes");
		String tablename = sc.next();
		String Columnfamily = sc.next();
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("emp1"));
		htd.addFamily(new HColumnDescriptor("Personal"));
		htd.addFamily(new HColumnDescriptor("professional"));
		admin.createTable(htd);
		System.out.println("Table Created");
	}
}
