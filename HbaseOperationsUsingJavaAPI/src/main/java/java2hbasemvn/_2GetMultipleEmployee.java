package java2hbasemvn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class _2GetMultipleEmployee {

	public static void main(String[] args) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("empdept"));
		
		Get Obj1 = new Get(Bytes.toBytes(111));
		Get Obj2 = new Get(Bytes.toBytes(222));
		
		List<Get> li = new ArrayList<Get>();
		li.add(Obj1);
		li.add(Obj2);
		
		Result result[] = table.get(li);
		
		for(Result r:result)
		{
			byte[] id = r.getValue(Bytes.toBytes("basic"),Bytes.toBytes("empid"));
			System.out.println(Bytes.toString(id));
		}
		//System.out.println(Bytes.toString(r1));
		//System.out.println(Bytes.toString(r2));
		

	}

}
