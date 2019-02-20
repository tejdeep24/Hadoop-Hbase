package java2hbasemvn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class _1GetSingleEmployee {

	public static void main(String[] args) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("empdept"));
		TableName table_name = table.getName();
		System.out.println(table.getName());
		System.out.println(table_name.getNameAsString());
		
		Scan sc = new Scan();
		Filter filter = new FirstKeyOnlyFilter();
		sc.setFilter(filter);
		HTableDescriptor hd = table.getTableDescriptor();
		HColumnDescriptor[] hcd =hd.getColumnFamilies();
		ResultScanner resultscanner = table.getScanner(sc);
		Iterator<Result> it = resultscanner.iterator();
		NavigableMap<byte[],byte[]> nm;
		ArrayList<String> al;
		ArrayList<String> li;
		Map<String,ArrayList<String>> map = new HashMap<String,ArrayList<String>>();
		while(it.hasNext())
		{
			Result result = it.next();
			al =new ArrayList<String>();
			//System.out.println(Bytes.toString(result.getRow())); --- Rowkey
			for(HColumnDescriptor h:hcd)
			{
				nm = result.getFamilyMap(Bytes.toBytes(h.getNameAsString()));
				if(nm.keySet().size()>0)
				{
					//System.out.println("Column Family:"+h.getNameAsString()); --- Column Family Name
					for(byte[] Quantifier: nm.keySet())
					{
						//System.out.println(Bytes.toString(Quantifier)); ---- Column Name
						byte[] val = result.getValue(Bytes.toBytes(h.getNameAsString()),Bytes.toBytes(Bytes.toString(Quantifier)));
						al.add(Bytes.toString(val));
						//System.out.println(Bytes.toString(result.getRow())+","+Bytes.toString(val));
					}
				}
			}
			map.put(Bytes.toString(result.getRow()),al);
			//System.out.println("********************");
		}
		Set<String> st = map.keySet();
		for(String s: st)
		{
			li = map.get(s);
			System.out.print(s+",");
			for(String l:li)
			{
				System.out.print(l+",");
			}
			System.out.println();
		}
		table.close();

	}

}
