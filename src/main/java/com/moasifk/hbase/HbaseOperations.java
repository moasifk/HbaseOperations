package com.moasifk.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseOperations {
	
	public void createHbaseTable(String tableName, String columnFamily) {
		Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "nn01.itversity.com,nn02.itversity.com,rm01.itversity.com");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        Connection connection = null;
        Admin admin = null;
        try {
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();

            if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
                HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(tableName));
                hbaseTable.addFamily(new HColumnDescriptor(columnFamily));
                admin.createTable(hbaseTable);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }

                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
	
	public void insertDataIntoHbaseTable(String tableName) {
		Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "nn01.itversity.com,nn02.itversity.com,rm01.itversity.com");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        Connection connection = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableName));
            
            Put employee1 = new Put(Bytes.toBytes("3"));
            employee1.addColumn(Bytes.toBytes("address"), Bytes.toBytes("name"), Bytes.toBytes("Mohammed Asif"));
            employee1.addColumn(Bytes.toBytes("address"), Bytes.toBytes("location"), Bytes.toBytes("India"));
            table.put(employee1);
            
            Put employee2 = new Put(Bytes.toBytes("4"));
            employee2.addColumn(Bytes.toBytes("address"), Bytes.toBytes("name"), Bytes.toBytes("John"));
            employee2.addColumn(Bytes.toBytes("address"), Bytes.toBytes("location"), Bytes.toBytes("US"));
            table.put(employee2);
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (table != null) {
                	table.close();
                }

                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
	}
}
