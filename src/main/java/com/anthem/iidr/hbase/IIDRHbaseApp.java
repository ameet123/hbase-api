package com.anthem.iidr.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * Created by ameet.chaubal on 10/23/2017.
 */
public class IIDRHbaseApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(IIDRHbaseApp.class);
    private Configuration conf;
    private Connection connection;
    private Table table;
    private String TABLE_NAME;
    private Random rand;
    private byte[] COLUMN_FAMILY_NAME;
    private byte[] COLUMN_NAME;

    public IIDRHbaseApp(String tableName, String columnFamily, String column) {
        conf = HBaseConfiguration.create();

        TABLE_NAME = tableName;
        COLUMN_FAMILY_NAME = columnFamily.getBytes();
        COLUMN_NAME = column.getBytes();
        rand = new Random();
    }

    public static void main(String[] args) {
        IIDRHbaseApp iidrHbaseApp = new IIDRHbaseApp("ameet-tab", "cf1", "col1");
        iidrHbaseApp.insertData();
    }

    public void insertData() {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            String rowKey = "myKey-" + rand.nextInt(100000);
            String val = "hello world-" + rand.nextInt(934552);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, Bytes.toBytes(val));
            table = connection.getTable(TableName.valueOf(TABLE_NAME));
            table.put(put);
        } catch (IOException e) {
            LOGGER.error("Error connecting to hbase, no insert done.");
        }
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }
}
