package com.anthem.iidr.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
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
    private String PRINCIPAL = "AF55267@DEVAD.WELLPOINT.COM";

    public IIDRHbaseApp(String tableName, String columnFamily, String column) {
        conf = HBaseConfiguration.create();
        conf.addResource("/etc/alternatives/hbase-conf/hbase-site.xml");
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");

        System.setProperty("sun.security.krb5.debug", "false");

        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab(PRINCIPAL, "/home/af55267/apps/hbase-api/af55267.keytab");
        } catch (IOException e) {
            e.printStackTrace();
        }
        TABLE_NAME = tableName;
        COLUMN_FAMILY_NAME = columnFamily.getBytes();
        COLUMN_NAME = column.getBytes();
        rand = new Random();
        System.out.println("Table:" + tableName);
        LOGGER.info("table:{} cf:{} col:{}", TABLE_NAME, COLUMN_FAMILY_NAME, COLUMN_NAME);
    }

    public static void main(String[] args) {
        System.out.println("hbase insert anthem---");

        IIDRHbaseApp iidrHbaseApp = new IIDRHbaseApp("dv_bdfrawz:ameet-tab", "cf1", "col1");
        System.out.println("Instantiation done....");
        iidrHbaseApp.insertData();
    }

    public void insertData() {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            System.out.println("Connection ready...");
            String rowKey = "myKey-" + rand.nextInt(100000);
            String val = "hello world-" + rand.nextInt(934552);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, Bytes.toBytes(val));
            table = connection.getTable(TableName.valueOf(TABLE_NAME));
            System.out.println("Table info from object:" + table.getName());
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
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
