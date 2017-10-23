package com.anthem.iidr.hbase;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import org.apache.commons.cli.*;
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
 * IIDR sample hbase insert
 */
public class IIDRHbaseApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(IIDRHbaseApp.class);
    private static final String HBASE_SITE = "/etc/alternatives/hbase-conf/hbase-site.xml";
    private static final String PROGRAM_NAME = "iidrAnthem";
    private static final String TABLE_NAME = "dv_bdfrawz:ameet-tab";
    private static final String COLUMN_FAMILY_NAME = "cf1";
    private static final String COLUMN_NAME = "col1";
    private static final String PRINCIPAL = "AF55267@DEVAD.WELLPOINT.COM";
    private static String KEYTAB;
    private Configuration conf;
    private Random rand;

    public IIDRHbaseApp(String tableName, String columnFamily, String column) {
        conf = HBaseConfiguration.create();
        conf.addResource(HBASE_SITE);
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");

        System.setProperty("sun.security.krb5.debug", "false");

        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab(PRINCIPAL, KEYTAB);
        } catch (IOException e) {
            LOGGER.error("Error logging to Hadoop using kerberos. exit");
            System.exit(4);
        }
        LOGGER.info("table:{} cf:{} col:{}", TABLE_NAME, COLUMN_FAMILY_NAME, COLUMN_NAME);
        rand = new Random();
    }

    public static void main(String[] args) {
        parseOptions(args);
        if (args == null || args.length == 0) {
            LOGGER.error("No arguments passed");
            System.exit(1);
        }

        IIDRHbaseApp iidrHbaseApp = new IIDRHbaseApp(TABLE_NAME, COLUMN_FAMILY_NAME, COLUMN_NAME);
        LOGGER.info(">>Instantiation done....");
        iidrHbaseApp.insertData();
    }

    private static void parseOptions(String[] args) {
        Options options = new Options();
        Option keytab = new Option("k", "keytab", true, "keytab absolute file path");
        keytab.setRequired(true);
        options.addOption(keytab);
        CommandLineParser parser = new BasicParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            LOGGER.error(e.getMessage());
            formatter.printHelp(PROGRAM_NAME, options);
            System.exit(1);
            return;
        }
        KEYTAB = cmd.getOptionValue("keytab");
        if (Strings.isNullOrEmpty(KEYTAB)) {
            LOGGER.error("Err: first argument needs to be keytab file absolute path");
            System.exit(2);
        }
        LOGGER.debug(">>Parsed keytab:{}", KEYTAB);
    }

    public void insertData() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            LOGGER.debug(">>Connection ready...");
            String rowKey = "myKey-" + rand.nextInt(100000);
            String val = "hello world-" + rand.nextInt(934552);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(COLUMN_FAMILY_NAME.getBytes(), COLUMN_NAME.getBytes(), Bytes.toBytes(val));
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            LOGGER.debug(">>Table info from object:" + table.getName());
            table.put(put);
        } catch (IOException e) {
            LOGGER.error("Error connecting to hbase, no insert done.", e);
        }
        LOGGER.debug(">>Insert completed in:{}", stopwatch.stop());
    }
}
