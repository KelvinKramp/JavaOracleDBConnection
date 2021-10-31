/* Copyright (c) 2020, Oracle and/or its affiliates. All rights reserved.
Licensed under the Universal Permissive License v 1.0 
as shown at http://oss.oracle.com/licenses/upl */

/*
 DESCRIPTION
 The code sample demonstrates establishing a connection to Autonomous Database (ATP/ADW) using
 Oracle JDBC driver and Universal Connection Pool (UCP). It does the following.  
 
 (a)  
 (b) Set the connection pool properties(e.g.,minPoolSize, maxPoolSize). 
 (c) Get the connection and perform some database operations. 
 For a quick test, the sample retrieves 20 records from the Sales History (SH) schema 
 that is accessible to any DB users on autonomous Database.  
 
 Step 1: Enter the Database details DB_URL and DB_USER. 
 You will need to enter the DB_PASSWORD of your Autonomous Database through console
 while running the sample.  
 Step 2: Download the latest Oracle JDBC driver(ojdbc8.jar) and UCP (ucp.jar) 
 along with oraclepki.jar, osdt_core.jar and osdt_cert.jar and add to your classpath.  
 Refer to https://www.oracle.com/database/technologies/maven-central-guide.html               
 Step 3: Compile and Run the sample. 
 
 SH Schema: 
 This sample uses the Sales History (SH) sample schema. SH is a data set suited for 
 online transaction processing operations. The Star Schema Benchmark (SSB) sample schema 
 is available for data warehousing operations. Both schemas are available 
 with your shared ADB instance and do not count towards your storage. 
 ou can use any ADB user account to access these schemas.
 
 NOTES
 Use JDK 1.8 and above 
  
 MODIFIED    (MM/DD/YY)
 nbsundar    11/09/2020 - Creation 
 */
// #######################################################################################
// Retrieved from quick start QuickStart Java applications with Oracle Autonomous Database
// https://www.oracle.com/database/technologies/getting-started-using-jdbc.html
// #######################################################################################

package com.oracle.jdbctest;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

import oracle.ucp.jdbc.PoolDataSourceFactory;
import oracle.ucp.jdbc.PoolDataSource;

/*
 * The sample demonstrates connecting to Autonomous Database using 
 * Oracle JDBC driver and UCP as a client side connection pool.
 */
public class ADBQuickStart {  
 
  public static void main(String args[]) throws Exception {
    // Make sure to have Oracle JDBC driver 18c or above 
    // to pass TNS_ADMIN as part of a connection URL.
    // TNS_ADMIN - Should be the path where the client credentials zip (wallet_dbname.zip) file is downloaded. 
    // dbname_medium - It is the TNS alias present in tnsnames.ora.
    final String DB_URL="";
    // Update the Database Username and Password to point to your Autonomous Database
    final String DB_USER = "";
    final String DB_PASSWORD = "" ;
    // Use OracleDataSource as a datasource object. A DataSource object is a factory for Connection objects. 
    final String CONN_FACTORY_CLASS_NAME="oracle.jdbc.pool.OracleDataSource";
    
    // For security purposes, you must enter the password through the console 
    try {
      //Scanner scanner = new Scanner(System.in);
      //System.out.print("Enter the password for Autonomous Database: ");
      //DB_PASSWORD = scanner.nextLine();
      System.out.println("ADBQuickStart - connecting to database : ");
    }
    catch (Exception e) {    
       System.out.println("ADBQuickStart - Exception occurred : " + e.getMessage());
       System.exit(1);
    } 
    
    // Get the PoolDataSource for UCP (Universal Connection Pool)
    // In software engineering, a connection pool is a cache of database connections 
    // maintained so that the connections can be reused when future requests to the 
    // database are required. Connection pools are used to enhance the performance of 
    // executing commands on a database.
    PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();

    // Factory pattern is used to create instances of different classes of the same superclass. For instance
    // a random number generator that is used to pick a type (subclass) of enemy (superclass). A class is
    // chosen at runtime during the game. For more info see:  https://www.youtube.com/watch?v=ub0DXaeV6hA
    // Below line sets the data source pool class instance name (connection factory name) to the factory
    // for connection objects.
    // oracle.jdbc.pool.OracleDataSource. 
    pds.setConnectionFactoryClassName(CONN_FACTORY_CLASS_NAME);
    // Set other properties
    pds.setURL(DB_URL);
    pds.setUser(DB_USER);
    pds.setPassword(DB_PASSWORD);
    pds.setConnectionPoolName("JDBC_UCP_POOL");

    // Default is 0. Set the initial number of connections to be created
    // when UCP is started.
    pds.setInitialPoolSize(5);

    // Default is 0. Set the minimum number of connections
    // that is maintained by UCP at runtime.
    pds.setMinPoolSize(5);

    // Default is Integer.MAX_VALUE (2147483647). Set the maximum number of
    // connections allowed on the connection pool.
    pds.setMaxPoolSize(20);


    // Get the database connection from UCP.
    try (Connection conn = pds.getConnection()) {
      System.out.println("Available connections after checkin: "
          + pds.getAvailableConnectionsCount());
      System.out.println("Borrowed connections after checkin: "
          + pds.getBorrowedConnectionsCount());       
      // Perform a database operation
      doSQLWork(conn);
    } catch (SQLException e) {
        System.out.println("ADBQuickStart - "
          + "doSQLWork()- SQLException occurred : " + e.getMessage());
    } 
    
    System.out.println("Available connections after checkin: "
        + pds.getAvailableConnectionsCount());
    System.out.println("Borrowed connections after checkin: "
        + pds.getBorrowedConnectionsCount());
  }
 /*
 * Selects 20 rows from the SH (Sales History) Schema that is the accessible to all 
 * the database users of autonomous database. 
 */
 private static void doSQLWork(Connection conn) throws SQLException {
    String queryStatement = "SELECT CUST_ID, CUST_FIRST_NAME, CUST_LAST_NAME, CUST_CITY," 
      + "CUST_CREDIT_LIMIT FROM SH.CUSTOMERS WHERE ROWNUM < 20 order by CUST_ID";
      
    System.out.println("\n Query is " + queryStatement);
    
    conn.setAutoCommit(false);
    // Prepare a statement to execute the SQL Queries.
    try (Statement statement = conn.createStatement(); 
      // Select 20 rows from the CUSTOMERS table from SH schema. 
      ResultSet resultSet = statement.executeQuery(queryStatement)) {
        System.out.println(String.join(" ", "\nCUST_ID", "CUST_FIRST_NAME", 
             "CUST_LAST_NAME", "CUST_CITY", "CUST_CREDIT_LIMIT"));
        System.out.println("-----------------------------------------------------------");
        while (resultSet.next()) {
          System.out.println(resultSet.getString(1) + " " + resultSet.getString(2) + " " +
           resultSet.getString(3)+ " " + resultSet.getString(4) + " " +
           resultSet.getInt(5));
        }
      System.out.println("\nCongratulations! You have successfully used Oracle Autonomous Database\n");
      } 
  } // End of doSQLWork
  
} // End of ADBQuickStart