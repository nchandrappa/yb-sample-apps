// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//
package com.yugabyte.sample.apps;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;

import com.yugabyte.sample.common.SimpleLoadGenerator.Key;

import io.codearte.jfairy.Fairy;
import io.codearte.jfairy.producer.person.Person;

/**
 * This workload writes and reads some random string keys from a postgresql table.
 */
public class SqlUserPrivacyData extends AppBase {
  private static final Logger LOG = Logger.getLogger(SqlUserPrivacyData.class);

  // Static initialization of this workload's config. These are good defaults for getting a decent
  // read dominated workload on a reasonably powered machine. Exact IOPS will of course vary
  // depending on the machine and what resources it has to spare.
  static {
    // Disable the read-write percentage.
    appConfig.readIOPSPercentage = -1;
    // Set the read and write threads to 1 each.
    appConfig.numReaderThreads = -1;
    appConfig.numWriterThreads = -1;
    // The number of keys to read.
    appConfig.numKeysToRead = -1;
    // The number of keys to write. This is the combined total number of inserts and updates.
    appConfig.numKeysToWrite = -1;
    // The number of unique keys to write. This determines the number of inserts (as opposed to
    // updates).
    appConfig.numUniqueKeysToWrite = NUM_UNIQUE_KEYS;  
  }

  // The default table name to create and use for CRUD ops.
  private static final String DEFAULT_TABLE1_NAME = "user_account";
  private static final String DEFAULT_TABLE2_NAME = "user_privacy";
  
  private static final String DEFAULT_TABLE1_COLUMNS = "userID,accountName,givenName,middleName,familyName,userGender,userAge,dob,address1,address2,city,zip,email,homePhone,mobilePhone,country,company,companyEmail,active";
  private static final String DEFAULT_TABLE2_COLUMNS = "userID,userPrivacyID,userGenderPrivacy,userAgePrivacy,userDobPrivacy,userAddressPrivacy,userEmailPrivacy,userHomePhonePrivacy,mobilePhonePrivacy,companyEmailPrivacy,createdDate";

  // The shared prepared select statement for fetching the data.
  private volatile PreparedStatement preparedSelect = null;

  // The shared prepared insert statement for Table 1.
  private volatile PreparedStatement preparedStatementForTable1 = null;

  // The shared prepared insert statement for Table 2.
  private volatile PreparedStatement preparedStatementForTable2 = null;

  // Lock for initializing prepared statement objects.
  private static final Object prepareInitLock = new Object();
  
  //jfairy fake data generator
  private static Fairy fairy;

  public SqlUserPrivacyData() {
    buffer = new byte[appConfig.valueSize];
    fairy = Fairy.create();
  }

  /**
   * Drop the table created by this app.
   */
  @Override
  public void dropTable() throws Exception {

    Connection connection = getPostgresConnection();
    connection.createStatement().execute("DROP TABLE IF EXISTS " + getTable2Name());
    LOG.info(String.format("Dropped table: %s", getTable2Name()));
    connection.createStatement().execute("DROP TABLE IF EXISTS " + getTable1Name());
    LOG.info(String.format("Dropped table: %s", getTable1Name()));

  }

  @Override
  public void createTablesIfNeeded() throws Exception {

//    Connection connection = getPostgresConnection();
//    connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
//
//    // (Re)Create the table (every run should start cleanly with an empty table).
//    connection.createStatement().execute(
//        String.format("DROP TABLE IF EXISTS %s", getTable2Name()));
//    connection.createStatement().execute(
//        String.format("DROP TABLE IF EXISTS %s", getTable1Name()));
//    LOG.info("Dropping table(s) left from previous runs if any");
//
//    try {
//      // Create the "users" table.
//      connection.createStatement().executeUpdate(
//          String.format("CREATE TABLE %s (user_id TEXT PRIMARY KEY, user_details TEXT);", getTable1Name()));
//      LOG.info(String.format("Created table: %s", getTable1Name()));
//
//      // Create the "orders" table.
//      connection.createStatement().executeUpdate(
//          String.format("CREATE TABLE %s (" + 
//                        "  user_id TEXT, " +
//                        "  order_time TIMESTAMP, " +
//                        "  order_id UUID DEFAULT gen_random_uuid(), " + 
//                        "  order_details TEXT, " +
//                        "  PRIMARY KEY (user_id, order_time DESC, order_id), " +
//                        "  FOREIGN KEY (user_id) REFERENCES %s (user_id)" +
//                        ");", getTable2Name(), getTable1Name()));
//      LOG.info(String.format("Created table: %s", getTable2Name()));
//    } catch (org.postgresql.util.PSQLException e) {
//      LOG.error("Failed to create tables", e);
//      System.err.println("\n================================================");
//      System.err.println("If you hit the following:");
//      System.err.println("    ERROR: function gen_random_uuid() does not exist");
//      System.err.println("  Run this cmd in ysqlsh: CREATE EXTENSION \"pgcrypto\";");
//      System.err.println("==================================================\n");
//      System.exit(0);
//    }

    // Pre-populate the users table with the desired number of users.
    
//    for (int idx = 0; idx < NUM_USERS_AT_START; idx++) {
//      insertUser();
//    }

  }

  public String getTable1Name() {
    return DEFAULT_TABLE1_NAME.toLowerCase();
  }

  public String getTable2Name() {
    return DEFAULT_TABLE2_NAME.toLowerCase();
  }

  private PreparedStatement getPreparedSelect() throws Exception {
    if (preparedSelect == null) {
      synchronized (prepareInitLock) {
        if (preparedSelect == null) {
        	 if (appConfig.actionType.equalsIgnoreCase(AppConfig.ACTION_TYPES.joinquery.toString())) {
		          preparedSelect = getPostgresConnection().prepareStatement(
		              String.format("SELECT * FROM %s, %s " + 
		                            "WHERE %s.userid = ? AND %s.userid = %s.userid", 
		                            getTable1Name(), getTable2Name(),
		                            getTable1Name(), getTable1Name(), getTable2Name()));
        		 
        		 
        	 } else {
//        		 preparedSelect = getPostgresConnection().prepareStatement(
//      		              String.format("SELECT * FROM %s WHERE %s.userid = ?", getTable1Name(), getTable1Name()));
        		 
        		 preparedSelect = getPostgresConnection().prepareStatement(
      		              String.format("SELECT " +
      		            		  "coalesce(json_agg(\"root\"), '[]') AS \"root\"" +
      		            		" FROM " +
      		            		  "(" +
      		            		    " SELECT " +
      		            		      "row_to_json(" +
      		            		        "(" +
      		            		          " SELECT " +
      		            		            "\"_1_e\"" +
      		            		          " FROM " +
      		            		            "(" +
      		            		              " SELECT " +
      		            		                "\"_0_root.base\".\"accountname\" AS \"accountname\"," +
      		            		                "\"_0_root.base\".\"givenname\" AS \"givenname\"," +
      		            		                "\"_0_root.base\".\"familyname\" AS \"familyname\"," +
      		            		                "\"_0_root.base\".\"city\" AS \"city\"," +
      		            		                "\"_0_root.base\".\"company\" AS \"company\"" +
      		            		            ") AS \"_1_e\"" +
      		            		        ")" +
      		            		      ") AS \"root\"" +
      		            		    " FROM " +
      		            		      "(" +
      		            		        " SELECT " +
      		            		          "*" +
      		            		        " FROM " +
      		            		          "\"public\".\"user_account\"" +
      		            		        " WHERE " +
      		            		          "(" +
      		            		            "(\"public\".\"user_account\".\"userid\") = ((?) :: bigint)" +
      		            		          ")" +
      		            		      ") AS \"_0_root.base\"" +
      		            		  ") AS \"_2_root\""));
        		 
        		 LOG.info("Select prepared statement: " + preparedSelect.toString());
        	 }
        }
      }
    }
    return preparedSelect;
  }

  @Override
  public long doRead() {
    Key key = getSimpleLoadGenerator().getKeyToRead();
    if (key == null) {
      return 0;
    }

    try {
      PreparedStatement statement = getPreparedSelect();
      Long primaryKeyValue = appConfig.loadStartSequence + key.asNumber();
      statement.setLong(1, primaryKeyValue);
      try (ResultSet rs = statement.executeQuery()) {
        int count = 0;
        while (rs.next()) {
          ++count;
          // Get data from the current row and use it
        }
        LOG.debug("Got " + count + " orders for user : " + key.toString());
      }
    } catch (Exception e) {
      LOG.fatal("Failed reading value: " + key.getValueStr(), e);
      return 0;
    }
    return 1;
  }

  private PreparedStatement getPreparedStatementForTable1() throws Exception {
    if (preparedStatementForTable1 == null) {
      synchronized (prepareInitLock) {
        if (preparedStatementForTable1 == null) {   	
          StringBuilder sb = new StringBuilder();
          sb.append("INSERT INTO ").append(getTable1Name()).append("(");
          String [] columnNames = DEFAULT_TABLE1_COLUMNS.split(",");
          sb.append(columnNames[0]);
          for(int i = 1; i < columnNames.length; i++) {
        	  sb.append(", ");
        	  sb.append(columnNames[i]);
          }
          sb.append(") VALUES ( ?");
          for (int i =1; i < columnNames.length; i++) {
        	  sb.append(", ?");
          }
          sb.append(")");
          preparedStatementForTable1 = getPostgresConnection().prepareStatement(sb.toString());
          LOG.info("Insert prepared statement: " + sb.toString());
        }
      }
    }
    return preparedStatementForTable1;
  }

  private PreparedStatement getPreparedStatementForTable2() throws Exception {
	  if (preparedStatementForTable2 == null) {
	      synchronized (prepareInitLock) {
	        if (preparedStatementForTable2 == null) { 
	          StringBuilder sb = new StringBuilder();
	          sb.append("INSERT INTO ").append(getTable2Name()).append("(");
	          String [] columnNames = DEFAULT_TABLE2_COLUMNS.split(",");
	          sb.append(columnNames[0]);
	          for(int i = 1; i < columnNames.length; i++) {
	        	  sb.append(", ");
	        	  sb.append(columnNames[i]);
	          }
	          sb.append(") VALUES ( ?");
	          for (int i =1; i < columnNames.length; i++) {
	        	  sb.append(", ?");
	          }
	          sb.append(")");
	          preparedStatementForTable2 = getPostgresConnection().prepareStatement(sb.toString());
	          LOG.info("Insert prepared statement: " + sb.toString());
	        }
	      }
	    }
	    return preparedStatementForTable2;
  }
  
  private PreparedStatement getPreparedStatementForUpdateTable1() throws Exception {
	    if (preparedStatementForTable1 == null) {
	      synchronized (prepareInitLock) {
	        if (preparedStatementForTable1 == null) {   	
	          StringBuilder sb = new StringBuilder();
	          sb.append("UPDATE ").append(getTable1Name()).append(" ");
	          sb.append("SET city = ? ");
	          sb.append("WHERE userID = ?");
	          preparedStatementForTable1 = getPostgresConnection().prepareStatement(sb.toString());
	          LOG.info("Insert prepared statement: " + sb.toString());
	        }
	      }
	    }
	    return preparedStatementForTable1;
	  }

  @Override
  public long doWrite(int threadIdx) {
	  
    if (appConfig.actionType.equalsIgnoreCase(AppConfig.ACTION_TYPES.loadprimary.toString())) {
      return insertDataTable1();
    } else {
      return insertDataTable2();
    } 
  }

	private long insertDataTable1() {

		HashSet<Key> keys = new HashSet<>();
		int result = 0;
		try {
			PreparedStatement statement = getPreparedStatementForTable1();
			if (appConfig.batchSize > 0) {
				for (int i = 0; i < appConfig.batchSize; i++) {
					keys.add(preparedStatementMapper(statement));
					statement.addBatch();
				}
				statement.executeBatch();
			} else {
				keys.add(preparedStatementMapper(statement));
				result = statement.executeUpdate();
			}
			for (Key key : keys) {
				getSimpleLoadGenerator().recordWriteSuccess(key);
			}
			return 1;
		} catch (Exception e) {

			for (Key key : keys) {
				getSimpleLoadGenerator().recordWriteFailure(key);
			}
			LOG.fatal("Failed write with error: " + e.getMessage());
		}
		return 0;
	}
  
  private Key preparedStatementMapper( PreparedStatement statement) throws SQLException {
	  
	  Key key = getSimpleLoadGenerator().getKeyToWrite();
	  Person userData = fairy.person();
      Long primaryKeyValue = appConfig.loadStartSequence + key.asNumber();
      
	  statement.setLong(1, primaryKeyValue);
      statement.setString(2, userData.username());
      statement.setString(3, userData.firstName());
      statement.setString(4, userData.middleName());
      statement.setString(5, userData.lastName());
      statement.setString(6, userData.sex().toString());
      statement.setInt(7, userData.age());
      statement.setTimestamp(8, new Timestamp(userData.dateOfBirth().getMillis()));
      statement.setString(9, userData.getAddress().toString());
      statement.setString(10, userData.getAddress().toString());
      statement.setString(11, userData.getAddress().getCity());
      statement.setString(12, userData.getAddress().getPostalCode());
      statement.setString(13, userData.email());
      statement.setString(14, userData.telephoneNumber());
      statement.setString(15, userData.telephoneNumber());
      statement.setString(16, "United States of America");
      statement.setString(17, userData.getCompany().name());
      statement.setString(18, userData.companyEmail());
      statement.setBoolean(19, Math.random() < 0.5); userData.getAddress();
      
      return key;  
  }
  
  private long insertDataTable2() {
	  HashSet<Key> keys = new HashSet<>();
		int result = 0;
		try {
			PreparedStatement statement = getPreparedStatementForTable2();
			if (appConfig.batchSize > 0) {
				for (int i = 0; i < appConfig.batchSize; i++) {

					insertMultipleRecordOfPrimaryTable(statement, 4, keys);
				}
				statement.executeBatch();
			} else {
				Key key = getSimpleLoadGenerator().getKeyToWrite();
				for(int i = 5; i <= 9; i++) {
					keys.add(preparedStatementMapperTable2(statement, i, key));
					result = statement.executeUpdate();
				}
			}
			for (Key key : keys) {
				getSimpleLoadGenerator().recordWriteSuccess(key);
			}
			return 1;
		} catch (Exception e) {

			for (Key key : keys) {
				getSimpleLoadGenerator().recordWriteFailure(key);
			}
			LOG.fatal("Failed write with error: " + e.getMessage());
		}
		return 0;
	  }
  
  private long insertAdditionalDataTable2() {
	  HashSet<Key> keys = new HashSet<>();
		int result = 0;
		try {
			PreparedStatement statement = getPreparedStatementForTable2();
			if (appConfig.batchSize > 0) {
				for (int i = 0; i < appConfig.batchSize; i++) {

					insertMultipleRecordOfPrimaryTable(statement, 4, keys);
				}
				statement.executeBatch();
			} else {
				Key key = getSimpleLoadGenerator().getKeyToWrite();
				for(int i = 5; i <= 8; i++) {
					keys.add(preparedStatementMapperTable2(statement, i, key));
					result = statement.executeUpdate();
				}
			}
			for (Key key : keys) {
				getSimpleLoadGenerator().recordWriteSuccess(key);
			}
			return 1;
		} catch (Exception e) {

			for (Key key : keys) {
				getSimpleLoadGenerator().recordWriteFailure(key);
			}
			LOG.fatal("Failed write with error: " + e.getMessage());
		}
		return 0;
	  }
  
  private void insertMultipleRecordOfPrimaryTable (PreparedStatement statement, 
		  int ratioToPrimaryTable, HashSet<Key> keys) throws SQLException {
	  Key key = getSimpleLoadGenerator().getKeyToWrite();
	  for(int i = 0; i < ratioToPrimaryTable; i++) {
		  keys.add(preparedStatementMapperTable2(statement, i, key));
		  statement.addBatch();
	  }
  }
  
  private Key preparedStatementMapperTable2( PreparedStatement statement, int seqNo, Key key) throws SQLException {
	  
      Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
      Long primaryKeyValue = appConfig.loadStartSequence + key.asNumber();
      
      statement.setLong(1, primaryKeyValue);
      statement.setLong(2, seqNo);
      statement.setBoolean(3, Math.random() < 0.5);
      statement.setBoolean(4, Math.random() < 0.5);
      statement.setBoolean(5, Math.random() < 0.5);
      statement.setBoolean(6, Math.random() < 0.5);
      statement.setBoolean(7, Math.random() < 0.5);
      statement.setBoolean(8, Math.random() < 0.5);
      statement.setBoolean(9, Math.random() < 0.5);
      statement.setBoolean(10, Math.random() < 0.5);
      statement.setTimestamp(11, timeStamp);
      
      return key;
  }

  @Override
  public List<String> getWorkloadDescription() {
    return Arrays.asList(
        "Simple user profile and privacy app based on SQL. The app creates unique user ids and adds ",
        "privacy selection made by each user.");
  }

  @Override
  public List<String> getWorkloadOptionalArguments() {
    return Arrays.asList(
    	"--num_writes " + appConfig.numKeysToWrite,
        "--num_unique_keys " + appConfig.numUniqueKeysToWrite,
        "--num_threads_read " + appConfig.numReaderThreads,
        "--num_threads_write " + appConfig.numWriterThreads,
        "--data_load_prefix " + appConfig.loadStartSequence,
        "--action_type" + appConfig.actionType,
        "--batch_size " + appConfig.batchSize,
        "--num_reads" + appConfig.numKeysToRead,
        "--max_written_key" + appConfig.maxWrittenKey,
        "--username " + "<DB USERNAME>",
        "--password " + "<OPTIONAL PASSWORD>"
        );
  }
}
