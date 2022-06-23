package com.cs.logs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cs.logs.db.Hsqldb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogAlertEventReader {
  private static final  Logger log = LoggerFactory.getLogger(LogAlertEventWriter.class);
  public List<Map<String, String>> readAllAlertEvents() throws SQLException {
    log.info("Started readAlertEvents()");
    final Connection connection =
        DriverManager.getConnection(Hsqldb.URI, Hsqldb.USER,
            Hsqldb.PASSWORD);

    final Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select * from \"alert_events\"");
    List<Map<String, String>> records = readResults(connection, statement, resultSet);
    log.info("Ended readAlertEvents()");
    return records;
  }

  private List<Map<String, String>> readResults(Connection connection, Statement statement, ResultSet resultSet) throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    List<Map<String, String>> records = new ArrayList<>();
    while (resultSet.next()) {
      Map<String, String> record = new HashMap<>();
      for (int i = 0; i < columnCount; i++) {
        record.put(metaData.getColumnLabel(i + 1), resultSet.getString(i + 1));
      }
      records.add(record);
    }
    statement.close();
    connection.close();
    return records;
  }

}
