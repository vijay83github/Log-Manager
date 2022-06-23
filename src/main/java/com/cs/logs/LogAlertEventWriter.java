package com.cs.logs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.cs.logs.db.Hsqldb;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogAlertEventWriter {

  public static final String TIMESTAMP = "timestamp";
  public static final String STATE = "state";
  public static final String TYPE = "type";
  public static final String HOST = "host";
  public static final String ID = "id";
  public static final String EMPTY_STRING = "";
  public static final String STATE_STARTED = "STARTED";

  private static final Logger log = LoggerFactory.getLogger(LogAlertEventWriter.class);

  void publishAlertEvent(JsonObject jsonObject, JsonObject existingObject) throws SQLException {
    log.info("started publishAlertEvent()");
    long duration = getDuration(jsonObject, existingObject);
    log.info("duration [{}]",duration);
    if (duration > 4L) {
      final Connection connection =
          DriverManager.getConnection(Hsqldb.URI, Hsqldb.USER,
              Hsqldb.PASSWORD);

      final Statement statement = connection.createStatement();
      String type = (null == jsonObject.get(TYPE)) ? EMPTY_STRING : jsonObject.get(TYPE).getAsString();
      String host = (null == jsonObject.get(HOST)) ? EMPTY_STRING : jsonObject.get(HOST).getAsString();
      statement.executeQuery(
          String.format("INSERT INTO \"alert_events\" VALUES ('%s', %d, '%s', '%s', %d)", jsonObject.get(ID).getAsString(), duration, type,
              host,
              1));
      statement.close();
      connection.close();
    }
    log.info("ended publishAlertEvent()");
  }

  long getDuration(JsonObject jsonObject, JsonObject existingObject) {
    return
        STATE_STARTED.equals(jsonObject.get(STATE).getAsString()) ? existingObject.get(TIMESTAMP).getAsLong() - jsonObject.get(TIMESTAMP)
            .getAsLong() :
            jsonObject.get(TIMESTAMP).getAsLong() - existingObject.get(TIMESTAMP).getAsLong();
  }

}
