package com.cs.logs;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LogAlertEventWriterTest {

  LogAlertEventWriter cut;
  LogAlertEventReader reader;

  @BeforeEach
  void setUp() {
    cut = new LogAlertEventWriter();
    reader = new LogAlertEventReader();
  }

  @AfterEach
  void tearDown() {
    cut = null;
    reader = null;
  }

  @Test
  void publishAlertEvent() throws SQLException {
    JsonObject jsonObject1 = JsonParser.parseString(
            "{\"id\": \"scsmbstgrd\",\"state\": \"STARTED\",\"type\": \"APPLICATION_LOG\",\"host\": \"11\",\"timestamp\": \"1655903091910\"}")
        .getAsJsonObject();
    JsonObject jsonObject2 = JsonParser.parseString(
            "{\"id\": \"scsmbstgrd\",\"state\": \"FINISHED\",\"type\": \"APPLICATION_LOG\",\"host\": \"11\",\"timestamp\": \"1655903091915\"}")
        .getAsJsonObject();
    cut.publishAlertEvent(jsonObject1, jsonObject2);
    List<Map<String, String>> records = reader.readAllAlertEvents();
    List<Map<String, String>> tx = records.stream()
        .filter(map -> (map.get("event_id").equals("scsmbstgrd")))
        .collect(Collectors.toList());
    assertEquals(1, tx.size());
    assertEquals("5", records.get(0).get("duration"));
  }

  @Test
  void getDuration() {
    JsonObject jsonObject1 = JsonParser.parseString(
            "{\"id\": \"scsmbstgrd\",\"state\": \"STARTED\",\"type\": \"APPLICATION_LOG\",\"host\": \"11\",\"timestamp\": \"1655903091910\"}")
        .getAsJsonObject();
    JsonObject jsonObject2 = JsonParser.parseString(
            "{\"id\": \"scsmbstgrd\",\"state\": \"FINISHED\",\"type\": \"APPLICATION_LOG\",\"host\": \"11\",\"timestamp\": \"1655903091915\"}")
        .getAsJsonObject();
    assertEquals(5L, cut.getDuration(jsonObject1, jsonObject2));
  }
}
