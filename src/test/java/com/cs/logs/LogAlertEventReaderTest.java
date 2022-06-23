package com.cs.logs;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LogAlertEventReaderTest {

  LogAlertEventReader cut;

  @BeforeEach
  void setUp() {
    cut = new LogAlertEventReader();
  }

  @AfterEach
  void tearDown() {
    cut = null;
  }

  @Test
  void readAlertEvents() throws Exception {
    String[] inputs = {"src/test/resources/test.log", "1"};
    LogManagerMain.main(inputs);
    List<Map<String, String>> records = cut.readAllAlertEvents();
    assertEquals(3, records.size());
  }

  @Test
  void readAlertEvents_emptyLogFile() throws Exception {
    String[] inputs = {"src/test/resources/testEmptyContent.log", "1"};
    LogManagerMain.main(inputs);
    List<Map<String, String>> records = cut.readAllAlertEvents();
    records = records.stream()
        .filter(map -> (map.get("event_id").equals("")))
        .collect(Collectors.toList());

    assertEquals(0, records.size());
  }
}
