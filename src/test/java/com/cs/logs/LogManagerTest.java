package com.cs.logs;

import static java.lang.Math.toIntExact;
import static org.junit.jupiter.api.Assertions.*;

import java.io.FileInputStream;
import java.nio.channels.FileChannel;
import java.util.Map;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class LogManagerTest {

  LogManager cut;


  @AfterEach
  void tearDown() {
    cut = null;
  }

  @Test
  void call() throws Exception{
    String filePath = "src/test/resources/test.log";
    FileInputStream fileInputStream = new FileInputStream(filePath);
    FileChannel channel = fileInputStream.getChannel();
    long remaining_size = channel.size(); //get the total number of bytes in the file
    long chunk_size = remaining_size / 1; //file_size/threads

    //load the last remaining piece
    Map<JsonElement, JsonObject> futureMap = new LogManager(channel, 0, toIntExact(chunk_size), 1).call();

    fileInputStream.close();
    assertEquals(0, futureMap.size());
  }

  @Test
  void call_withEmptyContent() throws Exception{
    String filePath = "src/test/resources/testEmptyContent.log";
    FileInputStream fileInputStream = new FileInputStream(filePath);
    FileChannel channel = fileInputStream.getChannel();
    long remaining_size = channel.size(); //get the total number of bytes in the file
    long chunk_size = remaining_size / 1; //file_size/threads

    //load the last remaining piece
    Map<JsonElement, JsonObject> futureMap = new LogManager(channel, 0, toIntExact(chunk_size), 1).call();

    fileInputStream.close();
    assertEquals(0, futureMap.size());
  }
}
