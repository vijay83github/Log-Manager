package com.cs.logs;

import static java.lang.Math.toIntExact;

import java.io.FileInputStream;
import java.nio.channels.FileChannel;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogManagerMain {
  private static final Logger log = LoggerFactory.getLogger(LogManagerMain.class);

  public static void main(String[] args) throws Exception {
    String filePath;
    String numberOfThreads;
    if(args.length > 0){
      filePath = args[0];
      numberOfThreads =  args[1];
    }else{
      filePath = "src/main/resources/test.log";
      numberOfThreads =  "1";
    }
    FileInputStream fileInputStream = new FileInputStream(filePath);
    FileChannel channel = fileInputStream.getChannel();
    long remaining_size = channel.size(); //get the total number of bytes in the file
    long chunk_size = remaining_size / Integer.parseInt(numberOfThreads); //file_size/threads

    //Max allocation size allowed is ~2GB
    if (chunk_size > (Integer.MAX_VALUE - 5)) {
      chunk_size = (Integer.MAX_VALUE - 5);
    }

    log.info("Started thread execution");
    //thread pool
    ExecutorService executor = Executors.newFixedThreadPool(Integer.parseInt(numberOfThreads));
    LogManager theOne;
    long start_loc = 0;//file pointer
    int i = 0; //loop counter
    Map<JsonElement, JsonObject> resultMap = new HashMap<>();
    while (remaining_size >= chunk_size) {
      //launches a new thread
      theOne = new LogManager(channel, start_loc, toIntExact(chunk_size), i);
      Future<Map<JsonElement, JsonObject>> futureMap = executor.submit(theOne);
      collateRemainingRecords(futureMap.get(), resultMap);
      remaining_size = remaining_size - chunk_size;
      start_loc = start_loc + chunk_size;
      i++;
    }

    //load the last remaining piece
    Future<Map<JsonElement, JsonObject>> futureMap = executor.submit(new LogManager(channel, start_loc, toIntExact(remaining_size), i));
    collateRemainingRecords(futureMap.get(), resultMap);
    //Tear Down
    executor.shutdown();

    //Wait for all threads to finish
    while (!executor.isTerminated()) {
      //wait for some time
      TimeUnit.SECONDS.sleep(5);
    }
    log.info("Finished all threads");
    fileInputStream.close();
  }

  private static void collateRemainingRecords(Map<JsonElement, JsonObject> futureMap, Map<JsonElement, JsonObject> resultMap)
      throws SQLException {
    LogAlertEventWriter eventWriter = new LogAlertEventWriter();
    for (Map.Entry<JsonElement, JsonObject> entry : futureMap.entrySet()) {
      if (resultMap.containsKey(entry.getKey())) {
        eventWriter.publishAlertEvent(resultMap.get(entry.getKey()), entry.getValue());
      } else {
        resultMap.put(entry.getKey(), entry.getValue());
      }
    }
  }

}
