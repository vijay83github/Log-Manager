package com.cs.logs;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LogManager implements Callable<Map<JsonElement, JsonObject>> {
  private final FileChannel _channel;
  private final long _startLocation;
  private final int _size;
  //can be used in future if you want to collate all the information
  private final int _sequence_number;

  public static final String ID = "id";

  Logger log = LoggerFactory.getLogger(LogManager.class);

  public LogManager(FileChannel channel, long start_loc, int size, int sequence_number) {
    this._channel= channel;
    this._startLocation = start_loc;
    this._size = size;
    this._sequence_number = sequence_number;
  }

  @Override
  public Map<JsonElement, JsonObject> call() {
    Map<JsonElement, JsonObject> map = new HashMap<>();
    try {
      log.info("Reading the channel: {},  size: {}", _startLocation, _size);
      LogAlertEventWriter logAlertEventWriter = new LogAlertEventWriter();
      //allocate memory
      ByteBuffer buff = ByteBuffer.allocate(_size);

      //Read file chunk to RAM
      _channel.read(buff, _startLocation);

      //chunk to String
      //String string_chunk = new String(buff.array(), StandardCharsets.UTF_8);
      BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buff.array())));
      String logRecord;
      while ((logRecord = reader.readLine()) != null && !logRecord.equals("")) {
        JsonObject jsonObject = JsonParser.parseString(logRecord).getAsJsonObject();
        JsonElement id = jsonObject.get(ID);
        JsonObject existingObject;
        if ((existingObject = map.get(id)) != null) {
          logAlertEventWriter.publishAlertEvent(jsonObject, existingObject );
          map.remove(id);
        } else {
          map.put(id, jsonObject);
        }
        System.out.println(jsonObject.get(ID));
      }

      log.info("Done Reading the channel: {},  size: {}" , _startLocation, _size);
    } catch (Exception e) {
      log.error("Exception occurred while processing log records", e);
    }
    return map;
  }

}
