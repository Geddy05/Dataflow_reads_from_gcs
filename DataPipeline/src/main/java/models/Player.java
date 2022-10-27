package models;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.Date;

@DefaultCoder(AvroCoder.class)
public class Player implements Serializable {
  public String username;
  public int userId;
  public int teamId;
  public int points;
  public Date timestamp;
}
