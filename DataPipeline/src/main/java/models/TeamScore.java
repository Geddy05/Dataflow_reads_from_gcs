package models;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@DefaultCoder(AvroCoder.class)
public class TeamScore implements Serializable {
  private List<Player> playerList;
  private int totalScore;
  private int teamId;
  private Date timestamp;

  public List<Player> getPlayerList() {
    return playerList;
  }

  public void setPlayerList(List<Player> playerList) {
    this.playerList = playerList;
  }

  public void addPlayer(Player player){
    if(playerList == null){
      playerList = new ArrayList<>();
    }
    playerList.add(player);
  }

  public int getTotalScore() {
    return totalScore;
  }

  public void setTotalScore(int totalScore) {
    this.totalScore = totalScore;
  }

  public int getTeamId() {
    return teamId;
  }

  public void setTeamId(int teamId) {
    this.teamId = teamId;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Date timestamp) {
    this.timestamp = timestamp;
  }
}
