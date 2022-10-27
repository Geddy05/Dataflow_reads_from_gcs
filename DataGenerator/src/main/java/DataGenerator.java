import com.github.javafaker.Faker;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.google.cloud.Timestamp;

import static java.nio.charset.StandardCharsets.UTF_8;

public class DataGenerator {

  static final int amountOfPlayers = 150;
  static final int amountOfTeams = 50;

  public static List<Player> getPlayers(){
    Faker faker = new Faker();
    List<Player> players = new ArrayList<>();
    for (int i = 0; i < amountOfPlayers; i++){
      Player player = new Player();
      player.setUsername(faker.name().username());
      player.setUserId(faker.number().numberBetween(0,amountOfPlayers));
      player.setTeamId(faker.number().numberBetween(0,amountOfTeams));
      players.add(player);
    }
    return players;
  }

  public static ScoreModel generateData(Player player){
    Faker faker = new Faker();
    ScoreModel score = new ScoreModel();
    score.setUsername(player.getUsername());
    score.setUserId(player.getUserId());
    score.setTeamId(player.getTeamId());
    score.setPoints(faker.number().numberBetween(0,100));
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.getDefault());
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    score.setTimestamp(dateFormat.format(new Date()));
    return score;

  }

  public static void main(String[] args) throws IOException, InterruptedException {

    // Instantiates a client
    Storage storage = StorageOptions.getDefaultInstance().getService();

    // The name for the new bucket
    String mainbucketName = "files2809"; // "my-new-bucket";

    // Create Players
    List<Player> players = getPlayers();
    Faker faker = new Faker();

    Bucket bucket = storage.get(mainbucketName);


    while(true){
      // Creates the new bucket
      List<ScoreModel> scores = new ArrayList<>();
      for (int i = 0; i < faker.number().numberBetween(0,100); i++){
        Player player = players.get(faker.number().numberBetween(0,amountOfPlayers-1));
        ScoreModel scoreModel = generateData(player);
        scores.add(scoreModel);
        TimeUnit.MILLISECONDS.sleep(faker.number().numberBetween(250,750));
      }

      Gson gson = new Gson();
      String value = gson.toJson(scores);
      byte[] bytes = value.getBytes(UTF_8);

      Date date = new Date();
      DateFormat dateFormatPart1 = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault());
      DateFormat dateFormatPart2 = new SimpleDateFormat("HH:mm:ss", Locale.getDefault());
      String datePart1 = dateFormatPart1.format(date);
      String datePart2 = dateFormatPart2.format(date);
      String bucketName = datePart1+"/"+datePart2;

      Blob blob = bucket.create(bucketName, bytes);

      System.out.printf("Bucket %s created. %n", bucketName);
    }
  }
}