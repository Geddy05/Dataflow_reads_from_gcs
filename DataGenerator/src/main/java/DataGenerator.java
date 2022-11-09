import com.github.javafaker.Faker;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.Gson;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

public class DataGenerator {

  static final int amountOfPlayers = 150;
  static final int amountOfTeams = 10;

  public static List<Player> getPlayers(){
    Faker faker = new Faker();
    List<Player> players = new ArrayList<>();
    for (int i = 0; i < amountOfPlayers; i++){
      Player player = new Player();
      player.setUsername(faker.name().username());
      player.setUserId(i);
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

  private static String generateFilePath() {
    // Add create bucket and file name based on current datetime.
    Date date = new Date();
    DateFormat dateFormatPart1 = new SimpleDateFormat("yyyy-MM/dd", Locale.getDefault());
    DateFormat dateFormatPart2 = new SimpleDateFormat("HH/mm:ss", Locale.getDefault());
    String datePart1 = dateFormatPart1.format(date);
    String datePart2 = dateFormatPart2.format(date);
    return datePart1+"/"+datePart2;
  }

  private static String createScores(List<Player> players, Faker faker) throws InterruptedException {
    Gson gson = new Gson();
    StringBuilder scores = new StringBuilder();

    for (int i = 0; i < faker.number().numberBetween(20,40); i++){
      Player player = players.get(faker.number().numberBetween(0,amountOfPlayers-1));
      ScoreModel scoreModel = generateData(player);
      scores.append(gson.toJson(scoreModel)).append(System.lineSeparator());
      TimeUnit.MILLISECONDS.sleep(faker.number().numberBetween(500,2500));
    }

    return scores.toString();
  }

  public static void main(String[] args) throws InterruptedException {

    // Instantiates a client
    Storage storage = StorageOptions.getDefaultInstance().getService();

    // The name for the top level bucket
    String mainbucketName = "files2810";

    // Create Players
    List<Player> players = getPlayers();
    Faker faker = new Faker();

    Bucket bucket = storage.get(mainbucketName);


    while(true){
      // Creates the new bucket
      String value = createScores(players, faker);
      String filePath = generateFilePath();

      // Write to file in GCS
      byte[] bytes = value.getBytes(UTF_8);
      bucket.create(filePath, bytes);

      System.out.printf("Bucket %s created. %n", filePath);
    }
  }
}