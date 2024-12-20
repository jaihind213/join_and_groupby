package org.jag;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
  private static Logger log = LoggerFactory.getLogger(Utils.class);

  public static String generateRandomString(int length) {
    String characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789@$%";
    // Characters to choose from
    Random random = new Random();
    StringBuilder stringBuilder = new StringBuilder();

    for (int i = 0; i < length; i++) {
      int randomIndex = random.nextInt(characters.length());
      char randomChar = characters.charAt(randomIndex);
      stringBuilder.append(randomChar);
    }

    return stringBuilder.toString();
  }

  public static void recordTimeToFile(
      final long timeTakenMs, final String timeTakenBy, String filePath) {
    File file = new File(filePath);
    final boolean fileExists = file.exists();

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
      if (!fileExists) {
        // header
        writer.write("Approach,TimeTakenMs");
        writer.newLine();
      }
      writer.write(timeTakenBy + "," + timeTakenMs);
      writer.newLine(); // Add a newline after the appended line
      System.out.println("time taken recorded successfully.");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
