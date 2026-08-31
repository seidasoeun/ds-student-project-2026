import java.io.*;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;

public class Check {
  public static void main(String[] args) {
    String fileName = args[0];
    String line;
    SortedMap<Integer, Integer> snapshots = new TreeMap<>();

    try {
      BufferedReader reader = new BufferedReader(new FileReader(fileName));
      
      while((line = reader.readLine()) != null) {
        String[] l = line.split(" ");
        String firstWord = l[0].replaceAll("[^a-zA-Z0-9]", "").toLowerCase(); // Normalize
        if (firstWord.equals("bank")) {
          int snapId = Integer.parseInt(l[3].replaceAll("[^a-zA-Z0-9]", "").toLowerCase());
          int balance = Integer.parseInt(l[5].replaceAll("[^a-zA-Z0-9]", "").toLowerCase());

          snapshots.compute(snapId, (Integer k, Integer v) -> {
            if (v==null) return balance;
            else return v + balance;
          });
        }
      }
      for (int x: snapshots.keySet()) {
        System.out.println("Snapshot ID: " + x + " total: " + snapshots.get(x));
      }
      reader.close();
    }
    catch(Exception ex) {
      ex.printStackTrace();
    }
  }
}
