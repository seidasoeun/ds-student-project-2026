import java.io.*;
import java.sql.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class Check {
  final static int N_NODES = 2; // initial nodes

  public static void main(String[] args) {
    String fileName = args[0];
    String line;
    SortedMap<Integer, Integer> numNodesInView = new TreeMap<>();
    SortedMap<Integer, Integer> numSentInView = new TreeMap<>();
    SortedMap<Integer, SortedMap<String, Integer>> numRecvInView = new TreeMap<>();

    //vsmanager view 1 of 3 nodes
    //vsnodeG1 multicasts 63 in view 6 to 3 nodes
    //vsnodeJ1 delivers 63 from vsnodeG1 in view 6

    try {
      BufferedReader reader =
        new BufferedReader(new FileReader(fileName));

      while((line = reader.readLine()) != null) {
        String[] l = line.split(" ");
        //System.out.println(Arrays.toString(l));
        if(l.length < 2) continue;

        String l0 = l[0].replaceAll("[^a-zA-Z0-9()]", "").toLowerCase(); // Normalize
        String l1 = l[1].replaceAll("[^a-zA-Z0-9()]", "").toLowerCase(); // Normalize
        String l2 = l[2].replaceAll("[^a-zA-Z0-9()]", "").toLowerCase(); // Normalize

        if (l1.equals("view")) {
          String l4 = l[4].replaceAll("[^a-zA-Z0-9()]", "").toLowerCase(); // Normalize
          int viewId = Integer.parseInt(l2);
          int numNodes = Integer.parseInt(l4);

          numNodesInView.put(viewId, numNodes);
        }
        else if (l1.equals("multicasts")) {
          String l5 = l[5].replaceAll("[^a-zA-Z0-9()]", "").toLowerCase(); // Normalize
          int viewId = Integer.parseInt(l5);
          //int numSent = Integer.parseInt(l[7]);
          int numSent = 1;
          //int seqnoSent = Integer.parseInt(l[2]);

          String sender = l0;

          numSentInView.compute(viewId, (Integer k, Integer v) -> {
            if (v==null) return numSent;
            else return v + numSent;
          });

          numRecvInView.putIfAbsent(viewId, new TreeMap<>());
          numRecvInView.get(viewId).compute(sender, (String k, Integer v) -> {
            if (v==null)
              return 1;
            else if (v < 0)
              return v;
            else
              return v + 1;
          });
          //System.out.println(numSentInView);
        }
        else if (l1.equals("delivers")) {
          String l7 = l[7].replaceAll("[^a-zA-Z0-9()]", "").toLowerCase(); // Normalize
          int viewId = Integer.parseInt(l7);
		      String receiver = l0;
          //int seqnoRecv = Integer.parseInt(l[2]);

          numRecvInView.putIfAbsent(viewId, new TreeMap<>());
          numRecvInView.get(viewId).compute(receiver, (String k, Integer v) -> {
            if (v==null)
              return 1;
            else if (v < 0)
              return v;
            else
              return v + 1;
          });
          //System.out.println(numRecvInView);
        } else if (l0.equals("(crashed)")) {
            String l8 = l[8].replaceAll("[^a-zA-Z0-9()]", "").toLowerCase(); // Normalize
			      String receiver = l1;
            int viewId = Integer.parseInt(l8);

            numRecvInView.putIfAbsent(viewId, new TreeMap<>());
            numRecvInView.get(viewId).put(receiver, -1);
		}
      }
      for (int x : numSentInView.keySet()) {
        int numNodes = numNodesInView.getOrDefault(x, N_NODES);
        int numSent = numSentInView.get(x);
        ArrayList<Integer> recvs = new ArrayList<>(numRecvInView.getOrDefault(x, new TreeMap<>()).values());
		recvs.removeIf(n -> n < 0);
        recvs.removeIf(n -> n == numSent);

        String perNodeCount = numRecvInView
                .getOrDefault(x, new TreeMap<>())
                .entrySet()
                .stream()
                .map(e -> "\n    " + e.getKey() + ":  " + e.getValue() + (e.getValue() < 0? " (crashed)": ""))
                .collect(Collectors.joining());

        System.out.println(
                "View: " + x
                  + "  Nodes: " + numNodes
                  + "  Sent: " + numSent
                  + "  Synch: " + recvs.isEmpty() + "\n"
                + "> Recv: " + perNodeCount
        );

      }
      reader.close();
    }
    catch(Exception ex) {
      ex.printStackTrace();
    }
  }
}
