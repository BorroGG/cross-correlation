package advisor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Advisor {
    //arg[0] = Stripes OR Pairs
    //arg[1] = "phone", "laptop", "xbox", "playstation", "fridge", "watch", "mayonnaise",
    //           "shampoo", "soap", "rope", "tie", "doll", "mirror", "tires", "engine",
    //           "bed", "closet", "coffee", "tea", "water", "gingerbread", "ketchup"
    public static void main(String[] args) throws IOException {
        String filePath = "/user/di/cross-correlation/output" + args[0] + "/cross-correlation/part-r-00000";
        String product = args[1];
        Configuration configuration = new Configuration();
        String hdfsPath = "hdfs://localhost:9000";
        configuration.set("fs.defaultFS", hdfsPath);
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(filePath);
        FSDataInputStream in = fileSystem.open(path);

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        Map<String, Integer> data = new HashMap<>();
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            if (line.contains(product)) {
                String[] tmp = line.split("\t");
                data.put(getFitProduct(product, tmp[0]), Integer.parseInt(tmp[1]));
            }
        }
        bufferedReader.close();
        in.close();
        fileSystem.close();

        Map<String, Integer> result = data.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue((a, b) -> b - a))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));

        int i = 1;
        for (Map.Entry<String, Integer> entry : result.entrySet()) {
            System.out.println(i++ + ") " + entry.getKey());
            if (i == 10) break;
        }

    }

    private static String getFitProduct(String product, String tmp) {
        String[] products = tmp.split(" ");
        return products[1].contains(product) ? products[0] : products[1];
    }
}
