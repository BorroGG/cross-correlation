package generator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class Generator {
    public static void main(String[] args) throws IOException {
        //22 products
        List<String> products = Arrays.asList(
           "phone", "laptop", "xbox", "playstation", "fridge", "watch", "mayonnaise",
           "shampoo", "soap", "rope", "tie", "doll", "mirror", "tires", "engine",
           "bed", "closet", "coffee", "tea", "water", "gingerbread", "ketchup");
        List<List<String>> orders = new LinkedList<>();
        double orderCounts = 100 + Math.random() * 500;
        for (int i = 0; i < orderCounts; i++) {
            double orderSize = 1 + Math.random() * 30;
            List<String> currentOrder = new LinkedList<>();
            for (int j = 0; j < orderSize; j++) {
                currentOrder.add(products.get((int) (Math.random() * 22)));
            }
            orders.add(currentOrder);
        }
        try (BufferedWriter out = new BufferedWriter(new FileWriter("orders"))) {
            for (List<String> order : orders) {
                StringBuilder sb = new StringBuilder();
                for (String s : order) {
                    sb.append(s).append(" ");
                }
                sb.append("\n");
                out.write(sb.toString());
            }
            out.flush();
        }
    }
}
