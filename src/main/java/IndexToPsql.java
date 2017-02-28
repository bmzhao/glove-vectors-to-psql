import com.google.common.base.CharMatcher;
import org.postgresql.core.BaseConnection;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

/**
 * Store all glove vectors into a postgresql database w/3 columns, of id, the string, and a double array
 * http://nlp.stanford.edu/projects/glove/
 */
public class IndexToPsql {
    public static String postgreslifyArray(double[] input) {
        String[] result = Arrays.stream(input).mapToObj(Double::toString).toArray(String[]::new);
        return "{" + String.join(", ", result) + "}";
    }


    public static void main(String[] args) {
        Set<String> indexed = new HashSet<>(2196017);
        try {
            Class.forName("org.postgresql.Driver");
            BaseConnection conn = (BaseConnection) DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/glove",
                    "postgres",
                    "");

            Statement statement = conn.createStatement();
            String createTable =
                    "CREATE TABLE IF NOT EXISTS glove_vectors ( " +
                            "id BIGSERIAL PRIMARY KEY NOT NULL, " +
                            "string TEXT UNIQUE NOT NULL," +
                            "vector DOUBLE PRECISION[] " +
                            " )";
            statement.execute(createTable);

            String indexCreate =
                    "CREATE INDEX IF NOT EXISTS glove_string_index on glove_vectors (string)";
            statement.execute(indexCreate);


            long count = 0;
            Scanner scanner = new Scanner(new File("conf/glove.840B.300d.txt"));
            while (scanner.hasNextLine()) {
                String[] line = scanner.nextLine().split("\\s+");
                String word = line[0];
                double[] vector = new double[line.length - 1];
                for (int i = 1; i < line.length; ++i) {
                    vector[i - 1] = Double.parseDouble(line[i]);
                }
                if (!CharMatcher.ascii().matchesAllOf(word)) {
                    System.out.println("Skipping non-ascii word " + word);
                    continue;
                }
                word = conn.escapeString(word);
                if (indexed.contains(word)) {
                    System.out.println("Skipping already indexed word " + word);
                    continue;
                } else {
                    indexed.add(word);
                }


                String insertEntry =
                        "INSERT INTO glove_vectors(string,vector) VALUES ( '" +
                                word + "', '" +
                                postgreslifyArray(vector) +
                                "' );";
                statement.addBatch(insertEntry);
                if (++count % 1000 == 0) {
                    if (!executeBatch(statement)) {
                        count -= 1000;
                    }
                    System.out.println(count + " vectors inserted");
                }
            }
            if (!executeBatch(statement)) {
                count -= 1000;
            }
            System.out.println(count + " vectors inserted");
            statement.close();
            conn.close();
        } catch (ClassNotFoundException | SQLException | FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    private static boolean executeBatch(Statement statement) throws SQLException {
        try {
            statement.executeBatch();
            return true;
        } catch (BatchUpdateException e) {
            e.printStackTrace();
            System.out.println("Failed to batch update, skipping...");
            return false;
        }
    }
}
