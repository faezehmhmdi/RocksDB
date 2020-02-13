package aut.db;
/**
 * Main Class of RocksDB project
 * This class handles creation, deletion, fetching and updating
 *
 * @author Faezeh
 * @version Feb 13,2020
 */

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws FileNotFoundException, RocksDBException {

        //CSV file path
        File file = new File("./American Stock Exchange 20200206_Names_ClosedVal.csv");
        Scanner scanner = new Scanner(file);

        //Opening of the database
        RocksDB.loadLibrary();
        final Options options = new Options().setCreateIfMissing(true);
        RocksDB db = RocksDB.open(options, "./db");

        //Putting the keys and values from CSV file into database
        while (scanner.hasNextLine()) {
            String[] string = scanner.nextLine().split(",");
            db.put(string[0].getBytes(), string[1].getBytes());
        }

        //Handling commands and their operations
        Scanner sc = new Scanner(System.in);
        while (true) {
            String st = sc.nextLine();
            String[] input = st.split(" ");

            /*
                Create
             */
            if ("create".equals(input[0])) {
                try {
                    String key = input[1];
                    StringBuilder value = new StringBuilder();
                    value.append(input[2]);
                    if (db.keyMayExist(key.getBytes(), 0, key.length(), value)) {
                        System.out.println("false" + "\n");
                    } else {
                        db.put(key.getBytes(), input[2].getBytes());
                        System.out.println("true" + "\n");
                    }
                } catch (ArrayIndexOutOfBoundsException a) {
                    System.out.println("Invalid format");
                }

            }

            /*
                Fetch
             */
            else if ("fetch".equals(input[0])) {
                try {
                    String key = input[1];
                    StringBuilder value = new StringBuilder();
                    if (!db.keyMayExist(key.getBytes(), 0, key.length(), value)) {
                        System.out.println("false" + "\n");
                    } else {
                        System.out.println("true");
                        System.out.println(StandardCharsets.UTF_8.decode(ByteBuffer.wrap(db.get(key.getBytes()))) + "\n");
                    }
                } catch (ArrayIndexOutOfBoundsException a) {
                    System.out.println("Invalid format");
                }
            }

            /*
                Update
             */
            else if ("update".equals(input[0])) {
                try {
                    String key = input[1];
                    StringBuilder value = new StringBuilder();
                    value.append(input[2]);
                    if (!db.keyMayExist(key.getBytes(), 0, key.length(), value)) {
                        System.out.println("false" + "\n");
                    } else if (db.keyMayExist(key.getBytes(), 0, key.length(), value)) {
                        System.out.println("true" + "\n");
                        db.delete(key.getBytes());
                        db.put(key.getBytes(), input[2].getBytes());
                    }
                } catch (ArrayIndexOutOfBoundsException a) {
                    System.out.println("Invalid format");

                }
            }

            /*
                Delete
             */
            else if ("delete".equals(input[0])) {
                try {
                    String key = input[1];
                    StringBuilder value = new StringBuilder();
                    if (!db.keyMayExist(key.getBytes(), 0, key.length(), value)) {
                        System.out.println("false" + "\n");
                    } else if (db.keyMayExist(key.getBytes(), 0, key.length(), value)) {
                        System.out.println("true" + "\n");
                        db.delete(key.getBytes());
                    }
                } catch (ArrayIndexOutOfBoundsException a) {
                    System.out.println("Invalid format");

                }
            }
        }
    }


}
