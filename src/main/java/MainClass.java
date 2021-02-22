import java.io.*;
import java.nio.ByteBuffer;
import java.util.Scanner;


public class MainClass {

    public static void main(String[] args) throws IOException {
        // src\main\resources\isfile
        ISAMFile isFile = new ISAMFile("src\\main\\resources\\isfile");
        isFile.clearFiles();
        isFile.initializeFiles(3,1);

        boolean exit = false;
        Scanner scan = new Scanner(System.in);
        String input, filepath, errorStr = "";
        int key;
        double[] values;
        Record newRec;

        while (!exit) {
            System.out.println("Enter key of action:" +
                    "\n     q) exit" +
                    "\n     1) import instructions from file" +
                    "\n     2) show index" +
                    "\n     3) show data" +
                    "\n     4) read all records sequentially" +
                    "\n     5) insert full record" +
                    "\n     6) insert record with key" +
                    "\n     7) read record" +
                    "\n     8) update record" +
                    "\n     9) delete record" +
                    "\n     0) run reorganization");

            input = scan.next();

            switch (input) {
                case "q":
                    // exit
                    exit = true;
                    break;
                case "1":
                    // import instructions from file
                    System.out.print("Enter filepath    ");
                    filepath = scan.next();
                    isFile.clearFiles();
                    isFile.initializeFiles(3,1);

                    readInstructions(filepath, isFile);
                    break;
                case "2":
                    // show index
                    isFile.printIndex();
                    break;
                case "3":
                    // show data
                    isFile.printData();
                    break;
                case "4":
                    // read all records sequentially
                    isFile.printAllSequentially();
                    break;
                case "5":
                    // insert full record
                    System.out.print("Enter 6 decimals to create a record   ");
                    values = new double[6];
                    for(int i = 0; i < 6; i++) {
                        values[i] = Double.parseDouble(scan.next());
                    }

                    newRec = new Record();
                    newRec.setKey();
                    newRec.setA(values[0], values[1]);
                    newRec.setB(values[2], values[3]);
                    newRec.setC(values[4], values[5]);

                    isFile.insertRecord(newRec);
                    isFile.printNumOfOp();
                    break;
                case "6":
                    //insert record with key
                    System.out.print("Enter key of record (natural number without zero), other values will be default   ");
                    key = Integer.parseInt(scan.next());
                    newRec = new Record(key);

                    isFile.insertRecord(newRec);
                    isFile.printNumOfOp();
                    break;
                case "7":
                    // read record
                    //insert record with key
                    System.out.print("Enter key of record to read (natural number without zero)   ");
                    key = Integer.parseInt(scan.next());

                    isFile.readRecord(key);
                    isFile.printNumOfOp();
                    break;
                case "8":
                    // update record
                    System.out.print("Enter key of record to update (natural number without zero)...  ");
                    key = Integer.parseInt(scan.next());

                    System.out.print("... and 6 decimals as new values  ");
                    values = new double[6];
                    for(int i = 0; i < 6; i++) {
                        values[i] = Double.parseDouble(scan.next());
                    }

                    newRec = new Record(key);
                    newRec.setA(values[0], values[1]);
                    newRec.setB(values[2], values[3]);
                    newRec.setC(values[4], values[5]);

                    isFile.updateRecord(newRec);
                    isFile.printNumOfOp();
                    break;
                case "9":
                    // delete record
                    System.out.print("Enter key of record to delete (natural number without zero)   ");
                    key = Integer.parseInt(scan.next());

                    isFile.deleteRecord(key);
                    isFile.printNumOfOp();
                    break;
                case "0":
                    // run reorganization
                    isFile.reorganize();
                    isFile.printNumOfOp();
                    break;
                default:
                    errorStr = "Please enter proper key";
                    break;
            }

            scan.nextLine();
            System.out.println("----------------------------------------------------");
            System.out.println(errorStr);
            errorStr = "";
        }

        scan.close();
    }

    private static void readInstructions(String filepath, ISAMFile isFile) {
        File file = new File(filepath);
        System.out.print("Reading test file " + file.getName() + ", size in bytes " + file.length() + "\n");

        String input, errorStr = "";
        boolean error = false;
        try (Scanner scan = new Scanner(file))
        {
            int key;
            double[] values;
            Record newRec;
            int readNum = 0, insertNum = 0, deleteNum = 0, updateNum = 0,
                    readR = 0, readW = 0, insertR = 0, insertW = 0,
                    deleteR = 0, deleteW = 0, updateR = 0, updateW = 0;

            while (!error && scan.hasNext()) {
                //input = scan.nextLine();
                input = scan.next();

                switch (input) {
                    case "r":
                        // read
                        key = Integer.parseInt(scan.next());
                        isFile.readRecord(key);

                        readNum++;
                        readR += isFile.getReads();
                        readW += isFile.getWrites();
                        isFile.printNumOfOp();
                        break;
                    case "i":
                        // insert
                        key = Integer.parseInt(scan.next());
                        values = new double[6];
                        for(int i = 0; i < 6; i++) {
                            values[i] = Double.parseDouble(scan.next());
                        }
                        newRec = new Record(key);
                        newRec.setA(values[0], values[1]);
                        newRec.setB(values[2], values[3]);
                        newRec.setC(values[4], values[5]);
                        isFile.insertRecord(newRec);

                        insertNum++;
                        insertR += isFile.getReads();
                        insertW += isFile.getWrites();
                        isFile.printNumOfOp();
                        break;
                    case "ik":
                        // insert only by key
                        key = Integer.parseInt(scan.next());
                        newRec = new Record(key);
                        isFile.insertRecord(newRec);

                        insertNum++;
                        insertR += isFile.getReads();
                        insertW += isFile.getWrites();
                        isFile.printNumOfOp();
                        break;
                    case "d":
                        // delete
                        key = Integer.parseInt(scan.next());
                        isFile.deleteRecord(key);

                        deleteNum++;
                        deleteR += isFile.getReads();
                        deleteW += isFile.getWrites();
                        isFile.printNumOfOp();
                        break;
                    case "u":
                        // update
                        key = Integer.parseInt(scan.next());
                        values = new double[6];
                        for(int i = 0; i < 6; i++) {
                            values[i] = Double.parseDouble(scan.next());
                        }
                        newRec = new Record(key);
                        newRec.setA(values[0], values[1]);
                        newRec.setB(values[2], values[3]);
                        newRec.setC(values[4], values[5]);
                        isFile.updateRecord(newRec);

                        updateNum++;
                        updateR += isFile.getReads();
                        updateW += isFile.getWrites();
                        isFile.printNumOfOp();
                        break;
                    case "p":
                        // update
                        isFile.printIndex();
                        isFile.printData();
                        break;
                    default:
                        errorStr = "Error in reading test file";
                        error = true;
                        break;
                }

                if(errorStr != "") {
                    System.out.println("----------------------------------------------------");
                    System.out.println(errorStr);
                }
            }

            // Pisanie liczb dot. operacji.
            System.out.println("Read-num: " + readNum + " num of ops = " + readR + " reads, " + readW + " writes");
            System.out.println("Insert-num: " + insertNum + " num of ops = " + insertR + " reads, " + insertW + " writes");
            System.out.println("Delete-num: " + deleteNum + " num of ops = " + deleteR + " reads, " + deleteW + " writes");
            System.out.println("Update-num: " + updateNum + " num of ops = " + updateR + " reads, " + updateW + " writes");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}