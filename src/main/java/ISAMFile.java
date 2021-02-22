/*
 Temat:
          49. Rekordy pliku: 3 współrzędne w układzie kartezjańskim.
          Uporządkowanie wg pola trójkąta tworzonego przez te współrzędne.
...............................................................................
wartość -1 to brak wartości dla klucza lub wskaźnika na stronę/rekord
wartość Integer.MIN_VALUE (=0x80000000) to wartość sztucznie wstawionego
            klucza jako pierwszego wpisu w indeksie i primary area
 */
import java.io.*;
import java.nio.ByteBuffer;


public class ISAMFile {
    private static double ALFA_FACTOR = 0.5;
    private static double REORGANIZATION_FACTOR = 0.5; // reorganizacja, kiedy rekordóww OV lub Del jest więcej niż x% poprawnych
    private static final int MAX_NUM_OF_IND = 4;
    private static final int INDEX_SIZE = 2 * Integer.BYTES;
    private static final int PAGE_SIZE = MAX_NUM_OF_IND * INDEX_SIZE;
    private File indexFile;
    private DataAreaFile dataAreaFile;
    private PageBuffer indexBuffer;
    private int reads;
    private int writes;

    private class PageBuffer {
        private int[] keyValues;
        private int[] pageIndexes;
        private int numOfIndexes;
        private int indOfFilePage;

        private PageBuffer() {
            keyValues = new int[MAX_NUM_OF_IND];
            pageIndexes = new int[MAX_NUM_OF_IND];

            numOfIndexes = 0;
            indOfFilePage = -1;
        }

        // Strony numerowane są od 0.
        // Odczytuje wypełnione i niewypełnione indeksy - o kluczu naturalnym albo -1
        // i odpowiednim numerze strony (od 0).
        // Indeksy "nieistniejące", czyli takie o nr. strony -1 nie są liczone jako odczytane.
        // @return true - udało się odczytać stronę o podanym indeksie (argument @pageInd)
        // or @return false - w pliku nie ma takiej strony, w buforze jest poprzednio załadowana strona.
        private boolean loadPage(int pageInd) {
            // Odczytujemy stronę tylko jeśli nie znajduje się w pamięci i zwiększamy licznik reads.
            if(indOfFilePage != pageInd) {

                // Bufor na wczytanie strony.
                byte[] buffer = new byte[PAGE_SIZE];
                int read = 0;

                // Czytanie strony ze strumienia pliku.
                try (FileInputStream fis = new FileInputStream(indexFile)) {
                    // Pomiń poprzednie strony i odczytaj bajty na wybranej stronie.
                    fis.skip(pageInd * PAGE_SIZE);
                    read = fis.read(buffer);
                } catch (FileNotFoundException e) {
                    System.out.println("Cannot Open the Input File");
                    return false;
                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }

                // Sprawdzanie, czy odczytywana część była niepusta.
                // Jeśli tak, to wczytujemy indeksy z pliku do pól odpowiednich obiektu PageBuffer.
                if (read != -1) {
                    int indNum = 0;
                    numOfIndexes = 0;
                    ByteBuffer bb = ByteBuffer.wrap(buffer);

                    // Odczytywanie kolejnych indeksów z wczytanego strumienia bajtów.
                    while (read >= INDEX_SIZE) {
                        keyValues[indNum] = bb.getInt();
                        pageIndexes[indNum] = bb.getInt();

                        // Zwiększ numOfIndexes, jeśli to nie pusty indeks (dopełnienie strony), o pageInd = -1
                        if (pageIndexes[indNum] != -1)
                            numOfIndexes++;
                        read -= INDEX_SIZE;
                        indNum++;
                    }

                    // jeśli odczytano pełną stronę, to numOfIndexes = MAX_NUM_OF_IND
                    // jeśli nie odczytano pełnej strony, to numOfIndexes < MAX_NUM_OF_IND

                    // Zapamiętujemy informację o indeksie odczytanej strony.
                    indOfFilePage = pageInd;

                    // Zwiększenie licznika operacji.
                    reads++;

                    return true;
                }

                // Nie udało się odczytać danej strony - koniec pliku
                // numOfIndexes = 0;
                // indOfPageFile = -1;
                // Bufor pozostaje bez zmian - jest w nim wczytana ostatnio używana strona w pliku.
                return false;
            }
            else return true;
        }

        private boolean overwritePage() {
            // Bufor na zapisanie strony.
            byte[] buffer = new byte[PAGE_SIZE];
            ByteBuffer bb = ByteBuffer.wrap(buffer);

            for(int indInd = 0; indInd < MAX_NUM_OF_IND; indInd++) {
                bb.putInt(keyValues[indInd]);
                bb.putInt(pageIndexes[indInd]);
            }

            // Nadpisanie odpowiedniej strony w pliku danymi z bufora.
            try (RandomAccessFile raf = new RandomAccessFile(indexFile, "rw"))
            {
                int offset = indOfFilePage * PAGE_SIZE;
                raf.seek(offset);
                raf.write(buffer);
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }

            // Zwiększenie licznika operacji.
            writes++;

            return true;
        }
    }


    public ISAMFile(String filepath) {
        indexFile = new File(filepath);
        this.dataAreaFile = new DataAreaFile(filepath);

        // Jeśli plik istnieje, to wczytujemy go jako taśmę,
        // a w przeciwnym wypadku tworzymy nowy plik na zapis taśmy.
        try {
            indexFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        indexBuffer = new PageBuffer();
    }


    // Inicjalizuje pliki danych i indeks. Rozmiar to liczba stron.
    public void initializeFiles(int primarySize, int overflowSize) {
        dataAreaFile.addEmptyPages(primarySize, true);
        dataAreaFile.addEmptyPages(overflowSize, false);
        dataAreaFile.setOverflowMax(overflowSize * DataAreaFile.MAX_NUM_OF_REC);
        this.addEmptyIndexes(primarySize);

        // Dodaj pierwszy rekord o najmniejszej wartości
        Record record = new Record();
        record.setKey(Integer.MIN_VALUE);
        indexBuffer.loadPage(0);
        indexBuffer.keyValues[0] = Integer.MIN_VALUE;
        indexBuffer.overwritePage();
        dataAreaFile.insertRecord(record, 0, true);
    }

    public void clearFiles() {
        try {
            new FileOutputStream(indexFile).close();
        } catch (IOException e) {
            System.out.println("Failed to clear file: " + indexFile.getName());
            e.printStackTrace();
        }
        dataAreaFile.clearFiles();
    }

    // Dodaje size niewypełnionych indeksów do pliku - o kluczu -1 i odpowiednim numerze strony (od 0).
    // Jeśli liczba nie jest wielokrotnością rozm. strony,
    // to pozostałe miejsca wypełnia "nieistniejącymi" indeksami - o kluczu -1 i num. strony -1.
    private void addEmptyIndexes(int size) {
        byte[] buffer = new byte[PAGE_SIZE];
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        int pageInd = 0;

        try (FileOutputStream fos = new FileOutputStream(indexFile, true))
        {
            while(size > 0) {
                bb.position(0);
                int numOfInd = size >= MAX_NUM_OF_IND ? MAX_NUM_OF_IND : size;
                for (int i = 0; i < numOfInd; i++) {
                    bb.putInt(-1);          // klucz
                    bb.putInt(pageInd++);      // indeks strony
                }

                // dodaj puste indeksy
                for(int i = numOfInd; i < MAX_NUM_OF_IND; i++) {
                    bb.putInt(-1);          // klucz
                    bb.putInt(-1);      // indeks strony
                }

                fos.write(buffer, 0, PAGE_SIZE);
                fos.flush();

                size = size - numOfInd;
            }
        }
        catch(FileNotFoundException e)
        {
            System.out.println("Cannot open the output file");
            return;
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
    }

    // Zapis, odczyt
    public void insertRecord(Record record) {
        System.out.println("===== INSERTING =====================");
        reads = 0;
        writes = 0;

        int N = dataAreaFile.getNumOfRecPA(), V = dataAreaFile.getNumOfRecOVA(), D = dataAreaFile.getNumOfDeleted();
        double coef = 0.0;
        if (N > 0) coef = V > D ? V/(double)N : D/(double)N;

        if(coef > REORGANIZATION_FACTOR
            || dataAreaFile.isOverflowFull())
            this.reorganize();

        dataAreaFile.reads = 0;
        dataAreaFile.writes = 0;

        int key = record.getKey();
        int dataPageInd = this.getPageFor(key, false);
        if(dataAreaFile.insertRecord(record, dataPageInd, true))
            System.out.println("Successfully inserted record with key: " + key);
        else
            System.out.println("Failed to insert record with key " + key);

        reads += dataAreaFile.reads;
        writes += dataAreaFile.writes;
    }

    // Zwraca indeks strony, na której powinien być umieszczony rekord o danym kluczu.
    // Jeśli w primary area są całkowicie puste strony, to w przypadku zwrócenia takiej pustej strony,
    // uaktualnia też dla niej indeks (wpisuje wartość klucza i nadpisuje plik indeksu).
    // Argument reading decyduje o numerze strony w przypadku, gdy w primary area istnieją puste strony,
    // jeśli tak to zwraca numer strony do zapisu - może być pusta.
    private int getPageFor(int key, boolean reading) {
        boolean isIndexFound = false;
        int dataPageInd = -1;

        int indPageInd = 0;
        while(indexBuffer.loadPage(indPageInd)) {
            for(int i = 0; i < indexBuffer.numOfIndexes; i++) {
                if(indexBuffer.keyValues[i] > key) {
                    dataPageInd = indexBuffer.pageIndexes[i] - 1;
                    isIndexFound = true;
                }
                // Jeśli primary area ma puste strony.
                else if(indexBuffer.keyValues[i] == -1) {
                    //Sprawdź ostatni rekord w poprzedniej stronie
                    dataPageInd = indexBuffer.pageIndexes[i] - 1;
                    Record rec = dataAreaFile.readRecordFromPA(dataPageInd, DataAreaFile.MAX_NUM_OF_REC - 1);

                    // Jeśli strona jest zapełniona i chcemy zapisać rekord, a nie tylko odczytać,
                    // zwróć następną stronę i zmień dla niej indeks.
                    if(!reading
                            && rec != null
                            && rec.getKey() != -1) {
                        dataPageInd++;
                        indexBuffer.keyValues[i] = key;
                        indexBuffer.overwritePage();
                    }
                    isIndexFound = true;
                }

                if(isIndexFound)
                    break;
            }

            if(isIndexFound)
                break;

            indPageInd++;
        }

        // Jeśli nie udało się indeksu dla danego klucza (warunek >),
        // to przyjmij ostatni indeks.
        if(indPageInd > 0 && !isIndexFound) {
            dataPageInd = indexBuffer.pageIndexes[indexBuffer.numOfIndexes - 1];
        }

        return dataPageInd;
    }

    // Zapis, odczyt
    public void readRecord(int key) {
        System.out.println("===== READING =======================");
        reads = 0;
        writes = 0;
        dataAreaFile.reads = 0;
        dataAreaFile.writes = 0;

        int dataPageInd = this.getPageFor(key, true);
        Record record = dataAreaFile.readRecord(dataPageInd, key);

        reads += dataAreaFile.reads;
        writes += dataAreaFile.writes;

        if(record != null)
            System.out.println("Found record:\n" + record.toString());
        else
            System.out.println("Failed to find - record with key " + key + " doesn't exist");
    }

    // Zapis, odczyt
    // Ustawia flagę usunięcia znalezionego rekordu na true.
    public void deleteRecord(int key) {
        System.out.println("===== DELETING ======================");
        reads = 0;
        writes = 0;
        dataAreaFile.reads = 0;
        dataAreaFile.writes = 0;

        int dataPageInd = this.getPageFor(key, true);
        Record record = new Record(key);
        record.setDeletion(true);

        // Aktualizuj rekord o danym kluczu z flagą usunięcia.
        if(dataAreaFile.updateRecord(dataPageInd, record, true))
            System.out.println("Successfully deleted record with key " + key);
        else
            System.out.println("Failed to delete - record with key " + key + " doesn't exist");

        reads += dataAreaFile.reads;
        writes += dataAreaFile.writes;
    }

    // Zapis, odczyt
    // Aktualizuje pola rekordu, który ma ten sam klucz co podany nowy rekord.
    public void updateRecord(Record record) {
        System.out.println("===== UPDATING ======================");
        reads = 0;
        writes = 0;
        dataAreaFile.reads = 0;
        dataAreaFile.writes = 0;

        int key = record.getKey();
        int dataPageInd = this.getPageFor(key, true);

        if(dataAreaFile.updateRecord(dataPageInd, record, false))
            System.out.println("Successfully updated record with key " + key);
        else
            System.out.println("Failed to update - record with key " + key + " doesn't exist");

        reads += dataAreaFile.reads;
        writes += dataAreaFile.writes;
    }

    // Zapis, odczyt
    public void reorganize() {
        System.out.println("===== REORGANIZATION ================");
        // Utworzenie nowych pustych plików.
        // Wyczyszczenie indeksu - nie będzie używany przy reorganizacji.
        try {
            new FileOutputStream(indexFile).close();
        } catch (IOException e) {
            System.out.println("Failed to create empty file " + indexFile.getPath());
            e.printStackTrace();
            return;
        }

        DataAreaFile newDataFile = new DataAreaFile(indexFile.getPath() + "_new");
        newDataFile.clearFiles();

        // Oblicz potrzebną ilość stron w nowych plikach.
        int N = dataAreaFile.getNumOfRecPA(), V = dataAreaFile.getNumOfRecOVA(), D = dataAreaFile.getNumOfDeleted();
        int alfaNum = (int) Math.ceil(ALFA_FACTOR * DataAreaFile.MAX_NUM_OF_REC);
        int primarySize = (int) Math.ceil((N + V - D) / (double)alfaNum);
        int overflowSize = (int) Math.ceil(0.2 * primarySize);

        // Zajmij miejsce na pliki.
        newDataFile.addEmptyPages(primarySize, true);
        newDataFile.addEmptyPages(overflowSize, false);
        newDataFile.setOverflowMax(overflowSize * DataAreaFile.MAX_NUM_OF_REC);
        this.addEmptyIndexes(primarySize);
        this.indexBuffer.indOfFilePage = -1;

        // Wyzeruj liczniki operacji.
        reads = 0;
        writes = 0;
        dataAreaFile.reads = 0;
        dataAreaFile.writes = 0;
        newDataFile.reads = 0;
        newDataFile.writes = 0;

        // Załaduj pierwszą stronę indeksu.
        indexBuffer.loadPage(0);

        // Pomijamy pierwszy rekord w PA - to ten sztucznie wstawiony, o specjalnym kluczu.
        int indexPage = 0, indexInd = 0,
                pageIndPAOld = 0, recIndPAOld = 0, pageIndPANew = 0, recIndPANew = 0;

        Record currRec = dataAreaFile.readRecordFromPA(pageIndPAOld, recIndPAOld);

        while(currRec != null) {
            int nextOVPageInd, nextOVRecInd;
            while (currRec != null) {
                // Zapamiętaj wskaźniki na następny rekord w OVA.
                nextOVPageInd = currRec.getPointerToPage();
                nextOVRecInd = currRec.getPointerToRecord();

                // Zapisz rekord w odpowiednim miejscu, tylko jeśli nie jest usunięty.
                // Zapisz indeks, jeśli trzeba.
                if(!currRec.isDeleted()) {
                    // Jeśli obecnie pisana strona newPA jest już zapełniona w stopniu ALFA,
                    // to zapisz nowy indeks i przejdź do kolejnej strony
                    if (recIndPANew == 0) {
                        // Zapisz indeks.
                        this.indexBuffer.keyValues[indexInd] = currRec.getKey();
                        indexInd++;

                        // Jeśli wypełniono wszystkie wpisy dostępne na załadowanej stronie indeksu,
                        // zapisz indeks do pamięci i przejdź do jego następnej strony.
                        if (indexInd == this.indexBuffer.numOfIndexes) {
                            this.indexBuffer.overwritePage();
                            indexPage++;
                            if (!this.indexBuffer.loadPage(indexPage)) {
                                System.out.println("End of index file while REORGANIZATING, may be natural");
                            }
                            indexInd = 0;
                        }
                    }

                    if (recIndPANew == alfaNum - 1) {
                        // Ostatni nadpisuje stronę - zapisz bufor PA w newDataFile i przejdź do kolejnej strony.
                        currRec.setPointerToNextRecord(-1, -1);
                        newDataFile.insertRecord(currRec, pageIndPANew, true);
                        pageIndPANew++;
                        recIndPANew = 0;
                    } else {
                        // Wstaw rekord bez zapisywania bufora PA do pliku.
                        currRec.setPointerToNextRecord(-1, -1);
                        newDataFile.insertRecord(currRec, pageIndPANew, false);
                        recIndPANew++;
                    }
                }

                // Odczytaj następny rekord ze dataAreaFile z OVA.
                if(nextOVPageInd != -1)
                    currRec = dataAreaFile.readRecordFromOVA(nextOVPageInd, nextOVRecInd);
                else
                    currRec = null;
            }

            // Odczytaj następny rekord ze dataAreaFile z PA.
            recIndPAOld++;
            if(recIndPAOld == dataAreaFile.getNumOfBufferRec(true)) {
                pageIndPAOld++;
                recIndPAOld = 0;
            }
            currRec = dataAreaFile.readRecordFromPA(pageIndPAOld, recIndPAOld);
        }
        if(recIndPANew != 0)
            newDataFile.flushPABuffer();

        // Ustaw liczniki operacji.
        reads += dataAreaFile.reads + newDataFile.reads;
        writes += dataAreaFile.writes + newDataFile.writes;

        // Usuń stare pliki dyskowe.
        dataAreaFile.deleteFiles();
        dataAreaFile = newDataFile;
        dataAreaFile.rename(this.indexFile.getPath());

        System.out.println("===== Files after reorganization ====");
        this.printIndex();
        this.printData();
        System.out.println("===== REORGANIZATION COMPLETED ======");
    }

    public void printData() { dataAreaFile.printFile(); }

    public void printIndex() {
        System.out.println("----- Index file --------------------");
        int pageInd = 0;
        while (indexBuffer.loadPage(pageInd++)) {
            for (int i = 0; i < indexBuffer.numOfIndexes; i++) {
                if (indexBuffer.pageIndexes[i] != -1)
                    System.out.println("K: " + indexBuffer.keyValues[i] +
                            " P: " + indexBuffer.pageIndexes[i]);
            }
        }
        System.out.println("-------------------------------------");
    }

    // Wyświetla wszystkie rekordy z primary area i overflow area łącznie
    // zgodnie z kolejnością wartości klucza.
    public void printAllSequentially() {
        System.out.println("===== Data file - all records =======");

        int pageIndPA = 0, recIndPA = 0;
        Record currRec = dataAreaFile.readRecordFromPA(pageIndPA, recIndPA);

        while(currRec != null) {
            // Odczytaj rekord i łańcuch z overflow area.
            int nextOVPageInd, nextOVRecInd;
            while (currRec != null) {
                if(!currRec.isDeleted())
                    System.out.println(currRec.toString());

                nextOVPageInd = currRec.getPointerToPage();
                nextOVRecInd = currRec.getPointerToRecord();
                if(nextOVPageInd != -1)
                    currRec = dataAreaFile.readRecordFromOVA(nextOVPageInd, nextOVRecInd);
                else
                    currRec = null;
            }

            // Przejdź do kolejnego rekordu w primaryArea
            recIndPA++;
            if(recIndPA == dataAreaFile.getNumOfBufferRec(true)) {
                pageIndPA++;
                recIndPA = 0;
            }

            currRec = dataAreaFile.readRecordFromPA(pageIndPA, recIndPA);
        }

        System.out.println("-------------------------------------");
    }

    public void printNumOfRec() { dataAreaFile.printNumOfRec(); }

    public void printNumOfOp() {
        System.out.println("Reads: " + reads + "       Writes: " + writes);
    }

    public int getReads() { return reads; }

    public int getWrites() { return writes; }
}
