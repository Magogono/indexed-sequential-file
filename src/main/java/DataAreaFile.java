import java.io.*;
import java.nio.ByteBuffer;


public class DataAreaFile {
    public static final int MAX_NUM_OF_REC = 4;
    private static double ALFA_FACTOR = 0.5;
    private final int RECORD_SIZE = Record.RECORD_SIZE;
    private final int PAGE_SIZE = MAX_NUM_OF_REC * RECORD_SIZE;
    private File primaryAreaFile;
    private File overflowAreaFile;
    public PageBuffer primaryAreaBuffer;
    public PageBuffer overflowAreaBuffer;

    private int PANumOfRec; // number of records in the main area
    private int OVANumOfRec; // number of records in the overflow area
    private int deletedNumOfRec;
    private int maxOVANumOfRec;
    public int reads;
    public int writes;

    private class PageBuffer {
        private Record[] records;
        private int numOfRecords;
        private int indOfFilePage;
        private File file;

        private PageBuffer(File file) {
            records = new Record[MAX_NUM_OF_REC];
            for(int i = 0; i < MAX_NUM_OF_REC; i++) {
                records[i] = new Record();
            }

            numOfRecords = 0;
            indOfFilePage = -1;
            this.file = file;
        }

        // Strony numerowane są od 0.
        // @return true - udało się odczytać stronę o podanym indeksie (argument @pageInd)
        // or @return false - w pliku nie ma takiej strony lub błąd przy odczycie.
        private boolean loadPage(int pageInd) {
            // Odczytujemy stronę tylko jeśli nie znajduje się w pamięci i zwiększamy licznik reads.
            if(indOfFilePage != pageInd) {
                // Bufor na wczytanie strony.
                byte[] buffer = new byte[PAGE_SIZE];
                int read = 0;

                // Czytanie strony ze strumienia pliku.
                try (FileInputStream fis = new FileInputStream(file)) {
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

                // Sprawdzanie, czy odczytano pełną stronę.
                // Jeśli tak, to wczytujemy rekordy z pliku do odpowiednich pól obiektu PageBuffer.
                // if(read != -1)
                if (read == PAGE_SIZE) {
                    int recInd = 0;
                    numOfRecords = 0;
                    ByteBuffer bb = ByteBuffer.wrap(buffer);

                    // Odczytywanie kolejnych indeksów z wczytanego strumienia bajtów.
                    while (read >= RECORD_SIZE) {
                        records[recInd].setKey(bb.getInt());
                        records[recInd].setA(bb.getDouble(), bb.getDouble());
                        records[recInd].setB(bb.getDouble(), bb.getDouble());
                        records[recInd].setC(bb.getDouble(), bb.getDouble());
                        records[recInd].setPointerToNextRecord(bb.getInt(), bb.getInt());
                        if(bb.getInt() == 1)
                            records[recInd].setDeletion(true);
                        else
                            records[recInd].setDeletion(false);

                        // Zwiększ numOfRecords, jeśli to nie pusty rekord, o key = -1
                        if (records[recInd].getKey() != -1)
                            numOfRecords++;

                        read -= RECORD_SIZE;
                        recInd++;
                    }

                    // Jeśli odczytano stronę zapełnioną rekordami, to numOfRecords = MAX_NUM_OF_REC.

                    // Zapamiętujemy informację o indeksie odczytanej strony.
                    indOfFilePage = pageInd;

                    // Zwiększenie licznika operacji.
                    reads++;

                    return true;
                }

                // Nie udało się odczytać danej strony.
                // numOfIndexes = 0;
                // indOfPageFile = -1;
                // Bufor pozostaje bez zmian - jest w nim wczytana ostatnio używana strona w pliku.
                return false;
            }
            else return true;
        }

        private boolean loadLastWritablePage() {
            // Załaduj do bufora ostatnią stronę wypełnioną rekordami z overflow area.
            int overflowPageInd = 0;
            boolean isEmptyPlace = false;

            while(this.loadPage(overflowPageInd)) {
                if(this.numOfRecords < MAX_NUM_OF_REC) {
                    isEmptyPlace = true;
                }
                overflowPageInd++;
                if(isEmptyPlace) break;
            }

            if(overflowPageInd == 0) {
                // Nie załadowano żadnej strony
                System.out.println("Failed to load overflow area file");
                return false;
            }
            else if(!isEmptyPlace) {
                // Overflow area jest zapełnione.
                System.out.println("Cannot insert record into overflow area (may be full)");
                return false;
            }
            else {
                // Załadowano do bufora ostatnią wypełnianą stronę z overflowarea i jest tam wolne miejsce.
                return true;
            }
        }

        private boolean overwritePage() {
            // Bufor na zapisanie strony.
            byte[] buffer = new byte[PAGE_SIZE];
            ByteBuffer bb = ByteBuffer.wrap(buffer);

            for(int recInd = 0; recInd < MAX_NUM_OF_REC; recInd++) {
                bb.put(records[recInd].convertToBytes());
            }

            // Nadpisanie odpowiedniej strony w pliku danymi z bufora.
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw"))
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

    public DataAreaFile(String filepath) {
        primaryAreaFile = new File(filepath + "_primary");
        overflowAreaFile = new File(filepath + "_overflow");
        PANumOfRec = 0;
        OVANumOfRec = 0;
        deletedNumOfRec = 0;

        // Jeśli plik istnieje, to wczytujemy go,
        // a w przeciwnym wypadku tworzymy nowy plik.
        try {
            primaryAreaFile.createNewFile();
            overflowAreaFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        primaryAreaBuffer = new PageBuffer(primaryAreaFile);
        overflowAreaBuffer = new PageBuffer(overflowAreaFile);
    }

    // Zwraca true, jeśli uda się wstawić rekord do primary area lub overflow area.
    // Jeśli overwritePA, to od razu nadpisuje stronę w pliku PA przy wstawianiu tam rekordu.
    // Overflow area jest nadpisywane automatycznie przy wstawianiu tam rekordu.
    public boolean insertRecord(Record record, int pageInd, boolean overwritePA) {
        // Jeśli trzeba, ładujemy odpowiednią stronę w primary area.
        if (!primaryAreaBuffer.loadPage(pageInd)) {
            System.out.println("Failed to load page from primary area");
            return false;
        }

        int lastRecInd = primaryAreaBuffer.numOfRecords - 1;

        // Sprawdzamy, czy rekord o danym kluczu nie istnieje już na stronie.
        // Pomijamy rekordy usunięte.
        for(int i = 0; i <= lastRecInd; i++) {
            if(primaryAreaBuffer.records[i].getKey() == record.getKey()
                && !primaryAreaBuffer.records[i].isDeleted()) {
                System.out.println("Record with given key " + record.getKey() + " already exists in file");
                return false;
            }
        }

        // Wstawiamy do primary area.
        if(lastRecInd == -1
            || (primaryAreaBuffer.records[lastRecInd].getKey() < record.getKey()
                && lastRecInd < MAX_NUM_OF_REC - 1)) {
            // Wstawiamy jako ostatni rekord na wczytaną stronę.
            primaryAreaBuffer.records[lastRecInd+1].copyFrom(record);
            primaryAreaBuffer.numOfRecords++;
            if(overwritePA) primaryAreaBuffer.overwritePage();

            // Zwiększamy licznik.
            PANumOfRec++;
        }
        // Wstawiamy rekord do overflow area.
        else {
            boolean isInserted = false;

            // Szukamy ostatniego mniejszego klucza (rekordu) w primarybuffer - głowy listy odwołań.
            int headRecInd = -1;
            for (int i = 0; i <= lastRecInd; i++) {
                if (primaryAreaBuffer.records[i].getKey() > record.getKey()) {
                    headRecInd = i - 1;
                    break;
                }
            }
            if (headRecInd == -1) headRecInd = lastRecInd;

            // currElem.key zawsze jest mniejszy od (<) record.key
            Record currElem = primaryAreaBuffer.records[headRecInd];
            int pagePointerPrev, recPointerPrev,
                    pagePointerCurr = primaryAreaBuffer.indOfFilePage, recPointerCurr = headRecInd;

            // Sprawdzamy overflow pointer w primary area - jeśli pusty, to aktualizujemy go
            // i wstawiamy rekord do części overflow.
            if (currElem.getPointerToPage() == -1) {

                // Wstaw nowy rekord na koniec overflow i uaktualnij currRec.
                if(insertIntoOverflowArea(record, primaryAreaBuffer, pagePointerCurr, recPointerCurr))
                    isInserted = true;
                else
                    return false;
            }
            // Jeśli pełny i i wstawiamy rekord od razu jako następny element.
            else {
                // Załaduj następny element - pierwszy w łańcuchu, który pochodzi z overflow area.
                pagePointerPrev = pagePointerCurr;
                recPointerPrev = recPointerCurr;
                pagePointerCurr = currElem.getPointerToPage();
                recPointerCurr = currElem.getPointerToRecord();
                if (!overflowAreaBuffer.loadPage(pagePointerCurr)) return false;
                currElem = overflowAreaBuffer.records[recPointerCurr];

                // Wstaw rekord między prevElem a currElem.
                if (currElem.getKey() > record.getKey()) {
                    // Wstawiany rekord niech wskazuje na ten currElem (o większym kluczu).
                    record.setPointerToNextRecord(pagePointerCurr, recPointerCurr);

                    // Wstaw nowy rekord na koniec overflow i uaktualnij prevElem.
                    if(insertIntoOverflowArea(record, primaryAreaBuffer, pagePointerPrev, recPointerPrev))
                        isInserted = true;
                    else
                        return false;
                }
                // Błąd.
                else if (currElem.getKey() == record.getKey()
                        && !currElem.isDeleted()) {
                    System.out.println("Record with given key already exists in overflow file");
                    return false;
                }
            }

            // W innym przypadku chodzimy w overflow area po łańcuchu odwołań
            // i wstawiamy rekord w odpowiednie miejsce.

            while (!isInserted) {
                // currElem.key zawsze jest mniejszy od (<) record.key

                // Jeśli jest to ostatni element łańcucha, po prostu wstawiamy rekord na koniec.
                if (currElem.getPointerToPage() == -1) {
                    // Wstawiany rekord ma pusty wskaźnik.
                    record.setPointerToNextRecord(-1, -1);

                    // Wstaw nowy rekord na koniec overflow i uaktualnij prevElem.
                    if(insertIntoOverflowArea(record, overflowAreaBuffer, pagePointerCurr, recPointerCurr))
                        isInserted = true;
                    else
                        return false;
                }
                // Jeśli jest, to załaduj następny element (na który wskazuje curr pointer)
                // i jeśli jest większy, to spróbuj wstawić pomiędzy prevElem a currElem.
                // W przeciwnym przypadku nic nie rób (czekaj na przejście do jeszcze następnego elementu).
                else {
                    pagePointerPrev = pagePointerCurr;
                    recPointerPrev = recPointerCurr;
                    pagePointerCurr = currElem.getPointerToPage();
                    recPointerCurr = currElem.getPointerToRecord();
                    if (!overflowAreaBuffer.loadPage(pagePointerCurr)) return false;
                    currElem = overflowAreaBuffer.records[recPointerCurr];

                    if (currElem.getKey() > record.getKey()) {
                        // Wstawiany rekord niech wskazuje na ten currElem (o większym kluczu).
                        record.setPointerToNextRecord(pagePointerCurr, recPointerCurr);

                        // Wstaw nowy rekord na koniec overflow i uaktualnij prevElem.
                        if(insertIntoOverflowArea(record, overflowAreaBuffer, pagePointerPrev, recPointerPrev))
                            isInserted = true;
                        else
                            return false;
                    }
                    // Niedozwolona operacja, klucz się powtarza.
                    else if (currElem.getKey() == record.getKey()
                            && !currElem.isDeleted()) {
                        System.out.println("Record with given key already exists in overflow file");
                        return false;
                    }
                }
            }

            if(isInserted)
                // Zwiększamy licznik.
                OVANumOfRec++;
        }

        return true;
    }

    // Wstawia rekord jako kolejny w łańcuchu, po rekordzie w pliku powiązanym z prevRecPageBuffer,
    // który znajduje się na stronie prevPageInd i prevRecInd.
    // Aktualizuje pointer poprzedniego rekordu.
    private boolean insertIntoOverflowArea(Record record, PageBuffer prevRecPageBuffer, int prevPageInd, int prevRecInd) {
        int newRecInd = -1, newRecPageInd = -1;

        // Załaduj do bufora ostatnią stronę overflow area.
        if (overflowAreaBuffer.loadLastWritablePage()) {
            // Można wstawić rekord, jest wolne miejsce.
            newRecInd = overflowAreaBuffer.numOfRecords;
            newRecPageInd = overflowAreaBuffer.indOfFilePage;

            // Umieść rekord w overflow area.
            overflowAreaBuffer.records[newRecInd].copyFrom(record);
            overflowAreaBuffer.numOfRecords++;
            overflowAreaBuffer.overwritePage();
        } else {
            // Brak pustego miejsca lub problem z załadowaiem strony z pliku overflow area.
            return false;
        }

        // Uaktualnij pointer w poprzednim rekordzie i nadpisz plik.
        prevRecPageBuffer.loadPage(prevPageInd);
        prevRecPageBuffer.records[prevRecInd].setPointerToNextRecord(newRecPageInd, newRecInd);
        prevRecPageBuffer.overwritePage();
        return true;
    }

    // Odczytuje rekord o danym indeksie na wybranej stronie w primary area.
    public Record readRecordFromPA(int pageInd, int recordInd) {
        if(primaryAreaBuffer.loadPage(pageInd)
            && recordInd < primaryAreaBuffer.numOfRecords)
            return primaryAreaBuffer.records[recordInd];
        else
            return null;
    }

    // Odczytuje rekord o danym indeksie na wybranej stronie w overflow area.
    public Record readRecordFromOVA(int pageInd, int recordInd) {
        if(overflowAreaBuffer.loadPage(pageInd)
                && recordInd < overflowAreaBuffer.numOfRecords)
            return overflowAreaBuffer.records[recordInd];
        else
            return null;
    }

    public Record readRecord(int pageInd, int key) {
        Record foundRec = null;

        // Ładujemy odpowiednią stronę w PA.
        if(!primaryAreaBuffer.loadPage(pageInd)) return null;

        // Szukamy na stronie primary area.
        int ind;
        for (ind = 0; ind < primaryAreaBuffer.numOfRecords; ind++) {
            if(primaryAreaBuffer.records[ind].getKey() >= key)
                break;
        }

        // Jeśli żaden rekord nie jest >= key
        if(ind == primaryAreaBuffer.numOfRecords) return null;

        // Znaleziono rekord w primary area.
        if(primaryAreaBuffer.records[ind].getKey() == key
            &&!primaryAreaBuffer.records[ind].isDeleted()) {
            foundRec = primaryAreaBuffer.records[ind];
        }
        // Wszystkie rekordy na stronie są większe niż szukany
        // i na stronie jako pierwszy nie jest usunięty rekord o szukanym kluczu.
        else if(ind == 0
                && primaryAreaBuffer.records[ind].getKey() != key) {
            foundRec = null;
        }
        // Szukaj w overflow area.
        else {
            // Weź ostatni mniejszy rekord - potrzebny do czytania łańcucha OV.
            // Jeśli na stronie PA jest usunięty rekord o szukanym kluczu, to jest pierwszym elementem.
            // Jeśli nie, to głową jest poprzedni element, bo @ind wskazuje obecnie na pierwszy większy klucz.
            if(primaryAreaBuffer.records[ind].getKey() == key
                && primaryAreaBuffer.records[ind].isDeleted())
                ind++;
            Record currElem = primaryAreaBuffer.records[ind - 1];

            int pagePointer, recPointer;

            boolean endOfSearching = false;
            while(!endOfSearching) {
                // Załaduj następny rekord
                pagePointer = currElem.getPointerToPage();
                recPointer = currElem.getPointerToRecord();
                if(pagePointer != -1) {
                    if (!overflowAreaBuffer.loadPage(pagePointer)) {
                        foundRec = null;
                        endOfSearching = true;
                        break;
                    }
                    currElem = overflowAreaBuffer.records[recPointer];

                    // Nie znaleziono rekordu.
                    if (currElem.getKey() > key) {
                        endOfSearching = true;
                        foundRec = null;
                    } else if (currElem.getKey() == key
                            && !currElem.isDeleted()) {
                        endOfSearching = true;
                        foundRec = currElem;
                    }
                }
                else {
                    endOfSearching = true;
                    foundRec = null;
                }
            }
        }

        return foundRec;
    }

    // Przeszukuje strony w buforach po wywołaniu funkcji readRecord,
    // aktualizuje znaleziony na podstawie klucza rekord i nadpisuje stronę.
    // Zwraca true, jeśli zaktualizowano rekord i false, jeśli nie ma szukanego klucza w pliku.
    public boolean updateRecord(int pageInd, Record record, boolean deleteToo) {
        if(this.readRecord(pageInd, record.getKey()) != null) {
            PageBuffer buffer = null;
            int ind;

            for (ind = 0; ind < primaryAreaBuffer.numOfRecords; ind++) {
                if (primaryAreaBuffer.records[ind].getKey() == record.getKey()
                    && !primaryAreaBuffer.records[ind].isDeleted()) {
                    buffer = primaryAreaBuffer;
                    break;
                }
            }
            if (buffer == null) {
                for (ind = 0; ind < overflowAreaBuffer.numOfRecords; ind++) {
                    if (overflowAreaBuffer.records[ind].getKey() == record.getKey()
                        && !overflowAreaBuffer.records[ind].isDeleted()) {
                        buffer = overflowAreaBuffer;
                        break;
                    }
                }
            }

            int pagePtr = buffer.records[ind].getPointerToPage(), recPtr = buffer.records[ind].getPointerToRecord();
            record.setPointerToNextRecord(pagePtr, recPtr);
            buffer.records[ind].copyFrom(record);
            // chyba niepotrzebne?? :
            // record.setPointerToNextRecord(-1, -1);
            buffer.overwritePage();

            // Zwiększ licznik usuniętych rekordów.
            if(deleteToo)
                deletedNumOfRec++;

            return true;
        }
        else return false;
    }

    public boolean isOverflowFull() { return OVANumOfRec >= maxOVANumOfRec; }

    // Faktyczna wstawionych liczba rekordów w PA (łącznie z usuniętymi).
    public int getNumOfRecPA() { return PANumOfRec; }

    // Faktyczna wstawionych liczba rekordów w OVA (łącznie z usuniętymi).
    public int getNumOfRecOVA() { return OVANumOfRec; }

    public int getNumOfDeleted() { return deletedNumOfRec; }

    public int getNumOfBufferRec(boolean fromPA) {
        if(fromPA)
            return primaryAreaBuffer.numOfRecords;
        else
            return overflowAreaBuffer.numOfRecords;
    }

    public void setOverflowMax(int max) { maxOVANumOfRec = max;    }

    public void printFile() {
        System.out.println("----- Data file - primary area ------");
        int pageInd = 0;
        String printedKey, deleted;
        while (primaryAreaBuffer.loadPage(pageInd++)) {
            for (int i = 0; i < MAX_NUM_OF_REC; i++) {
                Record rec = primaryAreaBuffer.records[i];

                if(rec.getKey() == -1) printedKey = "-";
                else printedKey = Integer.toString(rec.getKey());

                if(rec.isDeleted()) deleted = " del";
                else deleted = "";

                if (rec.getPointerToPage() == -1)
                    System.out.println(pageInd - 1 + ":" + i +
                            " K: " + printedKey +
                            " OV ptr: -:-" +
                            deleted);
                else
                    System.out.println(pageInd-1 + ":" + i +
                            " K: " + printedKey +
                            " OV ptr: " + rec.getPointerToPage() + ":" + rec.getPointerToRecord() +
                            deleted);
            }
        }
        System.out.println("-------------------------------------");

        System.out.println("----- Data file - overflow area -----");
        pageInd = 0;
        while (overflowAreaBuffer.loadPage(pageInd++)) {
            for (int i = 0; i < MAX_NUM_OF_REC; i++) {
                Record rec = overflowAreaBuffer.records[i];

                if(rec.getKey() == -1) printedKey = "-";
                else printedKey = Integer.toString(rec.getKey());

                if(rec.isDeleted()) deleted = " del";
                else deleted = "";

                if (rec.getPointerToPage() == -1)
                    System.out.println(pageInd - 1 + ":" + i +
                            " K: " + printedKey +
                            " OV ptr: -:-" +
                            deleted);
                else
                    System.out.println(pageInd-1 + ":" + i +
                            " K: " + printedKey +
                            " OV ptr: " + rec.getPointerToPage() + ":" + rec.getPointerToRecord() +
                            deleted);
            }
        }
        System.out.println("-------------------------------------");
    }

    // Wypisuje faktyczną liczbę rekordów w pliku (łącznie z usuniętymi).
    public void printNumOfRec() {
        System.out.println("===== Number of records =============");
        System.out.println("N: " + PANumOfRec + "       V: " + OVANumOfRec +
                "       deleted: " + deletedNumOfRec);
    }

    public void flushPABuffer() { primaryAreaBuffer.overwritePage(); }

    public void rename(String path) {
        File file = new File(path + "_primary");
        primaryAreaFile.renameTo(file);
        primaryAreaFile = file;
        primaryAreaBuffer.file = file;
        file = new File(path + "_overflow");
        overflowAreaFile.renameTo(file);
        overflowAreaFile = file;
        overflowAreaBuffer.file = file;
    }

    // Dopisuje liczbę pustych stron @size do pliku z primary area lub overflow area.
    public void addEmptyPages(int size, boolean primaryArea) {
        File file;
        if(primaryArea) file = primaryAreaFile;
        else file = overflowAreaFile;

        byte[] buffer = new byte[PAGE_SIZE];
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        byte[] rec = (new Record()).convertToBytes();

        for(int recInd = 0; recInd < MAX_NUM_OF_REC; recInd++) {
            bb.position(recInd*RECORD_SIZE);
            bb.put(rec);
        }

        try (FileOutputStream fos = new FileOutputStream(file, true))
        {
            for(int i = 0; i < size; i++) {
                fos.write(buffer);
                fos.flush();
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

    public void clearFiles() {
        try {
            new FileOutputStream(primaryAreaFile).close();
        } catch (IOException e) {
            System.out.println("Failed to clear file: " + primaryAreaFile.getName());
            e.printStackTrace();
        }
        try {
            new FileOutputStream(overflowAreaFile).close();
        } catch (IOException e) {
            System.out.println("Failed to clear file: " + overflowAreaFile.getName());
            e.printStackTrace();
        }
        PANumOfRec = 0; // number of records in the main area
        OVANumOfRec = 0; // number of records in the overflow area
        deletedNumOfRec = 0;
        maxOVANumOfRec = 0;
    }

    public void deleteFiles() {
        if(!primaryAreaFile.delete()) {
            System.out.println("Failed to delete file");
        }
        if(!overflowAreaFile.delete())
            System.out.println("Failed to delete file");
    }
}
