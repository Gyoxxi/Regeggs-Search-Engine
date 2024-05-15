package cis5550.kvs;

import cis5550.tools.KeyEncoder;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static cis5550.webserver.Server.*;

public class Worker extends cis5550.generic.Worker {

    public static final ConcurrentMap<String, ConcurrentMap<String, Row>> tables = new ConcurrentHashMap<>();
    private static final byte[] LF = {10};
    private static final int DEFAULT_PAGE_SIZE = 10;
    public static int port;
    public static String path;
    public static String coordinatorUrl;

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("usage: cis5550.kvs.Worker <port> <path> <ip:port>");
            System.exit(1);
        }

        port = Integer.parseInt(args[0]);
        port(port);
        path = args[1];
        String workerId = getWorkerId();
        coordinatorUrl = args[2];

        startPingThread(port, workerId, coordinatorUrl);

        get("/", Worker::handleGetTable);
        get("/count/:T", Worker::handleGetRowCount);
        get("/data/:T", Worker::handleGetTableData);
        get("/data/:T/:R", Worker::handleGetRowData);
        get("/data/:T/:R/:C", Worker::handleGetData);
        get("/view/:T", Worker::handleGetView);

        put("/data/:T", Worker::handlePutTableData);
        put("/data/:T/:R/:C", Worker::handlePutData);
        put("/delete/:T", Worker::handlePutDelete);
        put("/rename/:T", Worker::handlePutRename);
    }

    private static String handlePutData(Request req, Response res) {
        String tableName = req.params("T");
        String rowKey = req.params("R");
        String colKey = req.params("C");
        byte[] data = req.bodyAsBytes();

        createTableIfNecessary(tableName);

        synchronized (tables) {
            Row row = getRow(tableName, rowKey);
            if (row == null) {
                row = new Row(rowKey);
            }
            row.put(colKey, data);
            putRow(tableName, row);
        }

        return "OK";
    }

    private static String handlePutTableData(Request req, Response res) throws Exception {
        String tableName = req.params("T");
        byte[] data = req.bodyAsBytes();

        createTableIfNecessary(tableName);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
        while (true) {
            Row row = Row.readFrom(inputStream);
            if (row == null) {
                return "OK";
            }
            putRow(tableName, row);
        }
    }

    private static String handleGetData(Request req, Response res) {
        String tableName = req.params("T");
        String rowKey = req.params("R");
        String colKey = req.params("C");

        Row row = getRow(tableName, rowKey);
        if (!(row != null && row.columns().contains(colKey))) {
            String errMsg = "Not Found: Queried resource does not exist";
            res.status(404, errMsg);
            res.type("text/plain");
            return errMsg;
        }

        res.type("application/octet-stream");
        res.bodyAsBytes(row.getBytes(colKey));

        return null;
    }

    private static String handleGetTable(Request req, Response res) {
        res.type("text/html");
        StringBuilder doc = new StringBuilder("<html><body>");

        doc.append("<h1>Table List</h1>");
        doc.append("<table border=\"1\">");
        doc.append("<tr><th>Table Name</th><th>Number of Keys</th></tr>");

        TreeSet<String> orderedTableNames = new TreeSet<>(tables.keySet());
        File[] files = new File(path).listFiles();
        int fileCount = files != null ? files.length : 0;

        for (int i = 0; i < fileCount; ++i) {
            File file = files[i];
            String fileName = file.getName();
            if (file.isDirectory() && isPersistentTable(fileName)) {
                orderedTableNames.add(fileName);
            }
        }

        for (String tableName : orderedTableNames) {
            int keyCount = getTableSize(tableName);
            doc.append("<tr>");
            doc.append("<td><a href=\"/view/").append(tableName).append("\">").append(tableName).append("</a></td>");
            doc.append("<td>").append(keyCount).append("</td>");
            doc.append("</tr>");
        }

        doc.append("</table>");
        doc.append("</body></html>");

        return doc.toString();
    }

    private static String handleGetView(Request req, Response res) {
        String tableName = req.params("T");
        String startRowKey = req.queryParams("fromRow");

        ConcurrentMap<String, Row> rows = new ConcurrentHashMap<>();
        if (isPersistentTable(tableName)) {
            File[] files = new File(path + "/" + tableName).listFiles();
            if (files != null) {
                for (File file : files) {
                    String rowKey = file.getName();
                    rows.put(rowKey, getRow(tableName, rowKey));
                }
            }
        } else {
            rows = tables.get(tableName);
        }

        List<String> sortedRowKeys = new ArrayList<>(rows.keySet());
        sortedRowKeys.sort(Comparator.naturalOrder());

        List<String> paginatedSortedRowKeys = new ArrayList<>();
        String nextRowKey = null;
        if (startRowKey != null) {
            for (String rowKey : sortedRowKeys) {
                if (rowKey.compareTo(startRowKey) < 0) {
                    continue;
                }
                if (paginatedSortedRowKeys.size() == DEFAULT_PAGE_SIZE) {
                    nextRowKey = rowKey;
                    break;
                }
                paginatedSortedRowKeys.add(rowKey);
            }
        } else if (sortedRowKeys.size() > DEFAULT_PAGE_SIZE) {
            paginatedSortedRowKeys = sortedRowKeys.subList(0, DEFAULT_PAGE_SIZE);
            nextRowKey = sortedRowKeys.get(DEFAULT_PAGE_SIZE);
        } else {
            paginatedSortedRowKeys = sortedRowKeys;
        }

        TreeSet<String> sortedColKeys = new TreeSet<>();
        for (String rowKey : paginatedSortedRowKeys) {
            Row row = getRow(tableName, rowKey);
            Set<String> colKeys = row != null ? row.columns() : null;
            sortedColKeys.addAll(colKeys);
        }

        StringBuilder doc = new StringBuilder();

        doc.append("<html><body>");
        doc.append("<h1>").append(tableName).append("</h1>");

        doc.append("<table border=\"1\">");

        doc.append("<tr>");
        doc.append("<th>Row</th>");
        for (String colKey : sortedColKeys) {
            doc.append("<th>").append(colKey).append("</th>");
        }
        doc.append("</tr>");

        for (String rowKey : paginatedSortedRowKeys) {
            Row row = getRow(tableName, rowKey);
            doc.append("<tr>");
            doc.append("<td>").append(rowKey).append("</td>");
            for (String colKeyHeader : sortedColKeys) {
                String data = row.get(colKeyHeader);
                doc.append("<td>").append(data != null ? data : "").append("</td>");
            }
            doc.append("</tr>");
        }

        doc.append("</table>");

        if (nextRowKey != null) {
            doc.append("<a href=\"/view/").append(tableName).append("?fromRow=")
                    .append(nextRowKey).append("\">Next</a>");
        }

        doc.append("</body></html>");

        res.type("text/html");
        return doc.toString();
    }

    private static String handleGetRowData(Request req, Response res) {
        String tableName = req.params("T");
        String rowKey = req.params("R");

        Row row = getRow(tableName, rowKey);
        if (row == null) {
            String errMsg = "Not Found: Queried resource does not exist";
            res.status(404, errMsg);
            res.type("text/plain");
            return errMsg;
        }

        res.bodyAsBytes(row.toByteArray());

        return null;
    }

    private static String handleGetTableData(Request req, Response res) {
        String tableName = req.params("T");
        String startRow = req.queryParams("startRow");
        String endRow = req.queryParams("endRowExclusive");

        res.type("text/plain");

        try {
            if (isPersistentTable(tableName)) {
                File[] files = new File(path + "/" + tableName).listFiles();
                for (File file : files) {
                    String rowKey = file.getName();
                    boolean isStartRowKeyInRange = startRow == null
                            || rowKey.compareTo(startRow) >= 0;
                    boolean isEndRowKeyInRange = endRow == null || rowKey.compareTo(endRow) < 0;
                    if (!isStartRowKeyInRange || !isEndRowKeyInRange) {
                        continue;
                    }
                    Row row = getRow(tableName, rowKey);
                    byte[] entry = row.toByteArray();
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    outputStream.write(entry);
                    outputStream.write(LF);
                    byte[] data = outputStream.toByteArray();
                    res.write(data);
                }
                res.write(LF);
                return null;
            }

            Map<String, Row> table = tables.get(tableName);
            for (String rowKey : table.keySet()) {
                boolean isStartRowKeyInRange = startRow == null || rowKey.compareTo(startRow) >= 0;
                boolean isEndRowKeyInRange = endRow == null || rowKey.compareTo(endRow) < 0;
                if (!isStartRowKeyInRange || !isEndRowKeyInRange) {
                    continue;
                }
                Row row = getRow(tableName, rowKey);
                byte[] entry = row.toByteArray();
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                outputStream.write(entry);
                outputStream.write(LF);
                byte[] data = outputStream.toByteArray();
                res.write(data);
            }
            res.write(LF);
        } catch (Exception e) {
            String errMsg = "Not Found: Queried resource does not exist";
            res.status(404, errMsg);
            res.type("text/plain");
            return errMsg;
        }

        return null;
    }

    private static String handleGetRowCount(Request req, Response res) {
        String tableName = req.params("T");

        int tableSize = getTableSize(tableName);
        if (tableSize == 0) {
            res.status(404, "Not found");
            return "Not found";
        }
        return "" + tableSize;
    }

    private static String handlePutRename(Request req, Response res) throws IOException {
        String oldTableName = req.params("T");
        String newTableName = new String(req.bodyAsBytes());

        if (isPersistentTable(oldTableName) && !isPersistentTable(newTableName)) {
            res.status(400, "Cannot convert a table from persistent to in-memory");
            return "Cannot convert a table from persistent to in-memory";
        }

        if (isPersistentTable(oldTableName)) {
            Files.move(FileSystems.getDefault().getPath(path, oldTableName), FileSystems.getDefault().getPath(path, newTableName));
        }

        if (tables.get(oldTableName) == null) {
            res.status(404, "Not found");
            return "Table '" + oldTableName + "' not found";
        }

        if (tables.get(newTableName) != null) {
            res.status(409, "Conflict");
            return "Table '" + newTableName + "' already exists!";
        }

        ConcurrentMap<String, Row> oldTable = tables.get(oldTableName);
        tables.put(newTableName, oldTable);
        tables.remove(oldTableName);
        if (isPersistentTable(newTableName)) {
            (new File(path + File.separator + KeyEncoder.encode(newTableName))).mkdirs();
            for (String rowKey : oldTable.keySet()) {
                putRow(newTableName, oldTable.get(rowKey));
            }
        }

        return "OK";
    }

    private static String handlePutDelete(Request req, Response res) {
        String tableName = req.params("T");

        if (isPersistentTable(tableName)) {
            tables.remove(tableName);

            Path dir = Paths.get(path, tableName);
            if (!Files.exists(dir)) {
                res.status(404, "Not found");
                return "Not found";
            }
            try {
                Files.walk(dir)
                        .map(Path::toFile)
                        .forEach(File::delete);

                if (!dir.toFile().exists() || dir.toFile().delete()) {
                    res.status(200, "OK");
                    return "OK";
                } else {
                    res.status(500, "Internal Server Error");
                    return "Error deleting table directory";
                }
            } catch (IOException e) {
                res.status(500, "Internal Server Error");
                return "Error deleting table directory";
            }
        }

        if (tables.containsKey(tableName)) {
            tables.remove(tableName);
            res.status(200, "OK");
            return "OK";
        } else {
            res.status(404, "Not found");
            return "Not found";
        }
    }

    private static synchronized void putRow(String tableName, Row row) {
        if (isPersistentTable(tableName)) {
            String encodedTableName = KeyEncoder.encode(tableName);
            String encodedRowKey = KeyEncoder.encode(row.key);
            new File(path + "/" + encodedTableName).mkdirs();
            File rowFile = new File(path + "/" + tableName + "/" + encodedRowKey);
            try {
                FileOutputStream outputStream = new FileOutputStream(rowFile);
                outputStream.write(row.toByteArray());
                outputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return;
        }

        tables.get(tableName).put(row.key, row);
    }

    private static synchronized Row getRow(String tableName, String rowKey) {
        if (isPersistentTable(tableName)) {
            String encodedRowKey = KeyEncoder.encode(rowKey);
            Path rowPath = Paths.get(path, tableName, encodedRowKey);
            if (!Files.exists(rowPath)) {
                return null;
            }
            try (FileInputStream fis = new FileInputStream(rowPath.toFile())) {
                return Row.readFrom(fis);
            } catch (Exception e) {
                System.err.println("Failed to read row from disk: " + e.getMessage());
                throw new RuntimeException("Failed to read row from disk", e);
            }
        }

        ConcurrentMap<String, Row> table = tables.get(tableName);
        return table != null ? table.get(rowKey) : null;
    }

    private static synchronized void createTableIfNecessary(String tableName) {
        if (tables.get(tableName) == null) {
            tables.put(tableName, new ConcurrentHashMap<>());

            if (isPersistentTable(tableName)) {
                String encodedTableName = KeyEncoder.encode(tableName);
                new File(path + File.separator + encodedTableName).mkdirs();
            }
        }
    }

    private static boolean isPersistentTable(String tableName) {
        return tableName.startsWith("pt-");
    }

    private static int getTableSize(String tableName) {
        if (isPersistentTable(tableName)) {
            String encodedTableName = KeyEncoder.encode(tableName);
            File persistentTables = new File(path + File.separator + encodedTableName);
            File[] persistentTableFiles = persistentTables.listFiles();
            boolean hasPersistentTables = persistentTables.exists() && persistentTableFiles != null;
            return hasPersistentTables ? persistentTableFiles.length : 0;
        }

        ConcurrentMap<String, Row> table = tables.get(tableName);
        return table != null ? table.size() : 0;
    }

    private static String getWorkerId() {
        String id = null;

        try {
            File storagePath = new File(path);
            if (!storagePath.exists()) {
                storagePath.mkdir();
            }

            File file = new File(path + "/id");
            if (file.exists()) {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                id = bufferedReader.readLine().trim();
            }

            if (id == null) {
                id = randomId();
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
                bufferedWriter.write(id);
                bufferedWriter.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return id;
    }

    private static String randomId() {
        StringBuilder randomId = new StringBuilder();
        Random random = new Random();

        for (int i = 0; i < 5; i++) {
            char randomChar = (char) ('a' + random.nextInt(26));
            randomId.append(randomChar);
        }

        return randomId.toString();
    }
}