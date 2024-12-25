package com.laibo.service;

import com.laibo.config.MySqlConfig;
import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @Author: kexihe
 * @createTime: 2024年12月25日 08:38:20
 * @Description:
 */
public class SqlProcessorFileService {
    private static final int BATCH_SIZE = 500;
    private static final int THREAD_POOL_SIZE = 5;
    private final ExecutorService executorService;
    private final MySqlConfig dbConfig;

    private static Map<String, Integer> idJournalName = new HashMap<>();

    public SqlProcessorFileService(MySqlConfig dbConfig) {
        this.dbConfig = dbConfig;
        this.executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    }

    public void processSqlFile(String inputFilePath) {
        List<String> processedStatements = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("--") || !line.toUpperCase().contains("INSERT INTO")) {
                    continue;
                }
                if (line.endsWith(";")) {
                    // 处理单条SQL语句
                    handleSql(processedStatements, line);
                }
            }
            // 处理批量插入
            processBatchInsert(processedStatements);

            System.out.println("处理完成，共处理 " + processedStatements.size() + " 条SQL语句");
        } catch (IOException e) {
            System.err.println("处理文件时发生错误: " + e.getMessage());
        }
    }

    private static void handleSql(List<String> processedStatements, String line) {
        try {
            // 查找关键位置
            int columnsStart = line.indexOf("(");
            int valuesPos = line.toUpperCase().indexOf("VALUES");

            if (columnsStart == -1 || valuesPos == -1) {
                System.out.println("跳过无效的SQL语句: " + line.substring(0, Math.min(50, line.length())) + "...");
                return;
            }

            int valuesStart = line.indexOf("(", valuesPos);
            if (valuesStart == -1) {
                System.out.println("VALUES子句格式错误: " + line.substring(0, Math.min(50, line.length())) + "...");
                return;
            }

            // 提取并解析字段列表
            String columnsPart = line.substring(columnsStart + 1, valuesPos).trim();
            columnsPart = columnsPart.replaceAll("\\s+", " ").replaceAll("`", "");
            String[] columns = columnsPart.split(",");

            // 提取并解析值列表
            int valuesEnd = line.lastIndexOf(")");
            if (valuesEnd == -1 || valuesEnd <= valuesStart) {
                System.out.println("VALUES子句结束位置无效: " + line.substring(0, Math.min(50, line.length())) + "...");
                return;
            }

            String valuesPart = line.substring(valuesStart + 1, valuesEnd).trim();
            String[] values = splitValues(valuesPart);

            // 确保字段数量和值的数量匹配
            if (columns.length != values.length) {
                System.out.println("字段数量与值的数量不匹配: " + columns.length + " vs " + values.length);
                return;
            }

            // 查找所需字段的位置
            int journalNameIndex = -1;
            int yearIndex = -1;
            int phaseIndex = -1;

            for (int i = 0; i < columns.length; i++) {
                String column = columns[i].trim().toLowerCase();
                if (column.contains("journal_name")) {
                    journalNameIndex = i;
                } else if (column.contains("year")) {
                    yearIndex = i;
                } else if (column.contains("phase")) {
                    phaseIndex = i;
                }
            }

            if (journalNameIndex == -1 || yearIndex == -1 || phaseIndex == -1) {
                System.out.println("缺少必要的字段: journal_name, year 或 phase");
                return;
            }

            // 提取值并生成ID
            String journalName = cleanValue(values[journalNameIndex]);
            String year = cleanValue(values[yearIndex]);
            String phase = cleanValue(values[phaseIndex]);

            if (journalName.isEmpty() || year.isEmpty()) {
                System.out.println("journal_name 或 year 值为空");
                return;
            }

            String docId = generateId(journalName, year, phase);

            // 构建新的SQL语句
            StringBuilder newStatement = new StringBuilder();
            newStatement.append(line.substring(0, columnsStart + 1));
            newStatement.append("`doc_id`, ");
            newStatement.append(line.substring(columnsStart + 1, valuesStart + 1));
            newStatement.append("'").append(docId).append("', ");
            newStatement.append(line.substring(valuesStart + 1));
            newStatement.append(";");
            String sql = newStatement.toString().replace("document_ no", "document_no");
            processedStatements.add(sql);

        } catch (Exception e) {
            System.out.println("处理SQL语句时出错: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void processBatchInsert(List<String> statements) {
        int totalBatches = (int) Math.ceil((double) statements.size() / BATCH_SIZE);
        System.out.println("开始处理，总共 " + totalBatches + " 个批次");

        for (int i = 0; i < statements.size(); i += BATCH_SIZE) {
            int endIndex = Math.min(i + BATCH_SIZE, statements.size());
            List<String> batch = statements.subList(i, endIndex);

            executorService.submit(() -> {
                try (Connection conn = dbConfig.getConnection()) {
                    conn.setAutoCommit(false);
                    for (String sql : batch) {
                        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                            pstmt.executeUpdate();
                        }
                    }
                    conn.commit();
                    System.out.println("成功处理一个批次，包含 " + batch.size() + " 条SQL语句");
                } catch (SQLException e) {
                    System.err.println("执行批处理时发生错误: " + e.getMessage());
                }
            });
        }

        shutdownAndAwaitTermination();
    }

    private void shutdownAndAwaitTermination() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    System.err.println("线程池未能完全终止");
                }
            }
        } catch (InterruptedException ie) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static String generateId(String journalName, String year, String issue) {
        // 获取期刊名称的拼音首字母
        String idPrefix = getPinyinInitials(journalName);

        // 处理期号
        if (!issue.equals("nan") && issue.length() <= 1) {
            issue = "0" + issue;
        } else if (issue.equals("nan")) {
            issue = "00";
        }
        // 获取序号
        int idSuffix;
        if (idJournalName.containsKey(idPrefix)) {
            idSuffix = idJournalName.get(idPrefix) + 1;
        } else {
            idSuffix = 1;
        }
        idJournalName.put(idPrefix, idSuffix);

        // 生成最终的ID: 首字母 + 年份 + 期号 + 序号
        return idPrefix + year + issue + idSuffix;
    }

    private static String getPinyinInitials(String chinese) {
        if (chinese == null || chinese.trim().isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        HanyuPinyinOutputFormat format = new HanyuPinyinOutputFormat();
        format.setCaseType(HanyuPinyinCaseType.LOWERCASE);
        format.setToneType(HanyuPinyinToneType.WITHOUT_TONE);

        try {
            for (char c : chinese.toCharArray()) {
                if (Character.toString(c).matches("[\\u4E00-\\u9FA5]+")) {
                    String[] pinyinArray = PinyinHelper.toHanyuPinyinStringArray(c, format);
                    if (pinyinArray != null && pinyinArray.length > 0) {
                        builder.append(pinyinArray[0].charAt(0));
                    }
                }
            }
        } catch (BadHanyuPinyinOutputFormatCombination e) {
            System.out.println("转换拼音时出错: " + e.getMessage());
            e.printStackTrace();
        }

        return builder.toString().toUpperCase();
    }

    private static String[] splitValues(String valuesPart) {
        List<String> values = new ArrayList<>();
        StringBuilder currentValue = new StringBuilder();
        boolean inQuotes = false;

        for (char c : valuesPart.toCharArray()) {
            if (c == '\'' && (currentValue.length() == 0 || currentValue.charAt(currentValue.length() - 1) != '\\')) {
                inQuotes = !inQuotes;
            }

            if (c == ',' && !inQuotes) {
                values.add(currentValue.toString().trim());
                currentValue = new StringBuilder();
            } else {
                currentValue.append(c);
            }
        }
        values.add(currentValue.toString().trim());

        return values.toArray(new String[0]);
    }

    private static String cleanValue(String value) {
        return value.replaceAll("^'|'$", "").trim();
    }

    public static void main(String[] args) {
        MySqlConfig dbConfig = new MySqlConfig("jdbc:mysql://192.168.11.4:30312/my_database", "root", "Testmysql8");
        SqlProcessorFileService sqlProcessorFileService = new SqlProcessorFileService(dbConfig);
        sqlProcessorFileService.processSqlFile("C:\\Users\\Admin\\Downloads\\元数据.sql");
    }
}

