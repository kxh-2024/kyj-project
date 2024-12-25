package com.laibo.service;

import com.laibo.config.MySqlConfig;
import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class SqlProcessorDataService {
    private static Map<String, Integer> idJournalName = new HashMap<>();
    private final MySqlConfig dbConfig;

    public SqlProcessorDataService(MySqlConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    public void processEmptyDocIds() {
        Connection conn = null;
        try {
            conn = dbConfig.getConnection();

            // 设置手动提交事务
            conn.setAutoCommit(false);

            // 查询所有doc_id为空的记录
            String selectSql = "SELECT id, journal_name, year, phase FROM test_meta_data_journal WHERE doc_id IS NULL OR doc_id = ''";
            try (PreparedStatement selectStmt = conn.prepareStatement(selectSql);
                 ResultSet rs = selectStmt.executeQuery()) {

                // 准备更新语句
                String updateSql = "UPDATE test_meta_data_journal SET doc_id = ? WHERE id = ?";
                try (PreparedStatement updateStmt = conn.prepareStatement(updateSql)) {

                    int updateCount = 0;
                    while (rs.next()) {
                        long id = rs.getLong("id");
                        String journalName = rs.getString("journal_name");
                        String year = rs.getString("year");
                        String phase = rs.getString("phase");
                        // 生成新的doc_id
                        String docId = generateId(journalName, year, phase);
                        try {
                            // 更新记录
                            updateStmt.setString(1, docId);
                            updateStmt.setLong(2, id);
                            int affectedRows = updateStmt.executeUpdate();

                            if (affectedRows > 0) {
                                updateCount++;
                            } else {
                                System.err.println("更新失败 - 记录 ID: " + id + " 未找到或未更新");
                            }
                        } catch (SQLException e) {
                            System.err.println("更新记录 ID: " + id + " 时出错: " + e.getMessage());
                            throw e;
                        }
                    }
                    // 提交事务
                    conn.commit();
                    System.out.println("成功更新 " + updateCount + " 条记录的doc_id");
                }
            }
        } catch (SQLException e) {
            System.err.println("数据库操作出错: " + e.getMessage());
            e.printStackTrace();
            // 发生错误时回滚事务
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    System.err.println("回滚事务失败: " + ex.getMessage());
                }
            }
        } finally {
            // 关闭连接
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    System.err.println("关闭数据库连接失败: " + e.getMessage());
                }
            }
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

    public static void main(String[] args) {
        MySqlConfig dbConfig = new MySqlConfig("jdbc:mysql://192.168.11.4:30312/my_database", "root", "Testmysql8");
        SqlProcessorDataService service = new SqlProcessorDataService(dbConfig);
        service.processEmptyDocIds();
    }
}
