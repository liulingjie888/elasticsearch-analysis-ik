package org.wltea.analyzer.dic;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.wltea.analyzer.cfg.JdbcConfig;
import org.wltea.analyzer.help.ESPluginLoggerFactory;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author liulingjie
 * @date 2022/11/29 20:36
 */
public class JdbcMonitor implements Runnable {

    //static {
    //    try {
    //        Class.forName("com.mysql.cj.jdbc.Driver");
    //    } catch (Exception e) {
    //        e.getStackTrace();
    //    }
    //}

    /**
     * jdbc配置
     */
    private JdbcConfig jdbcConfig;
    /**
     * 主词汇上次更新时间
     */
    private Timestamp mainLastModitime = Timestamp.valueOf("2022-01-01 00:00:00");
    /**
     * 停用词上次更新时间
     */
    private Timestamp stopLastModitime = Timestamp.valueOf("2022-01-01 00:00:00");

    private static final Logger logger = ESPluginLoggerFactory.getLogger(JdbcMonitor.class.getName());

    public JdbcMonitor(JdbcConfig jdbcConfig) {
        this.jdbcConfig = jdbcConfig;
        try {
            Class.forName(jdbcConfig.getDriver());
        } catch (Exception e) {
            e.getStackTrace();
        }
    }

    @Override
    public void run() {
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            this.runUnprivileged();
            return null;
        });
    }

    /**
     * 加载词汇和停用词
     */
    public void runUnprivileged() {
        //Dictionary.getSingleton().reLoadMainDict();
        loadWords();
    }

    private void loadWords() {
        List<String> mainWords = new ArrayList<>();
        List<String> delMainWords = new ArrayList<>();
        List<String> stopWords = new ArrayList<>();
        List<String> delStopWords = new ArrayList<>();

        setAllWordList(mainWords, delMainWords, stopWords, delStopWords);

        mainWords.forEach(w -> Dictionary.getSingleton().fillSegmentMain(w));
        delMainWords.forEach(w -> Dictionary.getSingleton().disableSegmentMain(w));
        stopWords.forEach(w -> Dictionary.getSingleton().fillSegmentStop(w));
        delStopWords.forEach(w -> Dictionary.getSingleton().disableSegmentStop(w));


        logger.info("ik dic refresh from db. mainLastModitime: {} stopLastModitime: {}", mainLastModitime, stopLastModitime);
    }

    /**
     * 获取主词汇和停用词
     *
     * @param mainWords
     * @param delMainWords
     * @param stopWords
     * @param delStopWords
     */
    private void setAllWordList(List<String> mainWords, List<String> delMainWords, List<String> stopWords, List<String> delStopWords) {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(jdbcConfig.getUrl(), jdbcConfig.getUsername(), jdbcConfig.getPassword());
            setWordList(connection, jdbcConfig.getMainWordSql(), mainLastModitime, mainWords, delMainWords);
            setWordList(connection, jdbcConfig.getStopWordSql(), stopLastModitime, stopWords, delStopWords);
        } catch (SQLException throwables) {
            logger.error("jdbc load words failed: mainLastModitime-{} stopLostMOditime-{}", mainLastModitime, stopLastModitime);
            logger.error(throwables.getStackTrace());
        } finally {

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException throwables) {
                    logger.error("failed to close connection");
                    logger.error(throwables.getMessage());
                }
            }
        }
    }

    /**
     * 连接数据库获取词汇
     *
     * @param connection
     * @param sql
     * @param lastModitime
     * @param words
     * @param delWords
     */
    private void setWordList(Connection connection, String sql, Timestamp lastModitime, List<String> words, List<String> delWords) {
        PreparedStatement prepareStatement = null;
        ResultSet result = null;

        try {
            prepareStatement = connection.prepareStatement(sql);
            prepareStatement.setTimestamp(1, lastModitime);
            result = prepareStatement.executeQuery();

            while (result.next()) {
                String word = result.getString("word");
                Timestamp moditime = result.getTimestamp("moditime");
                String ifdel = result.getString("ifdel");

                if ("1".equals(ifdel)) {
                    delWords.add(word);
                } else {
                    words.add(word);
                }

                // 取最大的时间
                if (moditime.after(lastModitime)) {
                    lastModitime.setTime(moditime.getTime());
                }
            }
        } catch (SQLException throwables) {
            logger.error("jdbc load words failed: {}", lastModitime);
            logger.error(throwables.getMessage());
        } finally {
            if (result != null) {
                try {
                    result.close();
                } catch (SQLException throwables) {
                    logger.error("failed to close prepareStatement");
                    logger.error(throwables.getMessage());
                }
            }

            if (prepareStatement != null) {
                try {
                    prepareStatement.close();
                } catch (SQLException throwables) {
                    logger.error("failed to close prepareStatement");
                    logger.error(throwables.getMessage());
                }
            }
        }
    }
}
