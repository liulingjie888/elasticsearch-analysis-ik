package org.wltea.analyzer.cfg;

/**
 * @author liulingjie
 * @date 2022/11/30 16:03
 */
public class JdbcConfig {

    public JdbcConfig() {
    }

    public JdbcConfig(String url, String username, String password, String mainWordSql, String stopWordSql, Integer interval) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.mainWordSql = mainWordSql;
        this.stopWordSql = stopWordSql;
        this.interval = interval;
    }

    private String url;

    private String username;

    private String password;

    private String mainWordSql;

    private String stopWordSql;

    private Integer interval;

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getMainWordSql() {
        return mainWordSql;
    }

    public String getStopWordSql() {
        return stopWordSql;
    }

    public Integer getInterval() {
        return interval;
    }
}
