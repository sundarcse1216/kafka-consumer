package com.sundar.log.bean;

/**
 *
 * @author sundar
 * @since 2018-01-28
 */
public class LogProperties {

    private String occurranceDate;
    private String severity;
    private String className;
    private String message;

    public String getOccurranceDate() {
        return occurranceDate;
    }

    public void setOccurranceDate(String occurranceDate) {
        this.occurranceDate = occurranceDate;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

}
