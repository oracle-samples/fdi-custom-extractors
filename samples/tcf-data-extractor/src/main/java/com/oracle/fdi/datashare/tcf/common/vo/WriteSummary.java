package com.oracle.fdi.datashare.tcf.common.vo;

public class WriteSummary {
    private String status; 
    private int noOfRecordsInserted;
    private int noOfRecordsUpdated;
    private int noOfRecordsDeleted;

    public String getStatus() {
        return status;
    }
    public void setStatus(String status) {
        this.status = status;
    }
    public int getNoOfRecordsInserted() {
        return noOfRecordsInserted;
    }
    public void setNoOfRecordsInserted(int noOfRecordsInserted) {
        this.noOfRecordsInserted = noOfRecordsInserted;
    }
    public int getNoOfRecordsUpdated() {
        return noOfRecordsUpdated;
    }
    public void setNoOfRecordsUpdated(int noOfRecordsUpdated) {
        this.noOfRecordsUpdated = noOfRecordsUpdated;
    }
    public int getNoOfRecordsDeleted() {
        return noOfRecordsDeleted;
    }
    public void setNoOfRecordsDeleted(int noOfRecordsDeleted) {
        this.noOfRecordsDeleted = noOfRecordsDeleted;
    } 

    
}
