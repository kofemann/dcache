package org.dcache.restful.resources.srr.record;


public class Nearline {

    /**
     * (Required)
     */
    private Long totalsize;
    /**
     * (Required)
     */
    private Long usedsize;
    private Long reservedsize;

    /**
     * (Required)
     */
    public Long getTotalsize() {
        return totalsize;
    }

    /**
     * (Required)
     */
    public void setTotalsize(Long totalsize) {
        this.totalsize = totalsize;
    }

    public Nearline withTotalsize(long totalsize) {
        this.totalsize = totalsize;
        return this;
    }

    /**
     * (Required)
     */
    public Long getUsedsize() {
        return usedsize;
    }

    /**
     * (Required)
     */
    public void setUsedsize(Long usedsize) {
        this.usedsize = usedsize;
    }

    public Nearline withUsedsize(long usedsize) {
        this.usedsize = usedsize;
        return this;
    }

    public Long getReservedsize() {
        return reservedsize;
    }

    public void setReservedsize(Long reservedsize) {
        this.reservedsize = reservedsize;
    }

    public Nearline withReservedsize(long reservedsize) {
        this.reservedsize = reservedsize;
        return this;
    }

}
