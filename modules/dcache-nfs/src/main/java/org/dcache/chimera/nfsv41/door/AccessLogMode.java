package org.dcache.chimera.nfsv41.door;

/**
 * An {@code AccessLogMode} controls amount of information logged by access
 * logger.
 *
 * @since 2.15
 */
public enum AccessLogMode {

    /**
     * Logging is disabled.
     */
    NONE,
    /**
     * Logging enabled, parent directories logged in a string form of {@link PnfsId}.
     */
    MINIMAL,
    /**
     * Logging enabled, parent directories as full path.
     */
    FULL
}
