package org.dcache.rquota;

import org.dcache.rquota.xdr.rquota;

import javax.security.auth.Subject;

public interface QuotaVfs  {

    /**
     * Get the quota for the given subject.
     *
     * @param subject the subject to get the quota for
     * @return the quota for the given subject
     */
    rquota getQuota(int id, int type);
}
