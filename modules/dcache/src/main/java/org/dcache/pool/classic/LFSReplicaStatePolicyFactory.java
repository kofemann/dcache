package org.dcache.pool.classic;

@Deprecated
public class LFSReplicaStatePolicyFactory
{
    /**
     * Factory method for creating ReplicaStatePolicies based on LFS
     * mode.
     */
    public static ReplicaStatePolicy createInstance(String lfs)
    {
	// the all cases handled by sine implementation
        return new ALRPReplicaStatePolicy();
    }
}
