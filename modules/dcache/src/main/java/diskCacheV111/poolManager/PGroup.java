package diskCacheV111.poolManager;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

class PGroup extends PoolCore implements SelectionPoolGroup {

    private static final long serialVersionUID = 3883973457610397314L;
    final Map<String, Pool> _poolList = new ConcurrentHashMap<>();
    final List<PGroup> _pgroupList = new CopyOnWriteArrayList<>();
    final List<PGroup> _refs = new CopyOnWriteArrayList<>();

    private final boolean resilient;

    PGroup(String name, boolean resilient) {
        super(name);
        this.resilient = resilient;
    }

    @Override
    public boolean isResilient() {
        return resilient;
    }

    @Override
    public boolean isPrimary() {
        return resilient;
    }

    @Override
    public String toString() {
        return getName() + "(links=" + _linkList.size()
              + "; pools=" + _poolList.size() + "; resilient=" + resilient + "; nested groups=" + _pgroupList.size() + ")";
    }

    @Override
    public List<Pool> getPools() {
        List<Pool> allPools = new ArrayList<>(_poolList.values());
        _pgroupList.forEach(g -> allPools.addAll(g.getPools()));
        return allPools;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PGroup group = (PGroup) o;
        return getName().equals(group.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName());
    }
}
