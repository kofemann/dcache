package org.dcache.pool.classic;

import com.google.common.base.Optional;
import java.io.Serializable;

/**
 *
 */
public class MoverId {

    private final int _id;
    private final Optional<? extends Serializable> _attachement;

    public int getId() {
        return _id;
    }

    public Optional<? extends Serializable> getAttachement() {
        return _attachement;
    }

    public MoverId(int id, Optional<? extends Serializable> attachement) {
        this._id = id;
        this._attachement = attachement;
    }
    
}
