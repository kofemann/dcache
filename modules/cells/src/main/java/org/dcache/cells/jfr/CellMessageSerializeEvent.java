package org.dcache.cells.jfr;

import jdk.jfr.*;

@Name("org.dcache.cells.Message")
@Description("Cell Message event")
@Category({"dCache", "Cell Message", "Serialize"})
@Label("Cell Message")
@Enabled(value = false)
public class CellMessageSerializeEvent extends Event {

    public enum Type {
        Serialize,
        Deserialize
    }

    @Label("Payload type")
    public String peyloadType;
    @Label("Source")
    public String source;
    @Label("Destination")
    public String destination;
    @Label("Direction")
    public String type;

}
