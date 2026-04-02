package org.dcache.restful.providers;

import tools.jackson.core.Version;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JacksonModule;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

/**
 * Fine-tuning how JSON we be presented by redefine the default Jackson behaviour.
 **/
@Provider
public class ObjectMapperProvider implements ContextResolver<ObjectMapper> {

    private final static JacksonModule PNFSID_SERIALIZER = createPnfsIdSerializer();
    private final ObjectMapper defaultObjectMapper = createDefaultMapper();
    private final ObjectMapper listObjectMapper = createListObjectMapper();

    private static ObjectMapper createListObjectMapper() {
        return JsonMapper.builder()
              .addModule(PNFSID_SERIALIZER)
              .enable(DeserializationFeature.UNWRAP_ROOT_VALUE)
              .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
              .build();
    }

    private static ObjectMapper createDefaultMapper() {
        return JsonMapper.builder()
              .addModule(PNFSID_SERIALIZER)
              .enable(SerializationFeature.INDENT_OUTPUT)
              //.setSerializationInclusion(JsonInclude.Include.NON_NULL);
              .build();
    }

    private static JacksonModule createPnfsIdSerializer() {
        Version version = new Version(1, 0, 0,
              null, null, null);
        SimpleModule pnfsIdModule = new SimpleModule("PnfsIdModule", version);
        pnfsIdModule.addSerializer(new PnfsidSerializer());
        return pnfsIdModule;
    }

    @Override
    public ObjectMapper getContext(Class<?> type) {
        if (type == java.util.ArrayList.class) {
            return listObjectMapper;
        } else {
            return defaultObjectMapper;
        }
    }
}
