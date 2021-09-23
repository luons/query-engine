package io.github.luons.engine.core.filter;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class FilterDeserializer extends StdDeserializer<Filter> {

    protected FilterDeserializer() {
        super(Filter.class);
    }

    @Override
    public Filter deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();
        ObjectNode jsonNode = mapper.readTree(jsonParser);
        Iterator<Map.Entry<String, JsonNode>> ite = jsonNode.fields();
        while (ite.hasNext()) {
            Map.Entry<String, JsonNode> entry = ite.next();
            if ("operator".equalsIgnoreCase(entry.getKey())) {
                return mapper.treeToValue(jsonNode, SimpleFilter.class);
            } else if ("filterList".equalsIgnoreCase(entry.getKey())) {
                return mapper.treeToValue(jsonNode, FilterGroup.class);
            }
        }
        throw new UnsupportedOperationException("unsupported deserialization");
    }
}
