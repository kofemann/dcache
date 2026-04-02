/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 - 2020 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.gplazma.scitoken;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonPointer;
import tools.jackson.core.JsonToken;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.jsontype.TypeSerializer;
import tools.jackson.databind.node.JsonNodeType;
import java.util.List;

/**
 * A JsonNode that delegates all operations to some other JsonNode.
 */
public abstract class ForwardingJsonNode extends JsonNode {

    protected abstract JsonNode delegate();

    @Override
    public JsonToken asToken() {
        return delegate().asToken();
    }

    @Override
    public JsonParser.NumberType numberType() {
        return delegate().numberType();
    }

    @Override
    public JsonNode get(int index) {
        return delegate().get(index);
    }

    @Override
    public JsonNode get(String fieldName) {
        return delegate().get(fieldName);
    }

    @Override
    public JsonNode findValue(String fieldName) {
        return delegate().findValue(fieldName);
    }

    @Override
    public JsonNode findPath(String fieldName) {
        return delegate().findPath(fieldName);
    }

    @Override
    public JsonNode findParent(String fieldName) {
        return delegate().findParent(fieldName);
    }

    @Override
    public List<JsonNode> findValues(String fieldName, List<JsonNode> foundSoFar) {
        return delegate().findValues(fieldName, foundSoFar);
    }

    @Override
    public List<String> findValuesAsString(String fieldName, List<String> foundSoFar) {
        return delegate().findValuesAsString(fieldName, foundSoFar);
    }

    @Override
    public List<JsonNode> findParents(String fieldName, List<JsonNode> foundSoFar) {
        return delegate().findParents(fieldName, foundSoFar);
    }

    @Override
    public JsonNode path(String fieldName) {
        return delegate().path(fieldName);
    }

    @Override
    public JsonNode path(int index) {
        return delegate().path(index);
    }

    @Override
    public JsonParser traverse(ObjectReadContext objectReadContext) {
        return delegate().traverse(objectReadContext);
    }

    @Override
    public String toString() {
        return delegate().toString();
    }

    @Override
    public JsonNode deepCopy() {
        return delegate().deepCopy();
    }

    @Override
    public boolean equals(Object o) {
        return delegate().equals(o);
    }

    @Override
    protected JsonNode _at(JsonPointer jsonPointer) {
        return null;
    }

    @Override
    public JsonNodeType getNodeType() {
        return delegate().getNodeType();
    }

    @Override
    public void serialize(JsonGenerator jsonGenerator, SerializationContext serializationContext)
          throws JacksonException {
        delegate().serialize(jsonGenerator, serializationContext);
    }

    @Override
    public void serializeWithType(JsonGenerator jsonGenerator, SerializationContext serializers,
          TypeSerializer typeSerializer) throws JacksonException {
        delegate().serializeWithType(jsonGenerator, serializers, typeSerializer);
    }

    @Override
    public String stringValue() {
        return delegate().stringValue();
    }

    @Override
    public String stringValue(String defaultValue) {
        return delegate().stringValue(defaultValue);
    }

    @Override
    public Optional<String> stringValueOpt() {
        return delegate().stringValueOpt();
    }

    @Override
    public String asString() {
        return delegate().asString();
    }

    @Override
    public String asString(String defaultValue) {
        return delegate().asString(defaultValue);
    }

    @Override
    public Optional<String> asStringOpt() {
        return delegate().asStringOpt();
    }

    @Override
    public byte[] binaryValue() {
        return delegate().binaryValue();
    }

    @Override
    public boolean booleanValue() {
        return delegate().booleanValue();
    }

    @Override
    public boolean booleanValue(boolean defaultValue) {
        return delegate().booleanValue(defaultValue);
    }

    @Override
    public Optional<Boolean> booleanValueOpt() {
        return delegate().booleanValueOpt();
    }

    @Override
    public boolean asBoolean() {
        return delegate().asBoolean();
    }

    @Override
    public boolean asBoolean(boolean defaultValue) {
        return delegate().asBoolean(defaultValue);
    }

    @Override
    public Optional<Boolean> asBooleanOpt() {
        return delegate().asBooleanOpt();
    }

    @Override
    public Number numberValue() {
        return delegate().numberValue();
    }

    @Override
    public short shortValue() {
        return delegate().shortValue();
    }

    @Override
    public short shortValue(short defaultValue) {
        return delegate().shortValue(defaultValue);
    }

    @Override
    public Optional<Short> shortValueOpt() {
        return delegate().shortValueOpt();
    }

    @Override
    public short asShort() {
        return delegate().asShort();
    }

    @Override
    public short asShort(short defaultValue) {
        return delegate().asShort(defaultValue);
    }

    @Override
    public Optional<Short> asShortOpt() {
        return delegate().asShortOpt();
    }

    @Override
    public int intValue() {
        return delegate().intValue();
    }

    @Override
    public int intValue(int defaultValue) {
        return delegate().intValue(defaultValue);
    }

    @Override
    public OptionalInt intValueOpt() {
        return delegate().intValueOpt();
    }

    @Override
    public int asInt() {
        return delegate().asInt();
    }

    @Override
    public int asInt(int defaultValue) {
        return delegate().asInt(defaultValue);
    }

    @Override
    public OptionalInt asIntOpt() {
        return delegate().asIntOpt();
    }

    @Override
    public long longValue() {
        return delegate().longValue();
    }

    @Override
    public long longValue(long defaultValue) {
        return delegate().longValue(defaultValue);
    }

    @Override
    public OptionalLong longValueOpt() {
        return delegate().longValueOpt();
    }

    @Override
    public long asLong() {
        return delegate().asLong();
    }

    @Override
    public long asLong(long defaultValue) {
        return delegate().asLong(defaultValue);
    }

    @Override
    public OptionalLong asLongOpt() {
        return delegate().asLongOpt();
    }

    @Override
    public BigInteger bigIntegerValue() {
        return delegate().bigIntegerValue();
    }

    @Override
    public BigInteger bigIntegerValue(BigInteger defaultValue) {
        return delegate().bigIntegerValue(defaultValue);
    }

    @Override
    public Optional<BigInteger> bigIntegerValueOpt() {
        return delegate().bigIntegerValueOpt();
    }

    @Override
    public BigInteger asBigInteger() {
        return delegate().asBigInteger();
    }

    @Override
    public BigInteger asBigInteger(BigInteger defaultValue) {
        return delegate().asBigInteger(defaultValue);
    }

    @Override
    public Optional<BigInteger> asBigIntegerOpt() {
        return delegate().asBigIntegerOpt();
    }

    @Override
    public float floatValue() {
        return delegate().floatValue();
    }

    @Override
    public float floatValue(float defaultValue) {
        return delegate().floatValue(defaultValue);
    }

    @Override
    public Optional<Float> floatValueOpt() {
        return delegate().floatValueOpt();
    }

    @Override
    public float asFloat() {
        return delegate().asFloat();
    }

    @Override
    public float asFloat(float defaultValue) {
        return delegate().asFloat(defaultValue);
    }

    @Override
    public Optional<Float> asFloatOpt() {
        return delegate().asFloatOpt();
    }

    @Override
    public double doubleValue() {
        return delegate().doubleValue();
    }

    @Override
    public double doubleValue(double defaultValue) {
        return delegate().doubleValue(defaultValue);
    }

    @Override
    public OptionalDouble doubleValueOpt() {
        return delegate().doubleValueOpt();
    }

    @Override
    public double asDouble() {
        return delegate().asDouble();
    }

    @Override
    public double asDouble(double defaultValue) {
        return delegate().asDouble(defaultValue);
    }

    @Override
    public OptionalDouble asDoubleOpt() {
        return delegate().asDoubleOpt();
    }

    @Override
    public BigDecimal decimalValue() {
        return delegate().decimalValue();
    }

    @Override
    public BigDecimal decimalValue(BigDecimal defaultValue) {
        return delegate().decimalValue(defaultValue);
    }

    @Override
    public Optional<BigDecimal> decimalValueOpt() {
        return delegate().decimalValueOpt();
    }

    @Override
    public BigDecimal asDecimal() {
        return delegate().asDecimal();
    }

    @Override
    public BigDecimal asDecimal(BigDecimal defaultValue) {
        return delegate().asDecimal(defaultValue);
    }

    @Override
    public Optional<BigDecimal> asDecimalOpt() {
        return delegate().asDecimalOpt();
    }

    @Override
    public JsonNode required(String propertyName) {
        return delegate().required(propertyName);
    }

    @Override
    public JsonNode required(int index) {
        return delegate().required(index);
    }

    @Override
    public boolean isEmbeddedValue() {
        return delegate().isEmbeddedValue();
    }
}
