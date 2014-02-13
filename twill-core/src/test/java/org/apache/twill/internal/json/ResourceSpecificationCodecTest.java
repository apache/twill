package org.apache.twill.internal.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.internal.DefaultResourceSpecification;
import org.junit.Test;

import static org.apache.twill.api.ResourceSpecification.SizeUnit.GIGA;
import static org.junit.Assert.assertEquals;
import static org.unitils.reflectionassert.ReflectionAssert.assertLenientEquals;

public class ResourceSpecificationCodecTest {
  private final Gson gson = new GsonBuilder().serializeNulls()
          .registerTypeAdapter(ResourceSpecification.class, new ResourceSpecificationCodec())
          .registerTypeAdapter(DefaultResourceSpecification.class, new ResourceSpecificationCodec())
          .create();

  @Test
  public void testCodec() throws Exception {
    String expectedString = "{\"cores\":2,\"memorySize\":1024,\"instances\":2,\"uplink\":100,\"downlink\":100,\"hosts\":[\"one1\",\"two2\"],\"racks\":[\"three3\"]}";
    final ResourceSpecification expected =
            new DefaultResourceSpecification(2, 1024, 2, 100, 100, new String[]{"one1", "two2"}, new String[]{"three3"});
    final String actualString = gson.toJson(expected);
    assertEquals(expectedString, actualString);

    final JsonElement expectedJson = gson.toJsonTree(expected);
    final ResourceSpecification actual = gson.fromJson(expectedJson, DefaultResourceSpecification.class);
    final JsonElement actualJson = gson.toJsonTree(actual);

    assertEquals(expectedJson, actualJson);
    assertLenientEquals(expected, actual);
  }

  @Test
  public void testBuilder() throws Exception {
    final ResourceSpecification actual = ResourceSpecification.Builder.with()
            .setVirtualCores(5)
            .setMemory(4, GIGA)
            .setInstances(3)
            .setHosts("a1", "b2", "c3")
            .setRacks("r2")
            .setUplink(10, GIGA)
            .setDownlink(5, GIGA).build();
    final DefaultResourceSpecification expectd =
            new DefaultResourceSpecification(5, 4096, 3, 10240, 5120, new String[]{"a1", "b2", "c3"}, new String[]{"r2"});
    assertLenientEquals(expectd, actual);
  }

}
