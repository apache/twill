package org.apache.twill.discovery;

import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * Test for the {@link Discoverable} object.
 */
public class DiscoverableTest {

  @Test
  public void testEqualsAndHashCode() {
    Discoverable d1 = new Discoverable("service1", InetSocketAddress.createUnresolved("host1", 12345),
                                       new byte[] {1, 2, 3, 4});
    Discoverable d2 = new Discoverable("service1", InetSocketAddress.createUnresolved("host1", 12345),
                                       new byte[] {1, 2, 3, 4});
    Assert.assertEquals(d1, d2);
    Assert.assertEquals(d1.hashCode(), d2.hashCode());

    Discoverable d3 = new Discoverable("service2", InetSocketAddress.createUnresolved("host1", 12345),
                                       new byte[] {1, 2, 3, 4});
    Assert.assertNotEquals(d1, d3);
    Assert.assertNotEquals(d1.hashCode(), d3.hashCode());

    Discoverable d4 = new Discoverable("service1", InetSocketAddress.createUnresolved("host2", 12345),
                                       new byte[] {1, 2, 3, 4});
    Assert.assertNotEquals(d1, d4);
    Assert.assertNotEquals(d1.hashCode(), d4.hashCode());

    Discoverable d5 = new Discoverable("service1", InetSocketAddress.createUnresolved("host1", 12345));
    Assert.assertNotEquals(d1, d5);
    Assert.assertNotEquals(d1.hashCode(), d5.hashCode());
  }
}
