package org.apache.twill.internal;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.common.Cancellable;
import org.apache.twill.internal.zookeeper.LeaderElection;
import org.apache.twill.zookeeper.ZKClient;

/**
 * Tracks currently active leader elections within the Twill container.
 */
public class ElectionRegistry {
  private final ZKClient zkClient;
  private final Multimap<String, LeaderElection> registry;

  public ElectionRegistry(ZKClient zkClient) {
    this.zkClient = zkClient;
    Multimap<String, LeaderElection> multimap = HashMultimap.create();
    this.registry = Multimaps.synchronizedMultimap(multimap);
  }

  /**
   * Creates a new {@link LeaderElection} for the given arguments, starts the service, and adds it to the registry.
   * @param name Name for the election.
   * @param handler Callback to handle leader and follower transitions.
   * @return An object to cancel the election participation.
   */
  public Cancellable register(String name, ElectionHandler handler) {
    LeaderElection election = new LeaderElection(zkClient, name, handler);
    election.start();
    registry.put(name, election);
    return new CancellableElection(name, election);
  }

  /**
   * Stops all active {@link LeaderElection} processes.
   */
  public void shutdown() {
    for (LeaderElection election : registry.values()) {
      election.stop();
    }
  }

  private class CancellableElection implements Cancellable {
    private final String name;
    private final LeaderElection election;

    public CancellableElection(String name, LeaderElection election) {
      this.name = name;
      this.election = election;
    }

    @Override
    public void cancel() {
      election.stop();
      registry.remove(name, election);
    }
  }
}
