package site.hnfy258.cluster.replication;

public enum ReplicationState {
    DISCONNECTED("disconnected"),
    CONNECTING("connecting"),
    SYNCING("syncing"),
    STREAMING("streaming"),
    ERROR("error");

    private final String stateName;

    ReplicationState(String stateName) {
        this.stateName = stateName;
    }

    public String getStateName() {
        return stateName;
    }

    public boolean isHealthy(){
        return this == STREAMING;
    }

    public boolean canAcceptReplicationCommands(){
        return this == STREAMING;
    }

    @Override
    public String toString() {
        return stateName;
    }
}
