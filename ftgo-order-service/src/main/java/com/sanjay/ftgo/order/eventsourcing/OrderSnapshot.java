package com.sanjay.ftgo.order.eventsourcing;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

@Entity
@Table(name = "order_snapshots")
public class OrderSnapshot {

    @Id
    private Long orderId;

    @Column(name = "last_event_entity_id", nullable = false)
    private Long lastEventEntityId;

    @Lob
    @Column(name = "snapshot_json", nullable = false)
    private String snapshotJson;

    protected OrderSnapshot() {
    }

    public OrderSnapshot(Long orderId, Long lastEventEntityId, String snapshotJson) {
        this.orderId = orderId;
        this.lastEventEntityId = lastEventEntityId;
        this.snapshotJson = snapshotJson;
    }

    public Long getOrderId() {
        return orderId;
    }

    public Long getLastEventEntityId() {
        return lastEventEntityId;
    }

    public String getSnapshotJson() {
        return snapshotJson;
    }

    public OrderSnapshot update(Long lastEventEntityId, String snapshotJson) {
        this.lastEventEntityId = lastEventEntityId;
        this.snapshotJson = snapshotJson;
        return this;
    }
}
