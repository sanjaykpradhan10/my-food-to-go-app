# Ch.12 B3a Linkerd service mesh — captured evidence

Date: 2026-08-16

## Incremental verification: order-service + restaurant-service

After annotating the `ftgo` namespace for auto-injection (Task 2) and restarting only
`order-service` and `restaurant-service` (Task 3), both pods report `2/2 Ready`
(app container + `linkerd-proxy` sidecar).

`linkerd viz tap deploy/order-service -n ftgo --to deploy/restaurant-service`, captured while
`order-service` called `restaurant-service`'s `/actuator/health` in-cluster, shows:

```
req id=0:0 proxy=out src=10.244.0.155:39148 dst=10.244.0.154:8085 tls=true :method=GET :authority=restaurant-service:8085 :path=/actuator/health
rsp id=0:0 proxy=out src=10.244.0.155:39148 dst=10.244.0.154:8085 tls=true :status=200 latency=15963µs
end id=0:0 proxy=out src=10.244.0.155:39148 dst=10.244.0.154:8085 tls=true duration=1609µs response-length=0B
```

`linkerd viz stat deploy/order-service deploy/restaurant-service -n ftgo`:

```
NAME                 MESHED   SUCCESS      RPS   LATENCY_P50   LATENCY_P95   LATENCY_P99   TCP_CONN
order-service           1/1   100.00%   0.7rps           4ms         850ms         970ms          4
restaurant-service      1/1   100.00%   0.7rps         100ms         470ms         494ms          4
```

This confirms mTLS is active between meshed pods before rolling injection out to the rest of the
`ftgo` namespace (Task 4).
