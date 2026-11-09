# Uplink DC accounting

Helium charges each purchased uplink copy in 24-byte increments of the full
LoRaWAN frame, with a minimum of 1 DC. Headers, MIC, FOpts and any MAC commands
in the frame payload all contribute to its length.

For each integration uplink with Helium receptions, HPR looks up ChirpStack's
`device:{dev_eui}:stream:frame` Redis stream. It matches the DevEUI, DevAddr,
16-bit frame counter, port, message type and gateway reception IDs. The matching
`UplinkFrameLog.phy_payload` supplies the full frame length. The same measured
count is used for SQLite, the tenant Redis stream and SQS.

ChirpStack must retain per-device frame logs (`[monitoring]`
`per_device_frame_log_max_history` and `per_device_frame_log_ttl`) long enough
for the event consumer to read them, including after an outage. The lookup
examines at most 100 retained entries per device and excludes entries with a
Redis timestamp later than the integration event. Frame logs are written before
the integration event by ChirpStack.

If a matching frame is unavailable, including when logging is disabled, history
has expired, reception IDs are missing or Redis is unavailable, accounting falls
back to the application payload plus 13 bytes (12 for an empty payload). Each
fallback emits `[DC frame size unavailable, using estimate]`. This estimate
can miss FOpts and MAC-only payload bytes; it is not an exact frame measurement.

This improves the byte count for the uplinks delivered to this application. It
does not reconcile against Helium's purchased-packet records or recover uplinks
lost from ChirpStack's event stream, and it retains the existing Helium reception
count and delivery/retry behavior.
