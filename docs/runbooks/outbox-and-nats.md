# Outbox dan NATS wake-up hints

## Prinsip

PostgreSQL adalah sumber kebenaran untuk state, ownership, event, dan outbox. NATS JetStream hanya membawa wake-up hint agar control plane lebih cepat memicu scan. Worker tidak terhubung langsung ke NATS dan tidak boleh menganggap message sebagai izin eksekusi.

## Alur normal

```text
state transition + event + outbox
        -> dispatcher reserve batch
        -> JetStream publish ACK
        -> published_at
        -> control-plane wake-up subscriber
        -> authoritative PostgreSQL scan
```

Dispatcher memakai `event_id` stabil sebagai JetStream message ID. Crash setelah publish tetapi sebelum `published_at` boleh menghasilkan delivery ulang; claim/state transition harus tetap idempotent.

## Diagnosis read-only

1. Cek `/readyz` dan pastikan status `nats` dibaca terpisah dari database/schema readiness.
2. Cek log `[OUTBOX]` untuk reservation/publish/mark error.
3. Periksa jumlah outbox yang `published_at IS NULL`, umur `next_at`, dan `last_error` menggunakan koneksi observasi yang berwenang. Jangan mengubah atau menghapus baris secara manual.
4. Cek JetStream stream `DEADBOLT_RUNTIME`, subject `runtime.v1.wakeup`, dan durable queue `deadbolt-control-plane`.
5. Pastikan Worker tetap bisa menemukan task melalui polling/reconciliation ketika NATS degraded.

## Mitigasi

- NATS outage: jangan menghapus outbox; PostgreSQL reconciliation tetap menjadi jalur keselamatan.
- Publish failure: biarkan `next_at` dan backoff melakukan retry; perbaiki konektivitas atau kapasitas broker.
- Pending outbox menumpuk: cek broker, koneksi, permission, stream/subject, dan kapasitas sebelum menaikkan batch size.
- Consumer redelivery: normal setelah crash sebelum ACK; jangan membuat claim dari message. Selalu scan dan claim lewat engine PostgreSQL.
- `published_at` kosong setelah publish berhasil: jangan menandai manual tanpa evidence; duplicate publish dengan `event_id` stabil harus aman.

## Batas operasi

Tidak ada host-wide prune, destructive restore, SQL bypass engine, atau klaim exactly-once ke API eksternal. Rollback binary tidak me-rollback database; migration tetap forward-compatible.
