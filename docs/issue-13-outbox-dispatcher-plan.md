# Issue #13 — Outbox dispatcher dan NATS wake-up hints

## Tujuan

Mengirim notifikasi internal dari `outbox_events` ke NATS JetStream tanpa menjadikan broker sebagai sumber kebenaran atau pemberi ownership task.

Issue #12 sudah menulis state, event, dan outbox intent dalam transaksi yang sama. Issue #13 menambahkan petugas yang mengambil intent tersebut, mengirim wake-up hint, dan menandai hasil pengiriman.

## Batas tanggung jawab

```text
Issue #12
  state transition + claim + completion
        |
        | outbox intent dalam transaksi yang sama
        v
Issue #13
  outbox dispatcher -> NATS JetStream wake-up hint
        |
        | hanya membangunkan / memicu scan
        v
Worker/scheduler
  kembali ke PostgreSQL untuk claim resmi
```

NATS tidak menentukan siapa yang memiliki task. Message tidak berisi secret, output lengkap, atau izin eksekusi.

## Kontrak dispatcher

1. Ambil batch event yang `published_at IS NULL` dan `next_at <= now()`.
2. Reserve event dengan transaksi singkat dan `FOR UPDATE SKIP LOCKED` agar beberapa dispatcher tidak mengambil baris yang sama pada waktu yang sama.
3. Naikkan `attempts`, set `next_at` ke waktu retry berikutnya, lalu commit reservation sebelum network publish.
4. Publish menggunakan `event_id` stabil dan subject versioned, misalnya `runtime.v1.wakeup.<shard>`.
5. Setelah publish ACK, set `published_at`.
6. Jika publish gagal, simpan error terbatas dan biarkan event dicoba lagi setelah `next_at`.
7. Jika process crash setelah publish tetapi sebelum `published_at`, duplicate delivery diperbolehkan; claim dan state transition tetap harus idempotent di PostgreSQL.

## Bentuk wake-up hint minimum

Payload hanya membawa routing/identitas yang diperlukan untuk scan:

```json
{
  "eventId": "...",
  "organizationId": "...",
  "runId": "...",
  "subject": "execution.state_changed",
  "payloadVersion": 1
}
```

Payload aktual harus divalidasi agar tidak membawa secret atau output task. ACK NATS berarti hint diterima, bukan task selesai.

## Acceptance yang harus dibuktikan

- state + event + outbox tetap atomik;
- dispatcher mengirim event pending dengan real PostgreSQL dan real NATS JetStream;
- crash setelah publish sebelum `published_at` menghasilkan duplicate hint yang aman;
- publish failure menghasilkan retry/backoff;
- dua dispatcher tidak memberi dua ownership;
- Worker tetap melakukan claim melalui database;
- outbox tertua, pending count, publish failure, dan retry dapat diamati;
- NATS down tidak menghilangkan task `READY` karena database reconciliation tetap bisa menemukan task;
- payload tidak memuat secret/output;
- dokumentasi kontrak broker dan runbook diagnosis tersedia.

## Bukan bagian Issue #13

- menjalankan customer code di control plane;
- mengganti PostgreSQL sebagai authority;
- external exactly-once guarantee;
- full durability/recovery gate M2;
- perubahan arsitektur menjadi microservices;
- menganggap ACK broker sebagai completion atau ownership.

## Catatan implementasi

Role runtime dibatasi RLS per tenant. Dispatcher tidak boleh melewati RLS dengan akses global sembarangan; tenant discovery dan pemrosesan outbox harus memakai jalur system/runtime yang sudah disetujui, dengan tenant context hanya pada transaksi yang memproses data tenant tersebut.

`NATS_URL` yang sudah ada di Compose saat ini hanya membuktikan wiring connectivity/health. Issue #13 tetap memerlukan JetStream stream/consumer configuration, publish/ACK behavior, real-dependency tests, metrics, dan runbook.
