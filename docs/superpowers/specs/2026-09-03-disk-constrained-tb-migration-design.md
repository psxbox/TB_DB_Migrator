# Disk-cheklangan TB migratsiya — Design Spec

Date: 2026-09-03
Branch: `tb-3.4` | TB: 3.4.1 PE | Migrator: .NET 10 (host'da)

## 1. Kontekst va cheklovlar

Eski stack:

- TB 3.4.1 PE — Linux OS'ga RPM paket orqali o'rnatilgan (`systemctl`: `thingsboard` service).
- Eski PostgreSQL v18 — Docker containerda (nom o'zgaruvchan; spec'da `OLD_PG` env orqali aniqlanadi, hardcode yo'q).
- `ts_kv` — `RANGE (ts)` bo'yicha partitsiyalangan (~277 partition).
- Butun DB ~102 GB, root diskda ~10 GB bo'sh joy. `VACUUM` + log tozalash biroz joy ochishi mumkin, lekin kafolat yo'q.
- `/home` — alohida partition, bo'sh joy bor. `~/backup/` — partition dumplari (`pg_dump -Fc`) uchun joy. Docker data (`/var/lib/docker`) root'da — shuning uchun katta dump'lar faqat `/home` ga yoziladi.
- Yangi PostgreSQL + ScyllaDB + `thingsboard/tb-pe-node:3.4.1PE` — bitta compose'da, data uchun oldindan yaratilgan docker external volume'lar bilan. TB dastlab ishga tushirilmaydi.
- `ts_kv` siz baza kichik — yangi PG uchun katta joy kerak emas; asosiy o'sish Scylla'da bo'ladi va u eski partition'lar DROP qilinishi bilan bo'shagan joy hisobiga qoplanadi.

Tasdiqlangan qarorlar:

- Muvaffaqiyatli ko'chgan partition — count-sverka (`PG count == Scylla count`) dan keyin eski PG'dan `DROP` qilinadi (joy bo'shatish uchun).
- Har bir partition oldidan `pg_dump -Fc` bilan `~/backup/` ga dump olinadi (rollback sug'urtasi).
- Yangi TB tekshiruvi — to'liq + API check (login, dashboard, latest, history).
- `ts_kv` siz nusxa usuli — muallif tanloviga qoldirilgan.

## 2. Maqsad

1. `ts_kv` dan tashqari barcha jadvallarni yangi PostgreSQL'ga ko'chirish (eski TB to'xtatilmagan holda).
2. Avval **oxirgi (eng yangi) `ts_kv` partition** ni ScyllaDB'ga migratsiya qilish.
3. Eski TB'ni stop qilib, yangi TB'ni (yangi PG + Scylla) tekshirish.
4. Tekshiruv o'tsa, qolgan partitionlarni yangidan-eski tomon migratsiya qilish.
5. Har bir tasdiqlangan partition dump'ini `~/backup/` da saqlab, eski partitionni `DROP` qilib joy bo'shatish.

Non-goals:

- `attribute_kv`, entity, relation jadvallarini Scylla'ga ko'chirish yo'q (ular PG'da qoladi — hybrid arxitektura).
- Eski RPM TB'ni avtomatik o'chirish yo'q (faqat `stop`; o'chirish qo'lda, kuzatuvdan keyin).
- Scylla replication_factor tuning yo'q (single-node, RF=1 — mavjud `ScyllaWriter.InitSchema` saqlanadi).

## 3. Arxitektura

```text
ESKI (tegilmaydi, faqat o'qiladi + oxirida stop):
  TB RPM (systemctl thingsboard) ──> OLD_PG (docker, v18, :5432)

YANGI (bitta compose, external docker volume'lar bilan):
  postgres-new (postgres:18, host:15432, vol: tb-pg-new-data) ─┐
  scylladb (scylla:2026.1, host:9042, vol: tb-scylla-data) ────┤─> tb-pe:3.4.1PE (profile: tb, dastlab o'chiq)
  Migrator (.NET, host'da): OLD_PG:5432 dan o'qiydi -> scylladb:9042 ga yozadi
  ~/backup/ (/home): <partition>.dump (pg_dump -Fc, per-partition)
```

### 3.1 Yangi compose (`docker-compose.new-stack.yml`)

Bitta fayl, uchta service. TB `profiles: ["tb"]` bilan — dastlab `up postgres-new scylladb` qilinadi, TB keyin.

- `postgres-new`:
  - image: `postgres:18` (eski bilan bir xil major versiya — dump/restore mosligi uchun).
  - host port: `127.0.0.1:15432:5432` (eski 5432 bilan konflikt yo'q).
  - data: external docker volume `tb-pg-new-data:/var/lib/postgresql` (oldindan `docker volume create` bilan yaratiladi; `ts_kv` siz baza kichik; PG18 rasmiy image'da VOLUME `/var/lib/postgresql` — `/data` suffix yo'q).
  - memory: `mem_limit: 512m`, `mem_reservation: 256m`; `command: postgres -c shared_buffers=128MB -c effective_cache_size=256MB -c maintenance_work_mem=64MB -c max_connections=100`.
  - `POSTGRES_DB=thingsboard`, user/pass env'dan.
- `scylladb`:
  - image: mavjud `scylladb/scylla:2026.1` saqlanadi.
  - host port: `127.0.0.1:9042:9042` (eski stack'da Scylla yo'q — konflikt yo'q).
  - data: external docker volume `tb-scylla-data:/var/lib/scylla` (oldindan `docker volume create` bilan yaratiladi).
  - memory: `mem_limit: 1g` (Scylla process'ga 512 MB: `command: --smp 1 --memory 512M --overprovisioned 1`; ~512 MB — container OS/page-cache/housekeeping zaxirasi; 1G `--memory` host'da 500 MB bo'sh joyda `insufficient physical memory` bilan start olmaydi).
  - healthcheck: `cqlsh -e 'describe keyspaces'` (mavjud).
- `tb-pe`:
  - image: `thingsboard/tb-pe-node:3.4.1PE`.
  - `profiles: ["tb"]` — dastlab ishlamaydi.
  - memory: `mem_limit: 3g`, `mem_reservation: 2g`; `JAVA_OPTS: "-Xms1G -Xmx2G"` (konteyner limit ichida qolishi uchun heap 2 GB, qolgan 1 GB — off-heap/Metaspace/OS).
  - env: `SPRING_DATASOURCE_URL=jdbc:postgresql://postgres-new:5432/thingsboard`,
    `DATABASE_TS_TYPE=cassandra`, `TS_KV_PARTITIONING=MONTHS`,
    `CASSANDRA_URL=scylladb:9042`, `CASSANDRA_KEYSPACE_NAME=thingsboard`.
  - ports: eski RPM TB 8080/1883/5683 ni host'da band qilgan, shuning uchun yangi TB
    faqat eski TB stop qilingandan keyin ishga tushiriladi; port mapping 1:1 qoldiriladi.
  - PE license env (`TB_LICENSE_SECRET` / `TB_LICENSE_INSTANCE_DATA_FILE`) — operator kiritadi, spec'da secret saqlanmaydi.

```yaml
volumes:
  tb-pg-new-data:
    external: true
    name: tb-pg-new-data
  tb-scylla-data:
    external: true
    name: tb-scylla-data
```

Nega external docker volume: compose'dan oldin bir marta `docker volume create` qilinadi — compose qayta yaratilganda data o'chib ketmaydi. `ts_kv` siz baza kichik bo'lgani uchun `tb-pg-new-data` kichik bo'ladi. Diqqat: docker volume'lar default `/var/lib/docker` (root disk) da yashaydi — shuning uchun Scylla o'sishi root'ni to'ldirmasligi uchun har bir partition verify'dan keyin eski PG'dan DROP qilinadi (6.3-sikl).

### 3.2 RAM byudjeti (jami 8 GB)

| Service | Limit | Reservation | Izoh |
|---------|-------|-------------|------|
| `tb-pe` | 3g | 2g | Heap `JAVA_OPTS=-Xms1G -Xmx2G`; eski RPM TB stop qilingandan keyin ishga tushadi — bir vaqtda ikkita TB ishlamaydi |
| `scylladb` | 1g | — | Process'ga `--smp 1 --memory 512M --overprovisioned 1`; ~512 MB container OS/page-cache zaxirasi |
| `postgres-new` | 512m | 256m | `shared_buffers=128MB`, kichik baza uchun yetarli |

Jami yangi stack: ~4.5 GB. Qolgan ~3.5 GB: OS + eski PG container + migrator (.NET host'da) + boshqa servislar. Migrator'da `workers: 2` va `scylla_concurrency: 32` bilan boshlash tavsiya etiladi (Scylla 512M limitda timeout bermasligi uchun).

## 4. Tayyorgarlik (joy bo'shatish + inventar)

1. `df -h / /home` — root va home bo'sh joyini qayd etish.
2. `docker ps` — `OLD_PG` container nomini aniqlash: `export OLD_PG=$(docker ps --format '{{.Names}}' | grep -i postgres | head -1)`.
3. Eski TB/RPM holati: `systemctl status thingsboard`, `docker exec $OLD_PG psql -U postgres -d thingsboard -c '\d+ ts_kv'`.
4. Partition ro'yxati (migrator ham shu so'rovni ishlatadi):
   `SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent='ts_kv'::regclass ORDER BY 1;`
   Har bir partition uchun `pg_total_relation_size` + `SELECT min(ts), max(ts), count(*)` — eng yangi partitionni aniqlash uchun.
5. Joy bo'shatish (ixtiyoriy, lekin tavsiya): `VACUUM (VERBOSE off)` emas, balki
   `docker exec $OLD_PG psql -c 'VACUUM (ANALYZE) ts_kv_latest;'`, TB log rotate (`journalctl --vacuum-size=500M`), docker log prune. `VACUUM FULL` — **taqiqlanadi** (joy talab qiladi + lock).
6. `mkdir -p ~/backup` + external volume'larni yaratish:
   `docker volume create tb-pg-new-data && docker volume create tb-scylla-data`.

## 5. `ts_kv` siz nusxa (eski TB ishlayotgan holda)

Prinsip: schema to'liq, data — `ts_kv*` partitionlarsiz. Dump fayl root'ga yozilmaydi — pipe yoki to'g'ridan `pg_restore`.

Qadamlar (barchasi eski TB ishlayotgan holda, `screen` ichida).
Root diskni to'ldirmaslik uchun **barcha katta dump'lar pipe orqali** to'g'ridan `~/backup/` ga yoziladi
(`docker exec` ga `-t` berilmaydi — binary buziladi). Container ichidagi `/tmp/*.dump` oraliq fayl sifatida ishlatilmaydi
(chunki container overlay ham root diskda `/var/lib/docker` da yashaydi).

1. `docker compose -f docker-compose.new-stack.yml up -d postgres-new` — yangi PG'ni ko'tarish, bo'sh `thingsboard` DB tayyor.
2. Schema-only dump (barcha jadvallar, jumladan `ts_kv` bo'sh strukturasi, kichik fayl):
   `docker exec $OLD_PG pg_dump -U postgres -d thingsboard --schema-only -Fc > ~/backup/schema.dump`
   Yangi PG'ga restore: `pg_restore -h 127.0.0.1 -p 15432 -U postgres -d thingsboard ~/backup/schema.dump`.
3. Data-only dump, `ts_kv` datasisiz (schema allaqachon bor) — pipe, oraliq faylsiz:
   `docker exec $OLD_PG pg_dump -U postgres -d thingsboard --data-only -Fc --exclude-table-data='ts_kv*' > ~/backup/nontskv.dump`
   Bu `ts_kv` parent + barcha child partition datalarini tashlab ketadi, lekin `ts_kv_latest`, `ts_kv_dictionary`, barcha entity jadvallari, sequences datalarini oladi.
   Keyin `pg_restore -h 127.0.0.1 -p 15432 -U postgres -d thingsboard ~/backup/nontskv.dump` yangi PG'ga.
   Fayl hajmi — `ts_kv` siz DB hajmiga teng (noma'lum; `~/backup/` /home'da bo'lgani uchun root to'lmaydi).
4. Tekshiruv: yangi PG'da jadval soni (`\dt`), `ts_kv` bo'shligi (`SELECT count(*) FROM ts_kv` = 0), `ts_kv_dictionary` va `ts_kv_latest` count'larini eski bilan solishtirish.
5. Delta-izoh: nusxa paytida eski TB ishlayotgani uchun entity/latest jadvallariga ozgina yozuv tushishi mumkin. Switchover paytida (eski TB stop qilingan) kichik jadvallar (`ts_kv_latest`, `ts_kv_dictionary`) bir marta qayta dump/restore qilinadi (tez, MB'lar darajasi). Katta jadvallar uchun takror shart emas.

## 6. Partition migratsiya tartibi

### 6.1 Birinchi — oxirgi partition

1. Eng yangi partition nomini aniqlash (`max(ts)` eng katta bo'lgani, masalan `ts_kv_2026_08`).
2. Partition dump — pipe orqali to'g'ridan `~/backup/` ga (oraliq `/tmp` faylsiz,
   sababi 5-bo'limda: container overlay ham root diskda yashaydi):
   `docker exec $OLD_PG pg_dump -U postgres -d thingsboard -Fc -t <part> > ~/backup/<part>.dump && ls -lh ~/backup/<part>.dump`.
3. Migrator bilan faqat shu partitionni ko'chirish (yangi CLI — 8-bo'lim):
   `tbmigrator start --partition <part> [--resume]`.
   Eski TB hali ishlayapti — hot partition'ga yangi yozuvlar tushishda davom etadi.
4. Delta-pass: birinchi pass tugagach, `tbmigrator start --partition <part> --delta-from <pass1_max_ts>` — pass oralig'ida kelgan yozuvlarni ko'chirish.
5. Verify (9-bo'lim): `tbmigrator verify --partition <part>` — PG count == Scylla count + sample. O'tmasa — DROP yo'q, log tahlil qilinadi.
6. Verify o'tgach, partition hali DROP qilinmaydi (eski TB hali ishlayapti va hot partition'ga yozmoqda — 7-bo'lim switchover'dan keyin DROP).

### 6.2 Switchover + yangi TB tekshiruvi

1. `sudo systemctl stop thingsboard` (eski RPM TB) — shu paytdan eski PG frozen.
2. Kichik jadvallar deltasini qayta nusxalash (`ts_kv_latest`, `ts_kv_dictionary`) — 5-bo'lim 5-qadam.
3. Hot partition'ning so'nggi deltasini ko'chirish (`--delta-from`).
4. `docker compose -f docker-compose.new-stack.yml --profile tb up -d tb-pe`, log kuzatish (`docker logs -f tb-pe` — "ThingsBoard started").
5. To'liq tekshiruv (Hammasi + API):
   - Login (UI + `POST /api/auth/login` 200).
   - Dashboard ochilishi, device ro'yxati (yangi PG'dan).
   - Latest telemetry (`GET /api/plugins/telemetry/.../values/timeseries` — `ts_kv_latest_cf` dan).
   - History grafik oxirgi partition oralig'ida (`GET .../values/timeseries?startTs&endTs` — `ts_kv_cf` dan, count>0).
   - API check skripti (login→device→latest→history, barchasi 200 va bo'sh emas).
6. Tekshiruv o'tsa — 6.3 ga. O'tmasa — rollback (10-bo'lim): yangi TB stop, eski TB start.

### 6.3 Qolgan partitionlar (yangidan-eskiga)

Har bir partition uchun sikl (eski TB o'chiq, eski PG frozen — delta shart emas):

1. Dump (`pg_dump -Fc -t <part>` → `~/backup/<part>.dump`, pipe orqali).
2. Migrate (`tbmigrator start --partition <part>`).
3. Verify (count + sample).
4. Verify o'tsa — DROP: `tbmigrator drop --partition <part> --dump-file ~/backup/<part>.dump --verified` (double-guard — 9-bo'lim).
5. `df -h /` bilan bo'shagan joyni qayd etish; keyingi partition'ga o'tish.

Tartib yangidan-eskiga: yangi TB history so'rovlari avval yangi datalarni ko'radi, foydalanuvchi tajribasi buzilmaydi.

## 7. Hot partition DROP qoidasi

Oxirgi partition faqat switchover tekshiruvi o'tgandan keyin DROP qilinadi (6.2-6-qadamdan keyin). Sabab: eski TB stop bo'lguncha unga yozuv tushadi; erta DROP data-loss beradi.

## 8. Migrator o'zgarishlari (tool)

Mavjud kodda partition-scope yo'q (`PgReader` butun `ts_kv` ni entity-key bo'yicha stream qiladi). Yangi imkoniyatlar:

- `PgReader.ListPartitionsAsync()`: `pg_inherits` dan child partitionlar + har birining `min(ts)/max(ts)/count(*)`. `status` da ko'rsatiladi.
- `PgReader.StreamPartitionAsync(part, batchSize)`: `SELECT ... FROM <part>` (to'g'ridan child jadvaldan — parent scan emas, tez). Keyset pagination `ORDER BY ts, entity_id, key`.
- `PgReader.CountPartitionAsync(part)`: `SELECT count(*) FROM <part>`.
- `ScyllaReader.CountPartitionAsync(partRange)`: verify uchun — yangi metod (`ScyllaWriter` ga reader qo'shiladi yoki alohida `ScyllaReader` class). `ts_kv_cf` da `partition` qiymatlari `Partition.Compute(ts)` bilan hisoblanadi; count PG bilan solishtiriladi.
- CLI:
  - `tbmigrator list-partitions [--config]` — nom, min/max ts, count, size.
  - `tbmigrator start --partition <part> [--delta-from <ts>] [--resume] [--workers N]` — faqat shu partition. Mavjud `--historical-only` saqlanadi.
  - `tbmigrator verify --partition <part>` — exit code 0 = count match + sample match; 1 = mismatch.
  - `tbmigrator drop --partition <part> --dump-file <path> --verified` — uchala shart bajarilmasa rad etadi: dump fayl mavjud, verify o'tgan (checkpoint'da belgi), `--verified` flag berilgan. `DROP TABLE <part>;` ni eski PG'da bajaradi.
- Checkpoint (`migration_progress.json`): `partitions: {<part>: {state, pg_count, scylla_count, dump_file, verified, dropped}}` maydoni qo'shiladi. Resume partition darajasida.
- `config.yaml`: `migrator.partition_batch` (standart `batch_size` bilan bir xil) va `migrator.verify_sample_size` (standart 1000) qo'shiladi. Mavjud kalitlar o'zgarmaydi.

## 9. Verify + DROP safety gate

DROP — eng xavfli operatsiya. Qoida (istisnosiz):

1. `~/backup/<part>.dump` mavjud va `pg_restore --list` bilan o'qiladi.
2. `verify` o'tgan: `pg_count == scylla_count` VA random sample (default 1000 qator: entity_id+key+ts bo'yicha PG vs Scylla qiymat solishtirish) 100% mos.
3. Operator `drop` ni `--verified` bilan tasdiqlagan.
4. Hot (oxirgi) partition uchun qo'shimcha: switchover tekshiruvi (6.2-5) o'tgan bo'lishi shart.

`DROP` emas `DETACH`: `DROP TABLE <part>` — joy darhol OS'ga qaytadi. `DETACH PARTITION` data'ni saqlab qoladi — joy bo'shamaydi, maqsadga zid.

## 10. Rollback

- Har qanday partition verify'siz DROP qilinmaydi — dump + eski PG'da data bor.
- Switchover tekshiruvi o'tmasa: `docker compose --profile tb stop tb-pe`, `sudo systemctl start thingsboard` — eski stack joyida.
- Yangi PG buzilsa: `~/backup/schema.dump` + `nontskv.dump` dan qayta restore.
- DROP qilingan partition kerak bo'lsa: `pg_restore -h 127.0.0.1 -p 15432 ... ~/backup/<part>.dump` (yangi PG'ga) yoki eski PG'ga (`docker exec` orqali) — lekin eski PG'ga qaytarish root joyini yana to'ldiradi, faqat favqulodda.

## 11. Risklar

| Risk | Ta'sir | Yechim |
|------|--------|--------|
| `ts_kv` siz baza kutilgandan katta chiqsa | Nusxa o'rtada to'xtaydi | Dump/restore pipe orqali, oraliq fayl root'da saqlanmaydi; `~/backup` /home'da (tasdiqlandi: `ts_kv` siz baza kichik) |
| Bitta partition dump'ining o'zi root `/tmp` ga sig'maydi | Dump fail | To'g'ridan pipe: `docker exec pg_dump > ~/backup/x.dump` |
| Yangi TB PE license yo'q | TB start fail | Oldindan license env tayyorlash; switchover'dan oldin `docker compose config` tekshirish |
| Eski PG container nomi hardcode | Manual ishlamaydi | `OLD_PG` env + discovery buyrug'i; tool'da `--pg-container` flag |
| Hot partition deltasiz DROP | Data-loss | 7-bo'lim qoidasi: hot DROP faqat switchover'dan keyin |
| Docker volume'lar root diskda (`/var/lib/docker`) | Scylla o'sishi root'ni to'ldiradi | Har partition verify'dan keyin eski PG'dan DROP (6.3); `df -h /` har siklda; `docker system df -v` bilan volume o'sishini kuzatish |

## 12. Acceptance criteria

1. Bitta compose'da uchala service aniqlangan; TB `profiles: ["tb"]` bilan dastlab o'chiq; `tb-pg-new-data` va `tb-scylla-data` external volume'lar oldindan yaratilgan.
2. Dump'lar `~/backup/` (/home) da — root'da katta dump fayl yo'q; docker volume o'sishi DROP sikli bilan qoplanadi.
3. `ts_kv` siz nusxa + delta-qayta-nusxa dokumentlangan va bajarilgan.
4. Oxirgi partition birinchi ko'chgan; switchover to'liq + API check'dan o'tgan.
5. Har bir partition: dump → migrate → verify → DROP siklidan o'tgan; DROP faqat 9-bo'lim gate'dan keyin.
6. Tool'da `list-partitions / start --partition / verify / drop` + partition checkpoint mavjud.
7. Rollback har bosqichda mumkin (dump'lar `~/backup/` da).

## 13. Runbook (qisqa)

```bash
export OLD_PG=$(docker ps --format '{{.Names}}' | grep -i postgres | head -1)
mkdir -p ~/backup
docker volume create tb-pg-new-data
docker volume create tb-scylla-data
docker compose -f docker-compose.new-stack.yml up -d postgres-new scylladb
# schema + non-ts_kv (5-bo'lim)
tbmigrator list-partitions
# oxirgi partition: dump -> start -> delta -> verify (6.1)
sudo systemctl stop thingsboard
# latest/dictionary delta + hot delta (6.2), tb-pe up, full+API check
# qolganlar: dump -> start -> verify -> drop (6.3, yangidan-eskiga)
```
