# TB_DB_Migrator — ThingsBoard PostgreSQL → ScyllaDB ko'chirish vositasi

> **Versiya:** 3.0 (`tb-3.4` branch) | **ThingsBoard:** 3.4.1 PE | **Til:** O'zbek (Latin)
>
> **Ishlash modeli:** Yangi stack (PostgreSQL + ScyllaDB + TB PE) — bitta compose'da (`docker-compose.new-stack.yml`), migrator (.NET 10) — host'da.
> **Rejim:** disk-cheklangan partition sikl — har bir `ts_kv` partition: dump → migrate → verify → DROP.

---

## Mundarija

1. [Kirish](#1-kirish)
2. [Arxitektura](#2-arxitektura)
3. [Talablar](#3-talablar)
4. [Loyihani serverga olish (git clone)](#4-loyihani-serverga-olish-git-clone)
5. [Tayyorgarlik](#5-tayyorgarlik)
6. [Yangi stackni ko'tarish](#6-yangi-stackni-kotarish)
7. [.NET muhit va ulanishlar](#7-net-muhit-va-ulanishlar)
8. [`ts_kv` siz nusxa](#8-ts_kv-siz-nusxa)
9. [Partition migratsiya](#9-partition-migratsiya)
   - [9.1 Partition ro'yxati](#91-partition-royxati)
   - [9.2 Birinchi — oxirgi partition](#92-birinchi--oxirgi-partition)
   - [9.3 Switchover + yangi TB tekshiruvi](#93-switchover--yangi-tb-tekshiruvi)
   - [9.4 Qolgan partitionlar (yangidan-eskiga)](#94-qolgan-partitionlar-yangidan-eskiga)
   - [9.5 Hot partition DROP qoidasi](#95-hot-partition-drop-qoidasi)
10. [Verify + DROP safety gate](#10-verify--drop-safety-gate)
11. [Konfiguratsiya](#11-konfiguratsiya)
12. [Checkpoint va resume](#12-checkpoint-va-resume)
13. [Xatoliklarni ko'rish](#13-xatoliklarni-korish)
14. [Rollback](#14-rollback)
15. [Muhim eslatmalar](#15-muhim-eslatmalar)

---

## 1. Kirish

**TB_DB_Migrator** — ThingsBoard PE ning vaqt seriyali ma'lumotlarini (timeseries) PostgreSQL ma'lumotlar bazasidan ScyllaDB ga ko'chirish uchun mo'ljallangan amaliy vosita.

### Nima qiladi?

- Eski PostgreSQL dagi `ts_kv` partitionlarini bittadan o'qiydi, ScyllaDB dagi Cassandra-format jadvallarga yozadi
- `ts_kv_latest` va `ts_kv_dictionary` ni yangi PostgreSQL ga `pg_dump`/`pg_restore` bilan ko'chiradi
- Har bir partition uchun sikl: **dump → migrate → verify → DROP** (joy bo'shatish bilan)
- Avval **oxirgi (eng yangi) partition** ko'chiriladi, switchover tekshiruvidan keyin qolganlari yangidan-eskiga

### Cheklovlar (spec 1-bo'lim)

- Eski stack: TB 3.4.1 PE (RPM, `systemctl thingsboard`) + PostgreSQL v18 (Docker, ~102 GB DB, root'da ~10 GB bo'sh)
- `/home` alohida — `~/backup/` dagi `pg_dump -Fc` dumplari faqat shu yerga yoziladi (root'ga katta fayl yo'q)
- Yangi stack bitta compose'da (`docker-compose.new-stack.yml`): `postgres-new` + `scylladb` + `tb-pe` (dastlab o'chiq)
- Jami RAM 8 GB — limitlar: `tb-pe` 3g, `scylladb` 1g (`--memory 512M`), `postgres-new` 512m
- `VACUUM FULL` — **taqiqlanadi** (joy talab qiladi + lock); faqat oddiy `VACUUM`

### Qachon ishlatiladi?

- ThingsBoard yuklama o'sganda va PostgreSQL timeseries yozuvlari millionlab qatorga yetganda
- ScyllaDB ga o'tib, yozish/o'qish tezligini va gorizontal masshtablashni yaxshilash kerak bo'lganda
- PostgreSQL da saqlash hajmi muammo bo'lganda (bu rejim aynan disk-cheklangan server uchun)

### Key dictionary (TB 3.4.1 PE)

ThingsBoard 3.4.1 PE da kalitlar lug'ati jadvali `ts_kv_dictionary` deb nomlanadi:

```sql
ts_kv_dictionary (key varchar(255) PK, key_id serial UNIQUE)
```

`ts_kv.key` va `ts_kv_latest.key` — integer (`key_id` ga FK). Migrator `ts_kv_dictionary` ni o'qiydi va `key_id → key` xaritasini tuzadi (rasmiy `DictionaryParser` dagi kabi). Fallback sifatida TB 4.x dagi `key_dictionary` ham sinab ko'riladi. Lug'at topilmasa, `ts_kv.key` to'g'ridan-to'g'ri ishlatiladi (toza-SQL rejimi).

### Manba sxemasi (TB 3.4.1 PE, tasdiqlangan)

```text
ts_kv:            PARTITIONED TABLE, RANGE (ts) — 277 ta partitsiya
  entity_id uuid NOT NULL, key integer NOT NULL, ts bigint NOT NULL,
  bool_v, str_v varchar(10000000), long_v, dbl_v, json_v
  PK: (entity_id, key, ts)

ts_kv_dictionary: key varchar(255) PK, key_id serial UNIQUE

ts_kv_latest:     entity_id uuid NOT NULL, key integer NOT NULL, ts bigint NOT NULL, ...
  PK: (entity_id, key)
```

### Rasmiy migrator bilan farq

ThingsBoard `release-3.4` dagi rasmiy tool (`tools/.../migrator`: `MigratorTool`, `PgCaMigrator`, `DictionaryParser`, `RelatedEntitiesParser`, `WriterBuilder`) **offline** ishlaydi: `pg_dump` → SSTable generatsiya (`CQLSSTableWriter`) → fayllarni qo'lda `data/thingsboard` ga ko'chirish → `nodetool compact` → hybrid rejimga o'tish. Bu vosita esa **online** ishlaydi: PostgreSQL dan to'g'ridan-to'g'ri o'qiydi, ScyllaDB ga CQL orqali yozadi — avval oxirgi partition (eski TB ishlayotgan paytda, delta-pass bilan), switchover'dan keyin qolgan partitionlar yangidan-eskiga. Entity ro'yxati, `ts_kv_dictionary` formati, partition (oy boshi, UTC) va `str_v → dbl_v` cast mantig'i rasmiy tool bilan bir xil.

---

## 2. Arxitektura

```
REMOTE LINUX SERVER
═══════════════════════════════════════════════════════════════════

  ESKI (faqat o'qiladi + oxirida stop):
  ┌─────────────────────── Docker ───────────────────────┐
  │  ┌───────────────┐                                    │
  │  │ OLD_PG (v18)  │  :5432 (eski nom, OLD_PG env)      │
  │  │ 102 GB, ts_kv │                                    │
  │  │ RANGE(ts)     │                                    │
  │  └───────┬───────┘                                    │
  └──────────┼────────────────────────────────────────────┘
             │  TB RPM (systemctl thingsboard, host'da)
             │  migrator o'qiydi (SQL)

  YANGI (docker-compose.new-stack.yml, external volume'lar):
  ┌─────────────────────────────────────────────────────┐
  │  postgres-new (postgres:18, 127.0.0.1:15432, 512m)  │
  │    vol: tb-pg-new-data (ts_kv siz baza, kichik)      │
  │  scylladb (scylla:2026.1, 127.0.0.1:9042, 1g)          │
  │    vol: tb-scylla-data  (--smp 1 --memory 512M)       │
  │  tb-pe (tb-pe-node:3.4.1PE, profile: tb, 3g)         │
  │    dastlab O'CHIQ, switchover'da yoqiladi            │
  └─────────────────────────────────────────────────────┘
             │  migrator yozadi (CQL)

  HOST (to'g'ridan-to'g'ri):
  ┌─────────────────────────────────────────────────────┐
  │  Migrator (.NET 10) ~/projects/TB_DB_Migrator/      │
  │  OLD_PG:5432 dan o'qiydi → scylladb:9042 ga yozadi  │
  │  workers: 2, scylla_concurrency: 32 (tavsiya)       │
  └─────────────────────────────────────────────────────┘

  ~/backup/ (/home, alohida partition):
    schema.dump, nontskv.dump, <part>.dump (pg_dump -Fc, pipe orqali)
```

**Ishlash modeli:** eski TB (RPM) va eski PG ishlashda davom etadi — migrator eski PG dan o'qiydi (`localhost:5432`), yangi ScyllaDB ga yozadi (`localhost:9042`). Yangi PG (`127.0.0.1:15432`) va yangi TB dastlab tekshiruvgacha ishlamaydi. Har bir tasdiqlangan partition eski PG dan DROP qilinib joy bo'shatiladi.

**Migratsiya tartibi (spec 2-bo'lim):**

| # | Bosqich | Tavsif |
|---|---------|--------|
| 1 | Tayyorgarlik | Joy/inventar, volume'lar, `~/backup/` |
| 2 | Yangi stack | `postgres-new` + `scylladb` up (TB o'chiq) |
| 3 | `ts_kv` siz nusxa | Schema + data (`ts_kv*` siz), eski TB ishlayotgan holda |
| 4 | Oxirgi partition | Dump → migrate → delta → verify (DROP yo'q) |
| 5 | Switchover | Eski TB stop, delta, yangi TB up, to'liq + API tekshiruv |
| 6 | Qolganlar | Yangidan-eskiga: dump → migrate → verify → DROP |

---

## 3. Talablar

### Remote server (migratsiya bajariluvchi server)

| Talab | Qiymat | Izoh |
|-------|--------|------|
| OS | Linux (Ubuntu 20.04+) | Tavsiya: Ubuntu 22.04 LTS |
| Docker | 24.0+ | `docker compose` v2 plugin (`docker-compose` v1 emas) |
| .NET SDK | 10.0+ | Migrator host'da ishlaydi |
| RAM | **8 GB jami** | Byudjet: `tb-pe` 3g + `scylladb` 1g + `postgres-new` 512m ≈ 4.5 GB; qolgan ~3.5 GB: OS + eski PG + migrator + boshqa servislar |
| Eski stack | TB 3.4.1 PE (RPM) + PG v18 (Docker) | DB ~102 GB, root'da ~10 GB bo'sh |
| `/home` | Alohida partition, bo'sh joy bor | `~/backup/` — faqat shu yerga dump yoziladi |
| CPU | 2 yadro | Tavsiya: 4+ yadro |

### RAM byudjeti (spec 3.2)

| Service | Limit | Reservation | Izoh |
|---------|-------|-------------|------|
| `tb-pe` | 3g | 2g | `JAVA_OPTS=-Xms1G -Xmx2G`; eski RPM TB stop qilingandan keyin yoqiladi — ikkita TB bir vaqtda ishlamaydi |
| `scylladb` | 1g | — | `--smp 1 --memory 512M --overprovisioned 1`; ~512 MB container OS/page-cache zaxirasi |
| `postgres-new` | 512m | 256m | `shared_buffers=128MB`, kichik (`ts_kv` siz) baza uchun yetarli |

Migrator'da `workers: 2` va `scylla_concurrency: 32` bilan boshlang (Scylla 512M limitda timeout bermasligi uchun).

### Tekshirish buyruqlari

```bash
docker --version
docker compose version
dotnet --version   # 10.0+ bo'lishi kerak
free -h
df -h / /home
docker system df -v   # volume o'sishini kuzatish uchun
```

---

## 4. Loyihani serverga olish (git clone)

Serverda `~/projects` ga clone qiling va `tb-3.4` branch'ga o'ting:

```bash
mkdir -p ~/projects
cd ~/projects

git clone https://github.com/psxbox/TB_DB_Migrator.git
cd TB_DB_Migrator
git checkout tb-3.4
```

Keyingi barcha buyruqlar shu papkada bajariladi: `~/projects/TB_DB_Migrator/`.

---

## 5. Tayyorgarlik

Barcha quyidagi buyruqlar **remote serverda** bajariladi (SSH orqali kirgandan keyin). Uzoq buyruqlar `screen` (yoki `tmux`) ichida bajariladi — SSH uzilsa ish to'xtamasligi uchun (`screen -S migration`, chiqish `Ctrl+A` keyin `D`, qaytish `screen -r migration`).

### Eski stack inventari

```bash
# Konteyner nomini aniqlash (hardcode yo'q — keyingi qadamlarda $OLD_PG ishlatiladi)
export OLD_PG=$(docker ps --format '{{.Names}}' | grep -i postgres | head -1)
echo "OLD_PG=$OLD_PG"

# Eski TB (RPM, host'da o'rnatilgan) holati
sudo systemctl status thingsboard --no-pager | head -5

# Disk holati
df -h / /home
docker system df -v
```

```bash
# ts_kv strukturasi va partition ro'yxati (migrator ham shu so'rovni ishlatadi)
docker exec $OLD_PG psql -U postgres -d thingsboard -c '\d+ ts_kv'
docker exec $OLD_PG psql -U postgres -d thingsboard -c \
  "SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent='ts_kv'::regclass ORDER BY 1;"
docker exec $OLD_PG psql -U postgres -d thingsboard -c \
  "SELECT COUNT(*) FROM ts_kv_dictionary;"
```

`ts_kv.key` — integer (`key_id` ga FK), `ts_kv` — `RANGE (ts)` bo'yicha partitsiyalangan jadval (~277 partition).

### Joy bo'shatish (xavfsiz)

```bash
# Faqat oddiy VACUUM — VACUUM FULL TAQIQLANADI (joy talab qiladi + lock beradi)
docker exec $OLD_PG psql -U postgres -d thingsboard -c 'VACUUM (ANALYZE) ts_kv_latest;'

# Loglar
sudo journalctl --vacuum-size=500M
docker container prune -f
docker image prune -f
```

### Volume'lar va papkalar

```bash
mkdir -p ~/backup
docker volume create tb-pg-new-data
docker volume create tb-scylla-data
```

> **Muhim:** barcha katta dump'lar pipe orqali to'g'ridan `~/backup/` (`/home`) ga yoziladi — `docker exec` ga `-t` berilmaydi (binary buziladi). Container ichidagi `/tmp/*.dump` oraliq fayl sifatida ishlatilmaydi (container overlay ham root diskdagi `/var/lib/docker` da yashaydi).

---

## 6. Yangi stackni ko'tarish

`docker-compose.new-stack.yml` — bitta fayl, uchta service. TB `profiles: ["tb"]` bilan — dastlab faqat `postgres-new` + `scylladb` ko'tariladi, `tb-pe` switchover'gacha o'chiq turadi:

```bash
cd ~/projects/TB_DB_Migrator

docker compose -f docker-compose.new-stack.yml up -d postgres-new scylladb
```

Tayyor bo'lguncha kuting (30–90 soniya):

```bash
# Healthcheck holatini kuzatish
docker compose -f docker-compose.new-stack.yml ps

# postgres-new va scylladb STATUS ustuni "healthy" bo'lishi kerak
```

Loglarni ko'rish:

```bash
docker logs -f scylladb-new
# "Scylla version ... initialization completed" ko'ringanda tayyor
```

> Portlar: `postgres-new` → `127.0.0.1:15432`, `scylladb` → `127.0.0.1:9042` (eski 5432 bilan konflikt yo'q). `tb-pe` faqat switchover'da (`--profile tb` bilan) yoqiladi, chunki eski RPM TB 8080/1883/5683 portlarni band qilgan. CQL porti faqat host'dan (localhost) ko'rinadi, tashqaridan emas.

## 7. .NET muhit va ulanishlar

Host'da .NET SDK o'rnating va loyihani build qiling (bir marta):

```bash
cd ~/projects/TB_DB_Migrator

dotnet --version   # 10.0+ bo'lishi kerak
dotnet build TbMigrator.csproj -c Release
```

`config.yaml` standart holatda eski PG (`localhost:5432`) va yangi ScyllaDB (`localhost:9042`) ga sozlangan — o'zgartirish shart emas. Muhit o'zgaruvchilari (`PG_HOST`, `PG_PORT`, `SCYLLA_HOST` va boshqalar) `config.yaml` qiymatlarini ustidan yozadi.

> **Diqqat:** migrator har doim **eski** PG dan o'qiydi (`PG_PORT=5432`). Yangi PG (`15432`) ga faqat `pg_restore` orqali yoziladi (8-bo'lim) — migrator unga ulanmaydi.

> Schema (keyspace + jadvallar) avtomatik yaratiladi — `start` buyrug'i ishga tushganda `init-schema` ni o'zi chaqiradi. Alohida bajarish ham mumkin: `dotnet bin/Release/net10.0/tbmigrator.dll init-schema`.

---

## 8. `ts_kv` siz nusxa

Prinsip (spec 5-bo'lim): schema to'liq, data — `ts_kv*` partitionlarsiz. Barchasi eski TB ishlayotgan holda, `screen` ichida. **Barcha katta dump'lar pipe orqali** to'g'ridan `~/backup/` ga yoziladi (`docker exec` ga `-t` berilmaydi — binary buziladi).

**Qadam 1 — schema-only dump** (barcha jadvallar, jumladan bo'sh `ts_kv` strukturasi, kichik fayl):

```bash
docker exec $OLD_PG pg_dump -U postgres -d thingsboard --schema-only -Fc > ~/backup/schema.dump
pg_restore -h 127.0.0.1 -p 15432 -U postgres -d thingsboard ~/backup/schema.dump
```

**Qadam 2 — data-only dump, `ts_kv` datasisiz** (schema allaqachon bor) — pipe, oraliq faylsiz:

```bash
docker exec $OLD_PG pg_dump -U postgres -d thingsboard --data-only -Fc --exclude-table-data='ts_kv*' > ~/backup/nontskv.dump
pg_restore -h 127.0.0.1 -p 15432 -U postgres -d thingsboard ~/backup/nontskv.dump
```

Bu `ts_kv` parent + barcha child partition datalarini tashlab ketadi, lekin `ts_kv_latest`, `ts_kv_dictionary`, barcha entity jadvallari va sequences datalarini oladi.

**Qadam 3 — tekshiruv** (yangi PG'da):

```bash
pg_restore -h 127.0.0.1 -p 15432 -U postgres -d thingsboard -l ~/backup/nontskv.dump | grep -c "TABLE DATA"
docker exec $OLD_PG psql -U postgres -d thingsboard -t -c "SELECT count(*) FROM ts_kv_dictionary;"
psql -h 127.0.0.1 -p 15432 -U postgres -d thingsboard -t -c "SELECT count(*) FROM ts_kv_dictionary;"
psql -h 127.0.0.1 -p 15432 -U postgres -d thingsboard -t -c "SELECT count(*) FROM ts_kv;"   # 0 bo'lishi kerak
```

> Delta-izoh: nusxa paytida eski TB ishlayotgani uchun entity/latest jadvallarga ozgina yozuv tushishi mumkin. Switchover paytida (eski TB stop qilingan) kichik jadvallar (`ts_kv_latest`, `ts_kv_dictionary`) bir marta qayta dump/restore qilinadi (tez, MB'lar darajasi). Katta jadvallar uchun takror shart emas.

## 9. Partition migratsiya

Barcha `tbmigrator` buyruqlari `screen -S migration` ichida, `~/projects/TB_DB_Migrator/` papkasida bajariladi. Progress `migration_progress.json` ga har batch'dan so'ng yoziladi.

### 9.1 Partition ro'yxati

Eng yangi partition'ni aniqlash (spec 6.1):

```bash
dotnet bin/Release/net10.0/tbmigrator.dll list-partitions
```

Natija: nom, MinTs/MaxTs (ISO), Count, Size MB. `MaxTs` eng katta bo'lgani — oxirgi (hot) partition (masalan `ts_kv_2026_09`).

### 9.2 Birinchi — oxirgi partition

Eski TB hali ishlayapti — hot partition'ga yangi yozuvlar tushishda davom etadi.

**Qadam 1 — dump** (pipe orqali to'g'ridan `~/backup/` ga, oraliq `/tmp` faylsiz):

```bash
docker exec $OLD_PG pg_dump -U postgres -d thingsboard -Fc -t <part> > ~/backup/<part>.dump
ls -lh ~/backup/<part>.dump
```

**Qadam 2 — migrate** (faqat shu partition):

```bash
dotnet bin/Release/net10.0/tbmigrator.dll start --partition <part> --workers 2
```

**Qadam 3 — delta-pass:** birinchi pass tugagach, pass oralig'ida kelgan yozuvlarni ko'chirish (`MaxTs` ni checkpoint'dan oling):

```bash
dotnet bin/Release/net10.0/tbmigrator.dll start --partition <part> --delta-from <pass1_max_ts> --workers 2
```

**Qadam 4 — verify** (10-bo'lim):

```bash
dotnet bin/Release/net10.0/tbmigrator.dll verify --partition <part>
```

O'tmasa — DROP yo'q, log tahlil qilinadi. Verify o'tgach ham bu partition hali DROP qilinmaydi (eski TB hali yozmoqda — 9.5-bo'lim).

### 9.3 Switchover + yangi TB tekshiruvi

> **Bu bosqich downtime beradi.** Foydalanuvchilar vaqtincha yangi TB ga kira olmaydi.

**Qadam 1 — eski TB ni to'xtatish** (shu paytdan eski PG frozen):

```bash
sudo systemctl stop thingsboard
```

**Qadam 2 — kichik jadvallar deltasini qayta nusxalash** (`ts_kv_latest`, `ts_kv_dictionary` — tez, MB'lar darajasi):

```bash
docker exec $OLD_PG pg_dump -U postgres -d thingsboard --data-only -Fc -t ts_kv_latest -t ts_kv_dictionary > ~/backup/latest-delta.dump
pg_restore -h 127.0.0.1 -p 15432 -U postgres -d thingsboard ~/backup/latest-delta.dump
```

**Qadam 3 — hot partition'ning so'nggi deltasini ko'chirish:**

```bash
dotnet bin/Release/net10.0/tbmigrator.dll start --partition <part> --delta-from <last_max_ts> --workers 2
dotnet bin/Release/net10.0/tbmigrator.dll verify --partition <part>
```

**Qadam 4 — yangi TB ni yoqish** (license env oldindan tayyor bo'lishi kerak):

```bash
export TB_LICENSE_SECRET='...'
export TB_LICENSE_INSTANCE_DATA_FILE='...'
docker compose -f docker-compose.new-stack.yml --profile tb up -d tb-pe
docker logs -f tb-pe-new
# "ThingsBoard started" ko'ringanda tayyor
```

**Qadam 5 — to'liq tekshiruv (Hammasi + API):**

```bash
export TB_URL=http://localhost:8080
export TB_USER='...' TB_PASS='...'
export DEVICE_ID='...'   # migrate qilingan telemetry'li device
export START_TS='...'    # oxirgi partition min(ts), ms
bash scripts/tb-api-check.sh
# ALL_CHECKS_PASSED chiqishi kerak
```

Qo'lda ham: login (UI + `POST /api/auth/login` 200), dashboard ochilishi, device ro'yxati (yangi PG'dan), latest telemetry (`ts_kv_latest_cf` dan), history grafik oxirgi partition oralig'ida (`ts_kv_cf` dan, count>0).

Tekshiruv o'tsa — 9.4 ga. O'tmasa — rollback (14-bo'lim): yangi TB stop, eski TB start.

### 9.4 Qolgan partitionlar (yangidan-eskiga)

Har bir partition uchun sikl (eski TB o'chiq, eski PG frozen — delta shart emas):

```bash
# 1. Dump (pipe orqali)
docker exec $OLD_PG pg_dump -U postgres -d thingsboard -Fc -t <part> > ~/backup/<part>.dump

# 2. Migrate
dotnet bin/Release/net10.0/tbmigrator.dll start --partition <part> --workers 2

# 3. Verify (count + sample)
dotnet bin/Release/net10.0/tbmigrator.dll verify --partition <part>

# 4. Verify o'tsa — DROP (10-bo'lim gate)
dotnet bin/Release/net10.0/tbmigrator.dll drop --partition <part> --dump-file ~/backup/<part>.dump --verified

# 5. Bo'shagan joyni qayd etish
df -h /
```

Tartib yangidan-eskiga: yangi TB history so'rovlari avval yangi datalarni ko'radi.

### 9.5 Hot partition DROP qoidasi

Oxirgi partition faqat switchover tekshiruvi (9.3-5-qadam) o'tgandan keyin DROP qilinadi. Sabab: eski TB stop bo'lguncha unga yozuv tushadi; erta DROP data-loss beradi.

---

## 10. Verify + DROP safety gate

DROP — eng xavfli operatsiya. Qoida (istisnosiz):

1. `~/backup/<part>.dump` mavjud va `pg_restore --list ~/backup/<part>.dump` bilan o'qiladi (operator tekshiradi — tool shell out qilmaydi).
2. `verify` o'tgan: `pg_count == scylla_count` VA random sample (default 1000 qator: entity_id+key+ts bo'yicha PG vs Scylla qiymat solishtirish) 100% mos.
3. Operator `drop` ni `--verified` bilan tasdiqlagan.
4. Hot (oxirgi) partition uchun qo'shimcha: switchover tekshiruvi (9.3-5) o'tgan bo'lishi shart.

`DROP` emas `DETACH`: `DROP TABLE <part>` — joy darhol OS'ga qaytadi. `DETACH PARTITION` data'ni saqlab qoladi — joy bo'shamaydi, maqsadga zid.

`drop` uchala shartdan biri bajarilmasa rad etadi (xabar + exit 1) — hech qanday holatda verify'siz DROP bo'lmaydi.

---

## 11. Konfiguratsiya

`config.yaml` fayli barcha ulanish va migratsiya parametrlarini o'z ichiga oladi. Muhit o'zgaruvchilari (`PG_HOST`, `SCYLLA_HOST` va boshqalar) `config.yaml` dagi qiymatlarni ustidan yozadi.

```yaml
pg:
  host: localhost        # PG_HOST env o'zgaruvchisi ustidan yozadi (ESKI pg, :5432)
  port: 5432             # PG_PORT
  db: thingsboard        # PG_DB
  user: postgres         # PG_USER
  password: postgres     # PG_PASSWORD

scylla:
  host: localhost        # SCYLLA_HOST
  port: 9042             # SCYLLA_PORT
  keyspace: thingsboard  # SCYLLA_KEYSPACE

migrator:
  batch_size: 5000
  workers: 4
  scylla_concurrency: 64
  live_sync_interval: 5.0
  lag_threshold_ms: 30000
  partitioning: MONTHS
  cast_strings: false
  checkpoint_file: migration_progress.json
  partition_batch: 5000
  verify_sample_size: 1000
```

> Tavsiya (8 GB RAM, Scylla 1 GB limit): `workers: 2`, `scylla_concurrency: 32` bilan boshlang.

### Parametrlar jadvali

| Parametr | Standart | Tavsif |
|----------|----------|--------|
| `pg.host` | `localhost` | Eski PostgreSQL manzili (migrator faqat eskidan o'qiydi) |
| `pg.port` | `5432` | Eski PostgreSQL port |
| `pg.db` | `thingsboard` | Ma'lumotlar bazasi nomi |
| `pg.user` | `postgres` | PostgreSQL foydalanuvchi |
| `pg.password` | `postgres` | PostgreSQL parol |
| `scylla.host` | `localhost` | ScyllaDB server manzili |
| `scylla.port` | `9042` | ScyllaDB CQL port |
| `scylla.keyspace` | `thingsboard` | ScyllaDB keyspace nomi |
| `migrator.batch_size` | `5000` | Entity-key rejimida bir so'rovdagi qatorlar soni |
| `migrator.partition_batch` | `5000` | Partition rejimida bir so'rovdagi qatorlar soni |
| `migrator.verify_sample_size` | `1000` | Verify'da tasodifiy sample qatorlar soni (min 1) |
| `migrator.workers` | `4` | Parallel worker soni (`--workers N` bilan override; tavsiya: 2) |
| `migrator.scylla_concurrency` | `64` | ScyllaDB ga parallel yozish limiti (tavsiya: 32) |
| `migrator.live_sync_interval` | `5.0` | Live sync polling oralig'i (soniya, eski entity-key rejimi uchun) |
| `migrator.lag_threshold_ms` | `30000` | Switchover uchun ruxsat etilgan maksimal lag (ms) |
| `migrator.partitioning` | `MONTHS` | Partition strategiyasi: `MONTHS`, `DAYS`, `HOURS`, `YEARS`, `MINUTES`, `INDEFINITE` |
| `migrator.cast_strings` | `false` | `str_v` ni `long_v`/`dbl_v` ga aylantirish |
| `migrator.checkpoint_file` | `migration_progress.json` | Checkpoint fayli yo'li |

### Partition strategiyalari

| Qiymat | Tavsif | Qachon ishlatish |
|--------|--------|-----------------|
| `MONTHS` | Har oy alohida partition (standart, rasmiy tool bilan bir xil) | Ko'p hollarda mos |
| `DAYS` | Har kun alohida partition | Yuqori yozish tezligi bo'lganda |
| `HOURS` | Har soat alohida partition | Juda yuqori yozish tezligi bo'lganda |
| `MINUTES` | Har daqiqa alohida partition | Test uchun |
| `YEARS` | Har yil alohida partition | Kam yozish tezligi bo'lganda |
| `INDEFINITE` | Bitta partition, partition yo'q | Kam ma'lumot bo'lganda |

> **Muhim:** `TS_KV_PARTITIONING` ThingsBoard env o'zgaruvchisi `migrator.partitioning` bilan bir xil bo'lishi shart.

---

## 12. Checkpoint va resume

Migrator progress ni `migration_progress.json` fayliga saqlaydi (har bir batch'dan so'ng + partition holati: `partitions: {<part>: {state, pg_count, scylla_count, dump_file, verified, dropped, max_ts}}`). Agar migratsiya to'xtasa, `--resume` bilan davom ettirish mumkin (Scylla INSERT'lar idempotent upsert — takror yozish zararsiz).

### Partition resume

```bash
# To'xtagan partition'ni davom ettirish (MaxTs checkpoint'da saqlangan)
dotnet bin/Release/net10.0/tbmigrator.dll start --partition <part> --resume --workers 2

# Delta'dan boshlash (allaqachon ko'chgan qatorlarni o'tkazib yuborish)
dotnet bin/Release/net10.0/tbmigrator.dll start --partition <part> --delta-from <max_ts> --workers 2
```

### Holat tekshirish

```bash
dotnet bin/Release/net10.0/tbmigrator.dll status
dotnet bin/Release/net10.0/tbmigrator.dll list-partitions
cat ~/projects/TB_DB_Migrator/migration_progress.json
```

### Checkpoint faylini o'chirish (noldan boshlash)

```bash
rm -f ~/projects/TB_DB_Migrator/migration_progress.json
```

---

## 13. Xatoliklarni ko'rish

Barcha xato va ogohlantirishlar konsol (stderr) ga yoziladi — `screen -r migration` orqali ko'ring.

### Tez-tez uchraydigan xatolar

| Xato | Sabab | Yechim |
|------|-------|--------|
| `Connection refused` (PostgreSQL) | `PG_HOST` noto'g'ri yoki eski PG host'dan ko'rinmaydi | `PG_HOST`/`PG_PORT` ni tekshiring; eski PG 5432 porti ochiq bo'lishi shart |
| `Connection refused` (ScyllaDB) | ScyllaDB hali tayyor emas | ScyllaDB `healthy` bo'lguncha kuting (`docker compose -f docker-compose.new-stack.yml ps`) |
| `Keyspace ... does not exist` | Schema yaratilmagan | `dotnet bin/Release/net10.0/tbmigrator.dll init-schema` ni bajaring (yoki `start` o'zi yaratadi) |
| `Unknown partition` | `--partition` nomi xato | `list-partitions` bilan to'g'ri nomni oling |
| `Count mismatch` (verify) | Delta yozuvlar kelgan yoki yozish tugamagan | Delta-pass (`--delta-from`) ni qayta bajaring, keyin `verify` ni takrorlang |
| `Refusing to drop` | Gate sharti bajarilmagan | Xabardagi sababni o'qing: dump fayl, `verify`, `--verified` (10-bo'lim) |
| `Out of memory` | ScyllaDB/TB ga RAM yetishmayapti | `free -h`; limitlar 3-bo'limdagi byudjetga mosligini tekshiring |
| `Timeout` / sekin yozish | Yuk oshib ketgan (Scylla 1 GB limitda) | `workers: 2`, `scylla_concurrency: 32` bilan boshlang |

### ScyllaDB ichida tekshirish

```bash
docker exec -it scylladb-new cqlsh

# cqlsh ichida:
USE thingsboard;
SELECT * FROM ts_kv_cf LIMIT 10;
```

---

## 14. Rollback

- Har qanday partition verify'siz DROP qilinmaydi — dump (`~/backup/<part>.dump`) + eski PG'da data bor.
- Switchover tekshiruvi o'tmasa: `docker compose -f docker-compose.new-stack.yml --profile tb stop tb-pe`, keyin `sudo systemctl start thingsboard` — eski stack joyida.
- Yangi PG buzilsa: `~/backup/schema.dump` + `~/backup/nontskv.dump` dan qayta restore (8-bo'lim).
- DROP qilingan partition kerak bo'lsa: `pg_restore -h 127.0.0.1 -p 15432 -U postgres -d thingsboard ~/backup/<part>.dump` (yangi PG'ga) — lekin eski PG'ga qaytarish root joyini yana to'ldiradi, faqat favqulodda.

---

## 15. Muhim eslatmalar

### Faqat timeseries ScyllaDB ga ko'chiriladi

**Migrator ScyllaDB ga faqat quyidagilarni yozadi:**
- `ts_kv` partitionlari → ScyllaDB `ts_kv_cf` (+ `ts_kv_partitions_cf`)
- Eski entity-key rejimi (`start` partitionsiz) qo'shimcha `ts_kv_latest` → `ts_kv_latest_cf` ni ham ko'chiradi

**Yangi PostgreSQL da qoladi (ko'chirilmaydi, `pg_dump` bilan nusxalanadi):**
- Qurilmalar, mijozlar, aktivlar va boshqa entitylar (`device`, `asset`, `customer`, ...)
- `ts_kv_latest`, `ts_kv_dictionary`, atributlar (`attribute_kv`)
- Alarmlar, qoidalar, dashboardlar va boshqa konfiguratsiya ma'lumotlari

Bu ThingsBoard ning mo'ljallangan arxitekturasi: entities va attributes — PostgreSQL, timeseries — Cassandra/ScyllaDB.

### Ishonchli yozish (data-loss yo'q)

Migrator har bir INSERT ni alohida, lekin parallel (`scylla_concurrency`, tavsiya 32) yuboradi — bu ScyllaDB uchun to'g'ri usul. `WriteTimeoutException` va shunga o'xshash vaqtinchalik xatolar exponential backoff bilan 6 marta qayta uriniladi, shuning uchun timeout paytida ham hech qanday qator yo'qolmaydi. Scylla INSERT'lar idempotent upsert — resume'da takror yozish zararsiz.

### Tez o'qish (keyset pagination)

PostgreSQL dan o'qish `LIMIT/OFFSET` o'rniga `(ts, entity_id, key)` bo'yicha keyset pagination ishlatadi — to'g'ridan child partition jadvalidan (`SELECT ... FROM "<part>"`), parent scan emas. Bu yuz millionlab qatorlarda ham O(n) tezlikni saqlaydi.

### Resurslar (8 GB RAM byudjeti)

`docker-compose.new-stack.yml` dagi limitlar (3-bo'lim): `tb-pe` 3g, `scylladb` 1g (`--smp 1 --memory 512M --overprovisioned 1`), `postgres-new` 512m. Eski `docker-compose.scylla.yml` (cheklovsiz) bu rejimda ishlatilmaydi.

### screen ishlatish majburiy

SSH orqali ishlayotganda internet uzilishi yoki terminal yopilishi migratsiyani to'xtatib qo'yishi mumkin. Shuning uchun `screen` (yoki `tmux`) ichida ishlash **majburiy**:

```bash
screen -S migration          # sessiya ochish
# Ctrl+A, keyin D            # chiqish (migratsiya davom etadi)
screen -r migration          # qaytish
screen -r -d migration       # "Attached" bo'lsa majburiy ochish
```

### TTL va PostgreSQL ni tozalash

Agar ThingsBoard da `SQL_TTL_TS_*` yoki shunga o'xshash TTL parametrlari `docker-compose.yml` da yozilgan bo'lsa, switchoverdan keyin ularni ko'rib chiqing. ScyllaDB o'zining TTL mexanizmiga ega.

### Migratsiya tugagandan keyin

Switchover muvaffaqiyatli bo'lgandan so'ng yangi ThingsBoard (docker, `tb-pe-new`) to'liq cassandra rejimida ishlaydi. Eski RPM TB o'chiq turadi — bir necha kun kuzating va hammasi yaxshi ishlayotganiga ishonch hosil qiling. Shundan keyingina eski stack'ni tozalashingiz mumkin (eski RPM service'ni o'chirish qo'lda, kuzatuvdan keyin).

---

*TB_DB_Migrator — BlueStar loyihasi uchun ishlab chiqilgan. ThingsBoard 3.4.1 PE bilan ishlashga mo'ljallangan (`tb-3.4` branch). Rasmiy migrator (`thingsboard/release-3.4`, `tools/.../migrator`) offline SSTable usulida ishlaydi — bu vosita online CQL usulida ishlaydi.*

## Runbook (qisqa, spec 13-bo'lim)

```bash
export OLD_PG=$(docker ps --format '{{.Names}}' | grep -i postgres | head -1)
mkdir -p ~/backup
docker volume create tb-pg-new-data
docker volume create tb-scylla-data
docker compose -f docker-compose.new-stack.yml up -d postgres-new scylladb
# schema + non-ts_kv (8-bo'lim)
dotnet bin/Release/net10.0/tbmigrator.dll list-partitions
# oxirgi partition: dump -> start -> delta -> verify (9.2)
sudo systemctl stop thingsboard
# latest/dictionary delta + hot delta (9.3), tb-pe up, full+API check
# qolganlar: dump -> start -> verify -> drop (9.4, yangidan-eskiga)
```
