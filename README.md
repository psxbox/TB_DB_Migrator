# TB_DB_Migrator — ThingsBoard PostgreSQL → ScyllaDB ko'chirish vositasi

> **Versiya:** 2.0 (`tb-3.4` branch) | **ThingsBoard:** 3.4.1 PE | **Til:** O'zbek (Latin)
>
> **Ishlash modeli:** ScyllaDB — Docker'da, migrator (.NET 10) — host (remote Linux) mashinada to'g'ridan-to'g'ri.

---

## Mundarija

1. [Kirish](#1-kirish)
2. [Arxitektura](#2-arxitektura)
3. [Talablar](#3-talablar)
4. [Loyihani serverga olish (git clone)](#4-loyihani-serverga-olish-git-clone)
5. [Migratsiya bosqichlari](#5-migratsiya-bosqichlari)
   - [5.1 Mavjud TB stack holatini tekshirish](#51-mavjud-tb-stack-holatini-tekshirish)
   - [5.2 ScyllaDB ni Docker'da ko'tarish](#52-scylladb-ni-dockerda-kotarish)
   - [5.3 .NET muhitini tayyorlash](#53-net-muhitini-tayyorlash)
   - [5.4 Ulanishlarni sozlash](#54-ulanishlarni-sozlash)
   - [5.5 Migratsiyani screen ichida ishga tushirish](#55-migratsiyani-screen-ichida-ishga-tushirish)
   - [5.6 Progress kuzatish](#56-progress-kuzatish)
   - [5.7 Switchover — ThingsBoard ni cassandra rejimiga o'tkazish](#57-switchover--thingsboard-ni-cassandra-rejimiga-otkazish)
   - [5.8 Migratorni to'xtatish](#58-migratorni-toxtatish)
6. [Konfiguratsiya](#6-konfiguratsiya)
7. [Checkpoint va resume](#7-checkpoint-va-resume)
8. [Xatoliklarni ko'rish](#8-xatoliklarni-korish)
9. [Muhim eslatmalar](#9-muhim-eslatmalar)

---

## 1. Kirish

**TB_DB_Migrator** — ThingsBoard PE ning vaqt seriyali ma'lumotlarini (timeseries) PostgreSQL ma'lumotlar bazasidan ScyllaDB ga ko'chirish uchun mo'ljallangan amaliy vosita.

### Nima qiladi?

- PostgreSQL dagi `ts_kv` va `ts_kv_latest` jadvallaridan barcha timeseries qatorlarini o'qiydi
- ScyllaDB dagi ThingsBoard Cassandra-formatidagi jadvallarga yozadi
- Migratsiya davomida ThingsBoard ishlashda davom etadi (downtime yo'q)
- Faqat **switchover** paytida ~60 soniya to'xtash bo'ladi

### Qachon ishlatiladi?

- ThingsBoard yuklama o'sganda va PostgreSQL timeseries yozuvlari millionlab qatorga yetganda
- ScyllaDB ga o'tib, yozish/o'qish tezligini va gorizontal masshtablashni yaxshilash kerak bo'lganda
- PostgreSQL da saqlash hajmi muammo bo'lganda

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

ThingsBoard `release-3.4` dagi rasmiy tool (`tools/.../migrator`: `MigratorTool`, `PgCaMigrator`, `DictionaryParser`, `RelatedEntitiesParser`, `WriterBuilder`) **offline** ishlaydi: `pg_dump` → SSTable generatsiya (`CQLSSTableWriter`) → fayllarni qo'lda `data/thingsboard` ga ko'chirish → `nodetool compact` → hybrid rejimga o'tish. Bu vosita esa **online** ishlaydi: PostgreSQL dan to'g'ridan-to'g'ri o'qiydi, ScyllaDB ga CQL orqali yozadi, TB ishlayotgan paytda live-sync qiladi. Entity ro'yxati, `ts_kv_dictionary` formati, partition (oy boshi, UTC) va `str_v → dbl_v` cast mantig'i rasmiy tool bilan bir xil.

---

## 2. Arxitektura

```
REMOTE LINUX SERVER
═══════════════════════════════════════════════════════════════════

  ┌─────────────────────── Docker ───────────────────────┐
  │                                                       │
  │  ┌───────────────┐   ┌──────────────┐                 │
  │  │   postgres    │   │   scylladb   │                 │
  │  │    :5432      │   │    :9042     │                 │
  │  │  (TB stack)   │   │  (alohida    │                 │
  │  └───────┬───────┘   │   compose)   │                 │
  │          │           └──────▲───────┘                 │
  └──────────┼──────────────────┼─────────────────────────┘
             │ o'qish (SQL)      │ yozish (CQL)
             │                   │
  ┌──────────┴───────────────────┴────┐
  │  HOST (to'g'ridan-to'g'ri)         │
  │  ┌─────────────────────────────┐  │
  │  │ ThingsBoard PE 3.4.1        │  │
  │  │ (service, systemctl)        │  │
  │  └─────────────────────────────┘  │
  │  ┌─────────────────────────────┐  │
  │  │ Migrator (.NET 10)          │  │
  │  │ ~/projects/TB_DB_Migrator/  │  │
  │  │ dotnet tbmigrator.dll start │  │
  │  └─────────────────────────────┘  │
  └───────────────────────────────────┘
```

**Ishlash modeli:** PostgreSQL (Docker) va ScyllaDB (Docker, alohida compose) portlari host'ga ochilgan. ThingsBoard host OS ga to'g'ridan-to'g'ri o'rnatilgan (service). Migrator ham host'da .NET orqali ishlaydi — PostgreSQL dan o'qiydi (`localhost:5432`), ScyllaDB ga yozadi (`localhost:9042`). `dotnet publish -c Release` bilan bitta papka olinadi, loglar to'g'ridan-to'g'ri ko'rinadi.

**Migratsiya fazalari:**

| Faza | Nomi | Tavsif |
|------|------|--------|
| **0** | Preload | Entity map va key map yuklash (~bir necha soniya) |
| **1** | Historical | Barcha mavjud `ts_kv` qatorlarini ko'chirish |
| **2** | Live Sync | TB ishlayotgan paytda yangi qatorlarni real vaqtda sinxronlashtirish |
| **—** | Switchover | Lag < 30 s bo'lganda TB ni to'xtatib, cassandra rejimida qayta ishga tushirish |

---

## 3. Talablar

### Remote server (migratsiya bajariluvchi server)

| Talab | Minimal | Tavsiya |
|-------|---------|---------|
| OS | Linux (Ubuntu 20.04+) | Ubuntu 22.04 LTS |
| Docker | 24.0+ | so'nggi versiya |
| Docker Compose | v2 (plugin) | v2.20+ |
| .NET SDK | 10.0+ | so'nggi versiya |
| RAM | **4 GB** | 8 GB+ |
| Disk (ScyllaDB data) | PostgreSQL `ts_kv` hajmiga teng | 2x hajm (xavfsizlik uchun) |
| CPU | 2 yadro | 4+ yadro |

### Tekshirish buyruqlari

```bash
# Docker versiyasini tekshirish
docker --version
docker compose version

# .NET SDK versiyasini tekshirish
dotnet --version

# Mavjud RAMni ko'rish
free -h

# Disk joyini ko'rish
df -h
```

> **Diqqat:** `docker compose` (v2, plugin) ishlatiladi — `docker-compose` (v1, standalone) emas.

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

## 5. Migratsiya bosqichlari

Barcha quyidagi buyruqlar **remote serverda** bajariladi (SSH orqali kirgandan keyin).

### 5.1 Mavjud TB stack holatini tekshirish

Migratsiyadan oldin PostgreSQL (Docker) va ThingsBoard (host'da service) ishlayotganini tasdiqlang:

```bash
# PostgreSQL konteyneri
docker ps --filter name=postgres --format '{{.Names}} {{.Status}}'

# ThingsBoard service (host'da o'rnatilgan)
sudo systemctl status thingsboard --no-pager | head -5
```

```bash
# PostgreSQL ga ulanib ts_kv jadvalini tekshirish
docker exec -it postgres psql -U postgres -d thingsboard -c \
  "SELECT COUNT(*) FROM ts_kv;"
docker exec -it postgres psql -U postgres -d thingsboard -c \
  "SELECT COUNT(*) FROM ts_kv_dictionary;"
```

Agar jadvallar mavjud va qatorlar bor bo'lsa, migratsiyaga tayyor. `ts_kv.key` — integer (`key_id` ga FK), `ts_kv` — `RANGE (ts)` bo'yicha partitsiyalangan jadval.

> **Muhim:** Migrator host'da ishlagani uchun PostgreSQL host'dan ko'rinishi kerak. Postgres Docker'da bo'lgani uchun 5432 port host'ga ochiq bo'lishi shart:
> - `docker ps` da `0.0.0.0:5432->5432` yoki `127.0.0.1:5432->5432` ko'rinishi kerak;
> - bo'lmasa postgres compose'ga `ports: ["127.0.0.1:5432:5432"]` qo'shing.

### 5.2 ScyllaDB ni Docker'da ko'tarish

Migratsiya papkasiga o'ting va ScyllaDB ni ishga tushiring:

```bash
cd ~/projects/TB_DB_Migrator

# External volume bir marta yaratiladi (ma'lumotlar compose'dan tashqarida saqlanadi)
docker volume create thingsboard-scylla-data

docker compose -f docker-compose.scylla.yml up -d
```

ScyllaDB tayyor (healthy) bo'lguncha kuting (30–90 soniya):

```bash
# Healthcheck holatini kuzatish
docker compose -f docker-compose.scylla.yml ps

# STATUS ustuni "healthy" bo'lishi kerak
```

Loglarni ko'rish:

```bash
docker logs -f scylladb
# "Scylla version ... initialization completed" ko'ringanda tayyor
```

> CQL porti `127.0.0.1:9042` ga bind qilingan — faqat host'dan (localhost) ulanish mumkin, tashqaridan emas.

### 5.3 .NET muhitini tayyorlash

Host'da .NET SDK o'rnating va loyihani build qiling (bir marta):

```bash
cd ~/projects/TB_DB_Migrator

dotnet --version   # 10.0+ bo'lishi kerak
dotnet build TbMigrator.csproj -c Release
```

Yoki publish qilib bitta papka oling:

```bash
dotnet publish TbMigrator.csproj -c Release -o ~/projects/TB_DB_Migrator/publish
cd ~/projects/TB_DB_Migrator/publish
```

### 5.4 Ulanishlarni sozlash

`config.yaml` standart holatda `localhost` ga sozlangan. Agar PostgreSQL va ScyllaDB shu mashinada portlari host'ga ochiq bo'lsa, o'zgartirish shart emas.

Aks holda muhit o'zgaruvchilari bilan override qiling (`config.yaml` qiymatlari ustidan yoziladi):

```bash
export PG_HOST=127.0.0.1        # yoki postgres konteyner IP
export PG_PORT=5432
export PG_DB=thingsboard
export PG_USER=postgres
export PG_PASSWORD=postgres
export SCYLLA_HOST=127.0.0.1
export SCYLLA_PORT=9042
export SCYLLA_KEYSPACE=thingsboard
```

> Schema (keyspace + jadvallar) avtomatik yaratiladi — `start` buyrug'i ishga tushganda `init-schema` ni o'zi chaqiradi. Alohida bajarish ham mumkin: `dotnet bin/Release/net10.0/tbmigrator.dll init-schema`.

### 5.5 Migratsiyani screen ichida ishga tushirish

> **Muhim:** SSH ulanishi uzilsa, migratsiya to'xtamasligi uchun `screen` (yoki `tmux`) ichida ishga tushiring.

Yangi `screen` sessiyasi oching:

```bash
screen -S migration

cd ~/projects/TB_DB_Migrator
```

Migratsiyani ishga tushiring:

```bash
dotnet bin/Release/net10.0/tbmigrator.dll start
```

**Screen dan chiqish (migratsiya davom etishi bilan):** `Ctrl+A`, keyin `D`

**Screen ga qaytish:** `screen -r migration`

**Barcha screen sessiyalarini ko'rish:** `screen -ls`

#### Qo'shimcha parametrlar bilan ishga tushirish

Faqat historical ma'lumotlarni ko'chirish (live sync yo'q):

```bash
dotnet bin/Release/net10.0/tbmigrator.dll start --historical-only
```

Worker sonini o'zgartirish bilan:

```bash
dotnet bin/Release/net10.0/tbmigrator.dll start --workers 8
```

### 5.6 Progress kuzatish

#### status buyrug'i orqali

Boshqa SSH sessiyasida:

```bash
cd ~/projects/TB_DB_Migrator
dotnet bin/Release/net10.0/tbmigrator.dll status
```

Natija ko'rinishi:

```text
┌────────────────────┬──────────────────────────────┐
│ Field              │ Value                        │
├────────────────────┼──────────────────────────────┤
│ Phase              │ phase1                       │
│ Started At         │ 2026-01-15T10:23:45+00:00    │
│ Migrated Rows      │ 1,234,567                    │
│ Skipped Rows       │ 42                           │
│ Completed Entities │ 128                          │
│ Last Entity        │ 550e8400-e29b-41d4-...       │
│ Partitioning       │ MONTHS                       │
│ Cast Strings       │ False                        │
└────────────────────┴──────────────────────────────┘
```

> `Migrated rows` har bir batch (5000 qator) yozilgandan so'ng yangilanadi — katta entity ko'chayotganda ham hisoblagich o'sib boradi.

#### Konsol loglari orqali

Migrator barcha holat (`[INFO]`, `[WARN]`, `[STATUS]`) xabarlarini stderr ga yozadi:

```bash
# screen sessiyasiga qaytib jonli logni ko'rish
screen -r migration
```

`screen -r migration` orqali jonli konsol chiqishini ham ko'rish mumkin.

#### Live Sync fazasini kuzatish

Phase 1 (historical) tugagandan so'ng, migrator avtomatik ravishda Phase 2 (live sync) ga o'tadi. Bu paytda `lag` ko'rsatkichi < 30 soniyaga tushishi kutiladi. Lag < 30 soniyaga tushganda, migrator switchover uchun tayyor ekanligi haqida xabar beradi.

### 5.7 Switchover — ThingsBoard ni cassandra rejimiga o'tkazish

> **Bu bosqich ~60 soniya downtime beradi.** Foydalanuvchilar vaqtincha ThingsBoard ga kira olmaydi.

ThingsBoard host OS ga to'g'ridan-to'g'ri o'rnatilgan, ScyllaDB esa Docker'da `127.0.0.1:9042` ga bind qilingan — shuning uchun TB `localhost:9042` orqali to'g'ridan-to'g'ri ulana oladi, Docker tarmog'ini ulash shart emas.

**Qadam 1: ThingsBoard ni to'xtatish**

```bash
sudo systemctl stop thingsboard
```

**Qadam 2: `/etc/thingsboard/conf/thingsboard.conf` ni tahrirlash**

Quyidagi qatorlarni qo'shing yoki yangilang:

```bash
export DATABASE_TS_TYPE=cassandra
export TS_KV_PARTITIONING=MONTHS
export CASSANDRA_URL=127.0.0.1:9042
export CASSANDRA_CLUSTER_NAME="TB Cluster"
export CASSANDRA_USE_CREDENTIALS=false
export CASSANDRA_KEYSPACE_NAME=thingsboard
```

> **Diqqat:** `TS_KV_PARTITIONING` migratsiyada ishlatilgan partition strategiyasiga mos bo'lishi kerak (standart: `MONTHS` — rasmiy tool ham shuni ishlatadi).

**Qadam 3: ThingsBoard ni qayta ishga tushirish**

```bash
sudo systemctl start thingsboard
```

**Qadam 4: Loglar orqali muvaffaqiyatli ishga tushishini tasdiqlash**

```bash
sudo journalctl -u thingsboard -f | grep -i "started\|error\|cassandra"
# yoki
tail -f /var/log/thingsboard/thingsboard.log | grep -i "started\|error\|cassandra"
```

Cassandra bilan muvaffaqiyatli ulanganda quyidagicha log ko'rinadi:

```text
... ThingsBoard started in X seconds
```

### 5.8 Migratorni to'xtatish

Switchover muvaffaqiyatli bo'lgandan va ThingsBoard cassandra rejimida ishlayotganini tasdiqlaganingizdan so'ng, migratorni to'xtating:

```bash
# screen sessiyasiga qaytib, Ctrl+C bilan to'xtatish
screen -r migration
# Ctrl+C

# yoki screen sessiyasini butunlay yopish
screen -X -S migration quit
```

ScyllaDB konteyneri ishlashda davom etadi (ThingsBoard endi undan foydalanadi).

---

## 6. Konfiguratsiya

`config.yaml` fayli barcha ulanish va migratsiya parametrlarini o'z ichiga oladi. Muhit o'zgaruvchilari (`PG_HOST`, `SCYLLA_HOST` va boshqalar) `config.yaml` dagi qiymatlarni ustidan yozadi.

```yaml
pg:
  host: localhost        # PG_HOST env o'zgaruvchisi ustidan yozadi
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
```

### Parametrlar jadvali

| Parametr | Standart | Tavsif |
|----------|----------|--------|
| `pg.host` | `localhost` | PostgreSQL server manzili |
| `pg.port` | `5432` | PostgreSQL port |
| `pg.db` | `thingsboard` | Ma'lumotlar bazasi nomi |
| `pg.user` | `postgres` | PostgreSQL foydalanuvchi |
| `pg.password` | `postgres` | PostgreSQL parol |
| `scylla.host` | `localhost` | ScyllaDB server manzili |
| `scylla.port` | `9042` | ScyllaDB CQL port |
| `scylla.keyspace` | `thingsboard` | ScyllaDB keyspace nomi |
| `migrator.batch_size` | `5000` | Bir so'rovda o'qiladigan/yoziladigan qatorlar soni |
| `migrator.workers` | `4` | Parallel entity worker soni (`--workers N` bilan override) |
| `migrator.scylla_concurrency` | `64` | ScyllaDB ga parallel yozish limiti |
| `migrator.live_sync_interval` | `5.0` | Live sync polling oralig'i (soniya) |
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

## 7. Checkpoint va resume

Migrator progress ni `migration_progress.json` fayliga saqlaydi (har bir batch va entity dan so'ng). Agar migratsiya to'xtasa (server qayta ishga tushsa, xato bo'lsa, vaqtinchalik uzilish bo'lsa), `--resume` bayrog'i bilan davom ettirish mumkin.

### Checkpoint fayli

```bash
cat ~/projects/TB_DB_Migrator/migration_progress.json
```

### Davom ettirish

```bash
screen -S migration
cd ~/projects/TB_DB_Migrator

dotnet bin/Release/net10.0/tbmigrator.dll start --resume
```

### Holat tekshirish

```bash
dotnet bin/Release/net10.0/tbmigrator.dll status
```

`Last Entity` maydoni — oxirgi muvaffaqiyatli ko'chirilgan entity. Resume paytida migrator shu nuqtadan davom etadi.

### Checkpoint faylini o'chirish (noldan boshlash)

```bash
rm -f ~/projects/TB_DB_Migrator/migration_progress.json
dotnet bin/Release/net10.0/tbmigrator.dll start
```

---

## 8. Xatoliklarni ko'rish

Barcha xato va ogohlantirishlar konsol (stderr) ga yoziladi — `screen -r migration` orqali ko'ring.

### Tez-tez uchraydigan xatolar

| Xato | Sabab | Yechim |
|------|-------|--------|
| `Connection refused` (PostgreSQL) | `PG_HOST` noto'g'ri yoki PG host'dan ko'rinmaydi | postgres 5432 ni host'ga oching yoki `PG_HOST` ni konteyner IP ga sozlang |
| `Connection refused` (ScyllaDB) | ScyllaDB hali tayyor emas | ScyllaDB `healthy` bo'lguncha kuting (`docker compose -f docker-compose.scylla.yml ps`) |
| `Keyspace ... does not exist` | Schema yaratilmagan | `dotnet bin/Release/net10.0/tbmigrator.dll init-schema` ni bajaring (yoki `start` o'zi yaratadi) |
| `Out of memory` | ScyllaDB ga RAM yetishmayapti | Serverda bo'sh RAM ni tekshiring (`free -h`) |
| `Timeout` / sekin yozish | Yuk oshib ketgan | `config.yaml` da `batch_size` ni kamaytiring yoki `scylla_concurrency` ni tushiring |

### ScyllaDB ichida tekshirish

```bash
docker exec -it scylladb cqlsh

# cqlsh ichida:
USE thingsboard;
SELECT * FROM ts_kv_cf LIMIT 10;
```

---

## 9. Muhim eslatmalar

### Faqat timeseries ko'chiriladi

**TB_DB_Migrator faqat quyidagi jadvallarni ko'chiradi:**
- `ts_kv` → ScyllaDB `ts_kv_cf` (+ `ts_kv_partitions_cf`)
- `ts_kv_latest` → ScyllaDB `ts_kv_latest_cf`

**Quyidagilar PostgreSQL da qoladi (ko'chirilmaydi):**
- Qurilmalar, mijozlar, aktivlar va boshqa entitylar (`device`, `asset`, `customer`, ...)
- Atributlar (`attribute_kv`)
- Alarmlar, qoidalar, dashboardlar va boshqa konfiguratsiya ma'lumotlari

Bu ThingsBoard ning mo'ljallangan arxitekturasi: entities va attributes — PostgreSQL, timeseries — Cassandra/ScyllaDB.

### Ishonchli yozish (data-loss yo'q)

Migrator har bir INSERT ni alohida, lekin parallel (`scylla_concurrency`, standart 64) yuboradi — bu ScyllaDB uchun to'g'ri usul. `WriteTimeoutException` va shunga o'xshash vaqtinchalik xatolar exponential backoff bilan 6 marta qayta uriniladi, shuning uchun timeout paytida ham hech qanday qator yo'qolmaydi.

### Tez o'qish (keyset pagination)

PostgreSQL dan o'qish `LIMIT/OFFSET` o'rniga birlamchi kalit (primary key) indeksi bo'yicha keyset pagination ishlatadi — bu yuz millionlab qatorlarda ham O(n) tezlikni saqlaydi.

### ScyllaDB resurslari

`docker-compose.scylla.yml` da resurs cheklovi (`--smp`, `--memory`) ko'rsatilmagan — ScyllaDB mavjud barcha CPU va RAM dan foydalanadi. Agar server boshqa servislar bilan bo'lishilsa, `command:` orqali cheklash mumkin, masalan:

```yaml
    command: --smp 2 --memory 4G --overprovisioned 1
```

Serverda kamida **4 GB** bo'sh RAM bo'lishi tavsiya etiladi (`free -h`).

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

Switchover muvaffaqiyatli bo'lgandan so'ng ThingsBoard to'liq cassandra rejimida ishlaydi. PostgreSQL dagi `ts_kv` jadvalini **darhol o'chirmang** — bir necha kun kuzating va hammasi yaxshi ishlayotganiga ishonch hosil qiling. Shundan keyingina eski ma'lumotlarni PostgreSQL dan tozalashingiz mumkin.

---

*TB_DB_Migrator — BlueStar loyihasi uchun ishlab chiqilgan. ThingsBoard 3.4.1 PE bilan ishlashga mo'ljallangan (`tb-3.4` branch). Rasmiy migrator (`thingsboard/release-3.4`, `tools/.../migrator`) offline SSTable usulida ishlaydi — bu vosita online CQL usulida ishlaydi.*
