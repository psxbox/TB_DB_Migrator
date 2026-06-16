# TB_DB_Migrator — ThingsBoard PostgreSQL → ScyllaDB ko'chirish vositasi

> **Versiya:** 1.0 | **ThingsBoard:** 4.3.1.1 CE | **Til:** O'zbek (Latin)

---

## Mundarija

1. [Kirish](#1-kirish)
2. [Arxitektura](#2-arxitektura)
3. [Talablar](#3-talablar)
4. [Fayllarni remote serverga yuborish](#4-fayllarni-remote-serverga-yuborish)
5. [Migratsiya bosqichlari](#5-migratsiya-bosqichlari)
   - [5.1 Mavjud TB stack holatini tekshirish](#51-mavjud-tb-stack-holatini-tekshirish)
   - [5.2 ScyllaDB va migrator servislarini ko'tarish](#52-scylladb-va-migrator-servislarini-kotarish)
   - [5.3 ScyllaDB schema yaratish](#53-scylladb-schema-yaratish)
   - [5.4 Migratsiyani screen ichida ishga tushirish](#54-migratsiyani-screen-ichida-ishga-tushirish)
   - [5.5 Progress kuzatish](#55-progress-kuzatish)
   - [5.6 Switchover — ThingsBoard ni cassandra rejimiga o'tkazish](#56-switchover--thingsboard-ni-cassandra-rejimiga-otkazish)
   - [5.7 Migratorni to'xtatish](#57-migratorni-toxtatish)
6. [Konfiguratsiya](#6-konfiguratsiya)
7. [Checkpoint va resume](#7-checkpoint-va-resume)
8. [Xatoliklarni ko'rish](#8-xatoliklarni-korish)
9. [Muhim eslatmalar](#9-muhim-eslatmalar)

---

## 1. Kirish

**TB_DB_Migrator** — ThingsBoard CE ning vaqt seriyali ma'lumotlarini (timeseries) PostgreSQL ma'lumotlar bazasidan ScyllaDB ga ko'chirish uchun mo'ljallangan amaliy vosita.

### Nima qiladi?

- PostgreSQL dagi `ts_kv` va `ts_kv_latest` jadvallaridan barcha timeseries qatorlarini o'qiydi
- ScyllaDB dagi ThingsBoard Cassandra-formatidagi jadvallarga yozadi
- Migratsiya davomida ThingsBoard ishlashda davom etadi (downtime yo'q)
- Faqat **switchover** paytida ~60 soniya to'xtash bo'ladi

### Qachon ishlatiladi?

- ThingsBoard yuklama o'sganda va PostgreSQL timeseries yozuvlari millionlab qatorga yetganda
- ScyllaDB ga o'tib, yozish/o'qish tezligini va gorizontal masshtablashni yaxshilash kerak bo'lganda
- PostgreSQL da saqlash hajmi muammo bo'lganda

---

## 2. Arxitektura

```
LOCAL PC                         REMOTE SERVER (Docker)
──────────                       ──────────────────────────────────────────────
                                 ┌─────────────────────────────────────────┐
                                 │            Docker network               │
                                 │                                         │
rsync / scp                      │  ┌──────────────┐   ┌───────────────┐  │
──────────────────────────────►  │  │  thingsboard  │   │   postgres    │  │
  (kod fayllarini yuborish)      │  │  tb-node CE   │◄──│   :5432       │  │
                                 │  │  4.3.1.1      │   │  (internal)   │  │
                                 │  └──────────────┘   └───────┬───────┘  │
                                 │                             │           │
                                 │  ┌──────────────┐           │           │
                                 │  │  tb-migrator  │◄──────────┘           │
                                 │  │  (Python)     │                       │
                                 │  └──────┬───────┘                       │
                                 │         │ yozish                        │
                                 │  ┌──────▼───────┐                       │
                                 │  │  scylladb     │                       │
                                 │  │  :9042        │                       │
                                 │  └──────────────┘                       │
                                 └─────────────────────────────────────────┘
```

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
| RAM (ScyllaDB uchun) | **4 GB** (2 GB ScyllaDB + 2 GB boshqa servislar) | 8 GB+ |
| Disk (ScyllaDB data) | PostgreSQL `ts_kv` hajmiga teng | 2x hajm (xavfsizlik uchun) |
| CPU | 2 yadro | 4+ yadro |

### Tekshirish buyruqlari

```bash
# Docker versiyasini tekshirish
docker --version
docker compose version

# Mavjud RAMni ko'rish
free -h

# Disk joyini ko'rish
df -h
```

> **Diqqat:** `docker compose` (v2, plugin) ishlatiladi — `docker-compose` (v1, standalone) emas.

---

## 4. Fayllarni remote serverga yuborish

Barcha migratsiya kodlari **local PC dan remote serverga** ko'chiriladi. Docker image pull va pip install operatsiyalari remote serverda bajariladi.

### rsync orqali yuborish

```bash
# Local PC da bajarish:
rsync -avz --exclude='.git' --exclude='__pycache__' --exclude='*.pyc' \
  ./TB_DB_Migrator/ \
  user@remote-server:/opt/tb-migrator/
```

### scp orqali yuborish (alternativa)

```bash
# Arxivlab yuborish:
tar -czf tb_migrator.tar.gz TB_DB_Migrator/
scp tb_migrator.tar.gz user@remote-server:/opt/
ssh user@remote-server "cd /opt && tar -xzf tb_migrator.tar.gz"
```

### Remote serverda papkani tekshirish

```bash
ssh user@remote-server
ls /opt/tb-migrator/
# Ko'rinishi kerak:
# docker-compose.scylla.yml  Dockerfile  config.yaml
# requirements.txt  main.py  migrator/
```

---

## 5. Migratsiya bosqichlari

Barcha quyidagi buyruqlar **remote serverda** bajariladi (SSH orqali kirgandan keyin).

### 5.1 Mavjud TB stack holatini tekshirish

Migratsiyadan oldin mavjud ThingsBoard stack ishlayotganini tasdiqlang:

```bash
# TB papkasiga o'tish (docker-compose.yml joylashgan joy)
cd /opt/thingsboard

# Servislar holatini tekshirish
docker compose ps

# Kutilgan natija:
# NAME              STATUS
# postgres          Up
# thingsboard-ce    Up
```

```bash
# PostgreSQL ga ulanib ts_kv jadvalini tekshirish
docker exec -it postgres psql -U postgres -d thingsboard -c \
  "SELECT COUNT(*) FROM ts_kv;"
```

Agar jadval mavjud va qatorlar bor bo'lsa, migratsiyaga tayyor.

### 5.2 ScyllaDB va migrator servislarini ko'tarish

Migratsiya papkasiga o'ting (fayllar yuborilgan joy):

```bash
cd /opt/tb-migrator
```

TB stack ning `docker-compose.yml` fayli bilan overlay faylimizni birga ishlatib, ScyllaDB va migrator servislarini ishga tushiring:

```bash
docker compose \
  -f /opt/thingsboard/docker-compose.yml \
  -f /opt/tb-migrator/docker-compose.scylla.yml \
  up -d scylladb tb-migrator
```

ScyllaDB tayyor bo'lguncha kuting (30–90 soniya):

```bash
# Healthcheck holatini kuzatish
docker compose \
  -f /opt/thingsboard/docker-compose.yml \
  -f /opt/tb-migrator/docker-compose.scylla.yml \
  ps scylladb

# Holat "healthy" bo'lishi kerak
```

Loglarni ko'rish:

```bash
docker logs -f scylladb
# "Scylla version ... initialization completed" ko'ringanda tayyor
```

### 5.3 ScyllaDB schema yaratish

ScyllaDB tayyor bo'lgandan so'ng, ThingsBoard uchun zarur keyspace va jadvallarni yarating:

```bash
docker exec -it tb-migrator python main.py init-schema
```

Muvaffaqiyatli natija:

```
╭──────────────────────────────────────╮
│   Initializing ScyllaDB schema...    │
╰──────────────────────────────────────╯
✅ Schema created successfully!
   Keyspace : thingsboard
   Tables   : ts_kv_cf, ts_kv_partitions_cf, ts_kv_latest_cf
```

Schema to'g'ri yaratilganini cqlsh orqali tekshirish:

```bash
docker exec -it scylladb cqlsh -e "DESCRIBE KEYSPACE thingsboard;"
```

### 5.4 Migratsiyani screen ichida ishga tushirish

> **Muhim:** SSH ulanishi uzilsa, migratsiya to'xtamasligi uchun `screen` ichida ishga tushiring.

Yangi `screen` sessiyasi oching:

```bash
screen -S migration
```

`screen` ichida migratsiyani ishga tushiring:

```bash
docker exec -it tb-migrator python main.py start
```

**Screen dan chiqish (migratsiya davom etishi bilan):**

```
Ctrl+A, keyin D
```

**Screen ga qaytish:**

```bash
screen -r migration
```

**Barcha screen sessiyalarini ko'rish:**

```bash
screen -ls
```

#### Qo'shimcha parametrlar bilan ishga tushirish

Faqat historical ma'lumotlarni ko'chirish (live sync va switchover yo'q):

```bash
docker exec -it tb-migrator python main.py start --historical-only
```

String qiymatlarni raqamga aylantirish bilan:

```bash
docker exec -it tb-migrator python main.py start --cast-strings
```

Partition strategiyasini o'zgartirish bilan (standart: MONTHS):

```bash
docker exec -it tb-migrator python main.py start --partitioning DAYS
```

### 5.5 Progress kuzatish

#### status buyrug'i orqali

Yangi terminal oynasida (yoki boshqa SSH sessiyasida):

```bash
docker exec -it tb-migrator python main.py status
```

Natija ko'rinishi:

```
          Migration Status
 ──────────────────────────────────────
  Phase          │ historical
  Started at     │ 2026-01-15 10:23:45
  Partitioning   │ MONTHS
  Cast strings   │ False
  Migrated rows  │ 1,234,567
  Skipped rows   │ 42
  Last entity ID │ 550e8400-e29b-41d4-...
  Watermark      │ 2026-01-15 09:58:12 UTC
```

#### docker logs orqali

```bash
# Oxirgi loglarni ko'rish
docker logs --tail 50 tb-migrator

# Real vaqtda loglarni kuzatish
docker logs -f tb-migrator
```

#### Live Sync fazasini kuzatish

Phase 1 (historical) tugagandan so'ng, migrator avtomatik ravishda Phase 2 (live sync) ga o'tadi. Bu paytda `lag` ko'rsatkichi < 30 soniyaga tushishi kutiladi:

```bash
docker logs -f tb-migrator | grep -i "lag\|sync\|watermark"
```

Lag < 30 soniyaga tushganda, migrator switchover uchun tayyor ekanligi haqida xabar beradi.

### 5.6 Switchover — ThingsBoard ni cassandra rejimiga o'tkazish

> **Bu bosqich ~60 soniya downtime beradi.** Foydalanuvchilar vaqtincha ThingsBoard ga kira olmaydi.

Switchover uchun quyidagi ketma-ketlikni to'liq va tez bajaring:

**Qadam 1: ThingsBoard ni to'xtatish**

```bash
cd /opt/thingsboard
docker compose stop thingsboard-ce
```

**Qadam 2: docker-compose.yml ni tahrirlash**

`thingsboard-ce` servisining `environment` bo'limiga quyidagi o'zgaruvchilarni qo'shing:

```yaml
services:
  thingsboard-ce:
    environment:
      # ... mavjud o'zgaruvchilar ...
      DATABASE_TS_TYPE: cassandra
      TS_KV_PARTITIONING: MONTHS
      CASSANDRA_URL: scylladb:9042
      CASSANDRA_CLUSTER_NAME: TB Cluster
      CASSANDRA_USE_CREDENTIALS: "false"
      CASSANDRA_KEYSPACE_NAME: thingsboard
```

> **Diqqat:** `TS_KV_PARTITIONING` migratsiyada ishlatilgan partition strategiyasiga mos bo'lishi kerak (standart: `MONTHS`).

**Qadam 3: ThingsBoard ni overlay bilan qayta ishga tushirish**

```bash
docker compose \
  -f /opt/thingsboard/docker-compose.yml \
  -f /opt/tb-migrator/docker-compose.scylla.yml \
  up -d thingsboard-ce
```

**Qadam 4: ThingsBoard loglari orqali muvaffaqiyatli ishga tushishini tasdiqlash**

```bash
docker logs -f thingsboard-ce | grep -i "started\|error\|cassandra"
```

Cassandra bilan muvaffaqiyatli ulanganda quyidagicha log ko'rinadi:

```
... ThingsBoard started in X seconds
```

### 5.7 Migratorni to'xtatish

Switchover muvaffaqiyatli bo'lgandan va ThingsBoard cassandra rejimida ishlayotganini tasdiqlaganingizdan so'ng, migratorni to'xtating:

```bash
docker compose \
  -f /opt/thingsboard/docker-compose.yml \
  -f /opt/tb-migrator/docker-compose.scylla.yml \
  stop tb-migrator
```

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
| `migrator.batch_size` | `5000` | Bir so'rovda o'qiladigan qatorlar soni |
| `migrator.live_sync_interval` | `5.0` | Live sync polling oralig'i (soniya) |
| `migrator.lag_threshold_ms` | `30000` | Switchover uchun ruxsat etilgan maksimal lag (ms) |
| `migrator.partitioning` | `MONTHS` | Partition strategiyasi: `MONTHS`, `DAYS`, `HOURS`, `INDEFINITE` |
| `migrator.cast_strings` | `false` | `str_v` ni `long_v`/`dbl_v` ga aylantirish |
| `migrator.checkpoint_file` | `migration_progress.json` | Checkpoint fayli yo'li |

### Partition strategiyalari

| Qiymat | Tavsif | Qachon ishlatish |
|--------|--------|-----------------|
| `MONTHS` | Har oy alohida partition (standart) | Ko'p hollarda mos |
| `DAYS` | Har kun alohida partition | Yuqori yozish tezligi bo'lganda |
| `HOURS` | Har soat alohida partition | Juda yuqori yozish tezligi bo'lganda |
| `INDEFINITE` | Bitta partition, partition yo'q | Kam ma'lumot bo'lganda |

> **Muhim:** `TS_KV_PARTITIONING` ThingsBoard env o'zgaruvchisi `migrator.partitioning` bilan bir xil bo'lishi shart.

---

## 7. Checkpoint va resume

Migrator har bir entity (qurilma) ko'chirilganidan so'ng progress ni `migration_progress.json` fayliga saqlaydi. Agar migratsiya to'xtasa (server qayta ishga tushsa, xato bo'lsa, vaqtinchalik uzilish bo'lsa), `--resume` bayrog'i bilan davom ettirish mumkin.

### Checkpoint fayli

```bash
# Checkpoint fayl mazmunini ko'rish
docker exec -it tb-migrator cat migration_progress.json
```

### Davom ettirish

```bash
screen -r migration
# yoki yangi screen sessiyasida:
screen -S migration

docker exec -it tb-migrator python main.py start --resume
```

### Holat tekshirish

```bash
docker exec -it tb-migrator python main.py status
```

`Last entity ID` maydoni — oxirgi muvaffaqiyatli ko'chirilgan entity. Resume paytida migrator shu nuqtadan davom etadi.

### Checkpoint faylini o'chirish (noldan boshlash)

Agar migratsiyani noldan boshlash kerak bo'lsa:

```bash
docker exec -it tb-migrator rm -f migration_progress.json
docker exec -it tb-migrator python main.py start
```

---

## 8. Xatoliklarni ko'rish

Barcha xato va ogohlantirishlar `migration_errors.log` fayliga yoziladi.

### Log faylini ko'rish

```bash
# Container ichidan
docker exec -it tb-migrator cat migration_errors.log

# Oxirgi 100 qatorni ko'rish
docker exec -it tb-migrator tail -n 100 migration_errors.log

# Real vaqtda kuzatish
docker exec -it tb-migrator tail -f migration_errors.log
```

### Tez-tez uchraydigan xatolar

| Xato | Sabab | Yechim |
|------|-------|--------|
| `Connection refused` (PostgreSQL) | `PG_HOST` noto'g'ri yoki postgres ishlamayapti | `docker ps` bilan postgres holatini tekshiring |
| `Connection refused` (ScyllaDB) | ScyllaDB hali tayyor emas | ScyllaDB `healthy` bo'lguncha kuting |
| `Keyspace not found` | `init-schema` bajarilmagan | `python main.py init-schema` ni qayta bajaring |
| `Out of memory` | ScyllaDB ga RAM yetishmayapti | Serverda bo'sh RAM ni tekshiring (`free -h`) |
| `Timeout` | Yuk oshib ketgan | `batch_size` ni kamaytiring (`config.yaml`) |

### ScyllaDB ichida tekshirish

```bash
# ScyllaDB ga cqlsh orqali kirib, ma'lumotlarni tekshirish
docker exec -it scylladb cqlsh

# cqlsh ichida:
USE thingsboard;
SELECT COUNT(*) FROM ts_kv_cf LIMIT 1000000;
SELECT * FROM ts_kv_cf LIMIT 10;
```

---

## 9. Muhim eslatmalar

### Faqat timeseries ko'chiriladi

**TB_DB_Migrator faqat quyidagi jadvallarni ko'chiradi:**
- `ts_kv` → ScyllaDB `ts_kv_cf`
- `ts_kv_latest` → ScyllaDB `ts_kv_latest_cf`

**Quyidagilar PostgreSQL da qoladi (ko'chirilmaydi):**
- Qurilmalar, mijozlar, aktivlar va boshqa entitylar (`device`, `asset`, `customer`, ...)
- Atributlar (`attribute_kv`)
- Alarmlar, qoidalar, dashboardlar va boshqa konfiguratsiya ma'lumotlari

Bu ThingsBoard ning mo'ljallangan arxitekturasi: entities va attributes — PostgreSQL, timeseries — Cassandra/ScyllaDB.

### ScyllaDB RAM talabi

Docker container uchun minimal **2 GB RAM** ajratilgan (`--memory 2G`). Server da kamida **4 GB** bo'sh RAM bo'lishi kerak (ScyllaDB + boshqa servislar uchun). Kam RAM da ScyllaDB OOM (Out of Memory) xatosi beradi va to'xtaydi.

```bash
# RAM holatini tekshirish
free -h
# "available" ustuni ≥ 3 GB bo'lishi kerak
```

### screen ishlatish majburiy

SSH orqali ishlayotganda internet uzilishi yoki terminal yopilishi migratsiyani to'xtatib qo'yishi mumkin. Shuning uchun `screen` (yoki `tmux`) ichida ishlash **majburiy**:

```bash
# Sessiya ochish
screen -S migration

# Sessiyadan chiqish (migratsiya davom etadi)
Ctrl+A, keyin D

# Sessiyaga qaytish
screen -r migration

# Agar sessiya "Attached" bo'lsa, majburiy ochish
screen -r -d migration
```

### TTL va PostgreSQL ni tozalash

Agar ThingsBoard da `sql.ttl.ts.ts_key_value_ttl` yoki shunga o'xshash TTL parametrlari `docker-compose.yml` da yozilgan bo'lsa, switchoverdan keyin ularni **olib tashlang**. ScyllaDB o'zining TTL mexanizmiga ega va PostgreSQL TTL parametrlari ScyllaDB ga ta'sir qilmaydi — lekin eski parametrlar ThingsBoard konfiguratsiyasida chalkashlik yaratishi mumkin.

```yaml
# docker-compose.yml dan OLIB TASHLASH kerak:
# SQL_TTL_TS_TS_KEY_VALUE_TTL: "0"
# (yoki shunga o'xshash TTL o'zgaruvchilari)
```

### Overlay fayllar va tarmoq

Migrator `docker-compose.scylla.yml` overlay fayli ThingsBoard ning asosiy `docker-compose.yml` bilan birga ishlatiladi. Bu ikki faylni birga ko'rsatmasdan, servislar bir xil Docker tarmog'ida ko'rinmaydi:

```bash
# TO'G'RI: ikkala fayl ko'rsatilgan
docker compose \
  -f /opt/thingsboard/docker-compose.yml \
  -f /opt/tb-migrator/docker-compose.scylla.yml \
  up -d

# NOTO'G'RI: faqat overlay fayl — postgres ko'rinmaydi
docker compose -f /opt/tb-migrator/docker-compose.scylla.yml up -d
```

### Migratsiya tugagandan keyin

Switchover muvaffaqiyatli bo'lgandan so'ng ThingsBoard to'liq cassandra rejimida ishlaydi. PostgreSQL dagi `ts_kv` jadvalini **darhol o'chirmang** — bir necha kun kuzating va hammasi yaxshi ishlayotganiga ishonch hosil qiling. Shundan keyingina eski ma'lumotlarni PostgreSQL dan tozalashingiz mumkin.

---

*TB_DB_Migrator — BlueStar loyihasi uchun ishlab chiqilgan. ThingsBoard 4.3.1.1 CE bilan sinovdan o'tkazilgan.*
