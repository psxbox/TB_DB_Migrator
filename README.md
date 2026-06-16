# TB_DB_Migrator — ThingsBoard PostgreSQL → ScyllaDB ko'chirish vositasi

> **Versiya:** 1.1 | **ThingsBoard:** 4.x CE | **Til:** O'zbek (Latin)
>
> **Ishlash modeli:** ScyllaDB — Docker'da, migrator (Python) — host (remote Linux) mashinada to'g'ridan-to'g'ri.

---

## Mundarija

1. [Kirish](#1-kirish)
2. [Arxitektura](#2-arxitektura)
3. [Talablar](#3-talablar)
4. [Fayllarni remote serverga yuborish](#4-fayllarni-remote-serverga-yuborish)
5. [Migratsiya bosqichlari](#5-migratsiya-bosqichlari)
   - [5.1 Mavjud TB stack holatini tekshirish](#51-mavjud-tb-stack-holatini-tekshirish)
   - [5.2 ScyllaDB ni Docker'da ko'tarish](#52-scylladb-ni-dockerda-kotarish)
   - [5.3 Python muhitini tayyorlash](#53-python-muhitini-tayyorlash)
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

### Key dictionary (TB 4.x)

ThingsBoard CE 4.x da kalitlar lug'ati jadvali `key_dictionary` deb nomlanadi (eski versiyalarda `ts_kv_dictionary`). Migrator avtomatik ravishda avval `key_dictionary` ni, keyin `ts_kv_dictionary` ni sinab ko'radi. Agar ikkalasi ham bo'lmasa (toza-SQL rejimi), `ts_kv.key` ustunini to'g'ridan-to'g'ri ishlatadi.

---

## 2. Arxitektura

```
REMOTE LINUX SERVER
═══════════════════════════════════════════════════════════════════

  ┌─────────────────────── Docker ───────────────────────┐
  │                                                       │
  │  ┌──────────────┐   ┌───────────────┐                 │
  │  │ thingsboard  │   │   postgres    │                 │
  │  │  tb-node CE  │◄──│   :5432       │                 │
  │  │   4.x        │   │  (TB stack)   │                 │
  │  └──────────────┘   └───────┬───────┘                 │
  │                             │                         │
  │  ┌──────────────┐           │                         │
  │  │  scylladb     │ 127.0.0.1:9042                      │
  │  │  (alohida     │           │                         │
  │  │   compose)    │           │                         │
  │  └──────▲───────┘           │                         │
  └─────────┼────────────────────┼─────────────────────────┘
            │ yozish (CQL)        │ o'qish (SQL)
            │                     │
       ┌────┴─────────────────────┴────┐
       │   HOST PYTHON (venv)           │
       │   python main.py start         │
       │   migrator/                    │
       └────────────────────────────────┘
```

**Ishlash modeli:** ScyllaDB Docker konteynerida ishlaydi va CQL porti (`9042`) host'ga ochiladi. Migrator esa host (remote Linux) mashinasida to'g'ridan-to'g'ri Python orqali ishlaydi — PostgreSQL dan o'qiydi, ScyllaDB ga yozadi. Bu Docker image build qilish zaruratini yo'q qiladi va loglarni to'g'ridan-to'g'ri ko'rsatadi.

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
| Python | 3.9+ | 3.11+ |
| RAM | **4 GB** | 8 GB+ |
| Disk (ScyllaDB data) | PostgreSQL `ts_kv` hajmiga teng | 2x hajm (xavfsizlik uchun) |
| CPU | 2 yadro | 4+ yadro |

### Tekshirish buyruqlari

```bash
# Docker versiyasini tekshirish
docker --version
docker compose version

# Python versiyasini tekshirish
python3 --version

# Mavjud RAMni ko'rish
free -h

# Disk joyini ko'rish
df -h
```

> **Diqqat:** `docker compose` (v2, plugin) ishlatiladi — `docker-compose` (v1, standalone) emas.

---

## 4. Fayllarni remote serverga yuborish

Barcha migratsiya kodlari **local PC dan remote serverga** ko'chiriladi.

### rsync orqali yuborish

```bash
# Local PC da bajarish:
rsync -avz --exclude='.git' --exclude='__pycache__' --exclude='*.pyc' --exclude='.venv' \
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
# docker-compose.scylla.yml  config.yaml
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

> **Muhim:** Migrator host'da ishlagani uchun PostgreSQL host'dan ko'rinishi kerak. Agar TB postgres konteyneri 5432 portni host'ga ochmagan bo'lsa:
> - postgres konteyneriga `ports: ["127.0.0.1:5432:5432"]` qo'shing, **yoki**
> - `PG_HOST` ni postgres konteynerining IP manziliga sozlang (`docker inspect`).

### 5.2 ScyllaDB ni Docker'da ko'tarish

Migratsiya papkasiga o'ting va ScyllaDB ni ishga tushiring:

```bash
cd /opt/tb-migrator

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

### 5.3 Python muhitini tayyorlash

Host'da virtual muhit yarating va kutubxonalarni o'rnating (bir marta):

```bash
cd /opt/tb-migrator

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
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

> Schema (keyspace + jadvallar) avtomatik yaratiladi — `start` buyrug'i ishga tushganda `init-schema` ni o'zi chaqiradi. Alohida bajarish ham mumkin: `python main.py init-schema`.

### 5.5 Migratsiyani screen ichida ishga tushirish

> **Muhim:** SSH ulanishi uzilsa, migratsiya to'xtamasligi uchun `screen` (yoki `tmux`) ichida ishga tushiring.

Yangi `screen` sessiyasi oching va venv ni faollashtiring:

```bash
screen -S migration

cd /opt/tb-migrator
source .venv/bin/activate
```

Migratsiyani ishga tushiring:

```bash
python main.py start
```

**Screen dan chiqish (migratsiya davom etishi bilan):** `Ctrl+A`, keyin `D`

**Screen ga qaytish:** `screen -r migration`

**Barcha screen sessiyalarini ko'rish:** `screen -ls`

#### Qo'shimcha parametrlar bilan ishga tushirish

Faqat historical ma'lumotlarni ko'chirish (live sync va switchover yo'q):

```bash
python main.py start --historical-only
```

String qiymatlarni raqamga aylantirish bilan:

```bash
python main.py start --cast-strings
```

Partition strategiyasini o'zgartirish bilan (standart: MONTHS):

```bash
python main.py start --partitioning DAYS
```

### 5.6 Progress kuzatish

#### status buyrug'i orqali

Boshqa SSH sessiyasida (venv faollashtirilgan holda):

```bash
cd /opt/tb-migrator
source .venv/bin/activate
python main.py status
```

Natija ko'rinishi:

```
          Migration Status
 ──────────────────────────────────────
  Phase          │ phase1
  Started at     │ 2026-01-15 10:23:45
  Partitioning   │ MONTHS
  Cast strings   │ False
  Migrated rows  │ 1,234,567
  Skipped rows   │ 42
  Last entity ID │ 550e8400-e29b-41d4-...
  Watermark      │ 2026-01-15 09:58:12 UTC
```

> `Migrated rows` har bir batch (5000 qator) yozilgandan so'ng yangilanadi — katta entity ko'chayotganda ham hisoblagich o'sib boradi.

#### Log fayli orqali

```bash
# Oxirgi loglarni ko'rish
tail -n 50 /opt/tb-migrator/migration_errors.log

# Real vaqtda kuzatish
tail -f /opt/tb-migrator/migration_errors.log
```

`screen -r migration` orqali jonli konsol chiqishini ham ko'rish mumkin.

#### Live Sync fazasini kuzatish

Phase 1 (historical) tugagandan so'ng, migrator avtomatik ravishda Phase 2 (live sync) ga o'tadi. Bu paytda `lag` ko'rsatkichi < 30 soniyaga tushishi kutiladi. Lag < 30 soniyaga tushganda, migrator switchover uchun tayyor ekanligi haqida xabar beradi.

### 5.7 Switchover — ThingsBoard ni cassandra rejimiga o'tkazish

> **Bu bosqich ~60 soniya downtime beradi.** Foydalanuvchilar vaqtincha ThingsBoard ga kira olmaydi.

Switchover paytida ThingsBoard konteyneri ScyllaDB ga ulanishi kerak. ScyllaDB host'ning `127.0.0.1` portiga bind qilingani uchun, uni ThingsBoard ning Docker tarmog'iga ulash kerak.

**Qadam 1: ScyllaDB ni ThingsBoard tarmog'iga ulash**

```bash
# ThingsBoard tarmog'i nomini aniqlash
docker network ls | grep thingsboard
# masalan: tb_ce_new_default

# ScyllaDB konteynerini shu tarmoqqa ulash
docker network connect tb_ce_new_default scylladb
```

Endi ThingsBoard konteyneri `scylladb:9042` orqali ulana oladi.

**Qadam 2: ThingsBoard ni to'xtatish**

```bash
cd /opt/thingsboard
docker compose stop thingsboard-ce
```

**Qadam 3: docker-compose.yml ni tahrirlash**

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

**Qadam 4: ThingsBoard ni qayta ishga tushirish**

```bash
cd /opt/thingsboard
docker compose up -d thingsboard-ce
```

**Qadam 5: Loglar orqali muvaffaqiyatli ishga tushishini tasdiqlash**

```bash
docker logs -f thingsboard-ce | grep -i "started\|error\|cassandra"
```

Cassandra bilan muvaffaqiyatli ulanganda quyidagicha log ko'rinadi:

```
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

Migrator progress ni `migration_progress.json` fayliga saqlaydi (har bir batch va entity dan so'ng). Agar migratsiya to'xtasa (server qayta ishga tushsa, xato bo'lsa, vaqtinchalik uzilish bo'lsa), `--resume` bayrog'i bilan davom ettirish mumkin.

### Checkpoint fayli

```bash
cat /opt/tb-migrator/migration_progress.json
```

### Davom ettirish

```bash
screen -r migration
# yoki yangi screen sessiyasi:
screen -S migration
cd /opt/tb-migrator && source .venv/bin/activate

python main.py start --resume
```

### Holat tekshirish

```bash
python main.py status
```

`Last entity ID` maydoni — oxirgi muvaffaqiyatli ko'chirilgan entity. Resume paytida migrator shu nuqtadan davom etadi.

### Checkpoint faylini o'chirish (noldan boshlash)

```bash
rm -f /opt/tb-migrator/migration_progress.json
python main.py start
```

---

## 8. Xatoliklarni ko'rish

Barcha xato va ogohlantirishlar `migration_errors.log` fayliga (host'da, ishchi papkada) yoziladi.

### Log faylini ko'rish

```bash
cat /opt/tb-migrator/migration_errors.log

# Oxirgi 100 qatorni ko'rish
tail -n 100 /opt/tb-migrator/migration_errors.log

# Real vaqtda kuzatish
tail -f /opt/tb-migrator/migration_errors.log
```

### Tez-tez uchraydigan xatolar

| Xato | Sabab | Yechim |
|------|-------|--------|
| `Connection refused` (PostgreSQL) | `PG_HOST` noto'g'ri yoki PG host'dan ko'rinmaydi | postgres 5432 ni host'ga oching yoki `PG_HOST` ni konteyner IP ga sozlang |
| `Connection refused` (ScyllaDB) | ScyllaDB hali tayyor emas | ScyllaDB `healthy` bo'lguncha kuting (`docker compose -f docker-compose.scylla.yml ps`) |
| `Keyspace ... does not exist` | Schema yaratilmagan | `python main.py init-schema` ni bajaring (yoki `start` qayta yaratadi) |
| `ModuleNotFoundError` | venv faollashtirilmagan | `source .venv/bin/activate` |
| `Out of memory` | ScyllaDB ga RAM yetishmayapti | Serverda bo'sh RAM ni tekshiring (`free -h`) |
| `Timeout` / sekin yozish | Yuk oshib ketgan | `config.yaml` da `batch_size` ni kamaytiring |

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

Migrator har bir INSERT ni alohida, lekin parallel (`execute_concurrent`, concurrency 32) yuboradi — bu ScyllaDB uchun to'g'ri usul. Xato bergan qatorlar faqat o'zi qayta urinib ko'riladi (exponential backoff bilan), shuning uchun timeout paytida ham hech qanday qator yo'qolmaydi.

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

*TB_DB_Migrator — BlueStar loyihasi uchun ishlab chiqilgan. ThingsBoard CE 4.x bilan sinovdan o'tkazilgan.*
