# Snowflake Nesting Analysis — sf/sf_bq Instances

## Summary

| Metric | Count | % |
|---|---|---|
| **Total sf/sf_bq instances** | 207 | 100% |
| ✅ Uses **ONLY flat tables (or missing SQL)** | 112 | **54.1%** |
| ⚠️ Uses **≥1 nested table** (VARIANT/ARRAY) | 95 | **45.9%** |

**Nearly half (95/207) of all Snowflake questions directly query tables with nested types.**

---

## Nesting Breakdown (95 nested instances)

| Subcategory | Count |
|---|---|
| ALL gold tables are nested | 66 |
| Mix of nested + flat | 29 |

---

## Per-Database Breakdown (95 Nested Instances)

### CRYPTO

| Stat | Value |
|---|---|
| Total tables | 39 |
| Flat tables | 16 |
| Nested tables | 23 |
| Instances using nested tables | 15 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq005 | BLOCKS | — |
| sf_bq057 | TRANSACTIONS | — |
| sf_bq065 | ORACLE_REQUESTS | — |
| sf_bq068 | TRANSACTIONS | — |
| sf_bq083 | TRANSACTIONS | — |
| sf_bq092 | TRANSACTIONS | — |
| sf_bq093 | BLOCKS, TRANSACTIONS | TRACES |
| sf_bq135 | TRANSACTIONS, TRANSITIONS | — |
| sf_bq136 | TRANSACTIONS, TRANSITIONS | — |
| sf_bq195 | BLOCKS, TRANSACTIONS | TRACES |
| sf_bq256 | BLOCKS, TRANSACTIONS | TRACES |
| sf_bq292 | TRANSACTIONS | — |
| sf_bq334 | INPUTS, OUTPUTS, TRANSACTIONS | — |
| sf_bq335 | INPUTS, OUTPUTS | — |
| sf_bq444 | LOGS | — |

### ETHEREUM_BLOCKCHAIN

| Stat | Value |
|---|---|
| Total tables | 7 |
| Flat tables | 5 |
| Nested tables | 2 |
| Instances using nested tables | 1 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq450 | CONTRACTS | BLOCKS, TOKEN_TRANSFERS, TRACES, TRANSACTIONS |

### GENOMICS_CANNABIS

| Stat | Value |
|---|---|
| Total tables | 7 |
| Flat tables | 3 |
| Nested tables | 4 |
| Instances using nested tables | 2 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq020 | MNPR01_201703 | MNPR01_REFERENCE_201703 |
| sf_bq107 | MNPR01_201703 | MNPR01_REFERENCE_201703 |

### GEO_OPENSTREETMAP

| Stat | Value |
|---|---|
| Total tables | 10 |
| Flat tables | 0 |
| Nested tables | 10 |
| Instances using nested tables | 6 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq017 | PLANET_FEATURES | — |
| sf_bq131 | PLANET_FEATURES | — |
| sf_bq253 | PLANET_FEATURES, PLANET_RELATIONS | — |
| sf_bq254 | PLANET_FEATURES | — |
| sf_bq348 | HISTORY_NODES, PLANET_NODES | — |
| sf_bq349 | PLANET_FEATURES, PLANET_NODES | — |

### GEO_OPENSTREETMAP_BOUNDARIES

| Stat | Value |
|---|---|
| Total tables | 26 |
| Flat tables | 14 |
| Nested tables | 12 |
| Instances using nested tables | 1 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq056 | PLANET_WAYS | STATES |

### GEO_OPENSTREETMAP_CENSUS_PLACES

| Stat | Value |
|---|---|
| Total tables | 67 |
| Flat tables | 57 |
| Nested tables | 10 |
| Instances using nested tables | 1 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq289 | PLANET_FEATURES_POINTS | PLACES_PENNSYLVANIA |

### GEO_OPENSTREETMAP_WORLDPOP

| Stat | Value |
|---|---|
| Total tables | 11 |
| Flat tables | 1 |
| Nested tables | 10 |
| Instances using nested tables | 1 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq250 | PLANET_LAYERS | POPULATION_GRID_1KM |

### GITHUB_REPOS

| Stat | Value |
|---|---|
| Total tables | 6 |
| Flat tables | 4 |
| Nested tables | 2 |
| Instances using nested tables | 5 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq036 | LANGUAGES, SAMPLE_COMMITS | — |
| sf_bq193 | LANGUAGES | SAMPLE_CONTENTS, SAMPLE_FILES |
| sf_bq248 | LANGUAGES | SAMPLE_CONTENTS, SAMPLE_FILES |
| sf_bq255 | LANGUAGES, SAMPLE_COMMITS | LICENSES |
| sf_bq359 | LANGUAGES, SAMPLE_COMMITS | — |

### GITHUB_REPOS_DATE

| Stat | Value |
|---|---|
| Total tables | 38 |
| Flat tables | 4 |
| Nested tables | 34 |
| Instances using nested tables | 6 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq182 | LANGUAGES, _20230118 | — |
| sf_bq191 | _2017 | SAMPLE_CONTENTS, SAMPLE_FILES |
| sf_bq192 | _202204 | LICENSES, SAMPLE_FILES |
| sf_bq217 | LANGUAGES, _20230118 | — |
| sf_bq224 | _202204 | LICENSES |
| sf_bq295 | _201706 | SAMPLE_CONTENTS, SAMPLE_FILES |

### GOOGLE_ADS

| Stat | Value |
|---|---|
| Total tables | 2 |
| Flat tables | 0 |
| Nested tables | 2 |
| Instances using nested tables | 2 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq412 | REMOVED_CREATIVE_STATS | — |
| sf_bq423 | CREATIVE_STATS | — |

### GOOG_BLOCKCHAIN

| Stat | Value |
|---|---|
| Total tables | 7 |
| Flat tables | 5 |
| Nested tables | 2 |
| Instances using nested tables | 2 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq058 | LOGS | — |
| sf_bq416 | LOGS | BLOCKS |

### HUMAN_GENOME_VARIANTS

| Stat | Value |
|---|---|
| Total tables | 8 |
| Flat tables | 4 |
| Nested tables | 4 |
| Instances using nested tables | 2 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq037 | _1000_GENOMES_PHASE_3_OPTIMIZED_SCHEMA_VARIANTS_20150220 | — |
| sf_bq415 | _1000_GENOMES_PHASE_3_VARIANTS_20150220 | — |

### IDC

| Stat | Value |
|---|---|
| Total tables | 16 |
| Flat tables | 9 |
| Nested tables | 7 |
| Instances using nested tables | 14 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq069 | DICOM_ALL | — |
| sf_bq070 | DICOM_ALL | — |
| sf_bq321 | DICOM_ALL | — |
| sf_bq323 | DICOM_ALL | — |
| sf_bq324 | DICOM_ALL | — |
| sf_bq345 | DICOM_ALL | — |
| sf_bq346 | DICOM_ALL, SEGMENTATIONS | — |
| sf_bq347 | DICOM_ALL, SEGMENTATIONS | — |
| sf_bq390 | DICOM_ALL, SEGMENTATIONS | — |
| sf_bq417 | DICOM_ALL | DICOM_METADATA_CURATED |
| sf_bq421 | DICOM_ALL | — |
| sf_bq422 | DICOM_ALL | — |
| sf_bq455 | DICOM_ALL | COLUMNS, ROWS |
| sf_bq456 | DICOM_ALL, QUANTITATIVE_MEASUREMENTS | — |

### NOAA_GLOBAL_FORECAST_SYSTEM

| Stat | Value |
|---|---|
| Total tables | 11 |
| Flat tables | 0 |
| Nested tables | 11 |
| Instances using nested tables | 1 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq291 | NOAA_GFS0P25 | — |

### OPEN_TARGETS_GENETICS_2

| Stat | Value |
|---|---|
| Total tables | 13 |
| Flat tables | 8 |
| Nested tables | 5 |
| Instances using nested tables | 1 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq325 | STUDIES, VARIANT_DISEASE | GENES, LOCUS2GENE |

### PATENTS

| Stat | Value |
|---|---|
| Total tables | 3 |
| Flat tables | 1 |
| Nested tables | 2 |
| Instances using nested tables | 15 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq026 | PUBLICATIONS | — |
| sf_bq027 | PUBLICATIONS | — |
| sf_bq029 | PUBLICATIONS | — |
| sf_bq033 | PUBLICATIONS | — |
| sf_bq091 | PUBLICATIONS | — |
| sf_bq099 | PUBLICATIONS | — |
| sf_bq209 | PUBLICATIONS | — |
| sf_bq210 | PUBLICATIONS | — |
| sf_bq211 | PUBLICATIONS | — |
| sf_bq212 | PUBLICATIONS | — |
| sf_bq213 | PUBLICATIONS | — |
| sf_bq215 | PUBLICATIONS | — |
| sf_bq221 | CPC_DEFINITION, PUBLICATIONS | — |
| sf_bq222 | CPC_DEFINITION, PUBLICATIONS | — |
| sf_bq223 | CPC_DEFINITION, PUBLICATIONS | — |

### PATENTS_GOOGLE

| Stat | Value |
|---|---|
| Total tables | 4 |
| Flat tables | 1 |
| Nested tables | 3 |
| Instances using nested tables | 4 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq127 | ABS_AND_EMB, PUBLICATIONS | — |
| sf_bq214 | ABS_AND_EMB, PUBLICATIONS | — |
| sf_bq216 | ABS_AND_EMB, PUBLICATIONS | — |
| sf_bq247 | ABS_AND_EMB, PUBLICATIONS | — |

### PATENTS_USPTO

| Stat | Value |
|---|---|
| Total tables | 46 |
| Flat tables | 42 |
| Nested tables | 4 |
| Instances using nested tables | 2 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq207 | PUBLICATIONS | MATCH, PATENT_CLAIMS_STATS |
| sf_bq420 | PUBLICATIONS | MATCH_APP, OFFICE_ACTIONS |

### PYPI

| Stat | Value |
|---|---|
| Total tables | 2 |
| Flat tables | 0 |
| Nested tables | 2 |
| Instances using nested tables | 1 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq251 | DISTRIBUTION_METADATA, FILE_DOWNLOADS | — |

### WORD_VECTORS_US

| Stat | Value |
|---|---|
| Total tables | 3 |
| Flat tables | 2 |
| Nested tables | 1 |
| Instances using nested tables | 3 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq458 | GLOVE_VECTORS | NATURE, WORD_FREQUENCIES |
| sf_bq459 | GLOVE_VECTORS | NATURE, WORD_FREQUENCIES |
| sf_bq460 | GLOVE_VECTORS | NATURE, WORD_FREQUENCIES |

### _1000_GENOMES

| Stat | Value |
|---|---|
| Total tables | 3 |
| Flat tables | 2 |
| Nested tables | 1 |
| Instances using nested tables | 4 |

| Instance | Nested Tables Used | Flat Tables Used |
|---|---|---|
| sf_bq451 | VARIANTS | SAMPLE_INFO |
| sf_bq452 | VARIANTS | SAMPLE_INFO |
| sf_bq453 | VARIANTS | SAMPLE_INFO |
| sf_bq454 | VARIANTS | SAMPLE_INFO |

---

## Table Utilization

- **43 unique nested tables** are actually referenced in questions
- Spread across **21 databases** (out of 58 total)
- Out of **254 total nested tables** in Snowflake
- **16.9% utilization** — 83% of nested tables are never queried

### Counts

| Database | Total Tables | Total Nested | Flat Used | Nested Used |
|---|---|---|---|---|
| AUSTIN | 10 | 0 | 2 | 0 |
| CENSUS_BUREAU_ACS_2 | 296 | 2 | 8 | 0 |
| CRYPTO | 39 | 11 | 2 | 7 |
| DEATH | 28 | 0 | 4 | 0 |
| DEPS_DEV_V1 | 10 | 7 | 4 | 0 |
| ETHEREUM_BLOCKCHAIN | 7 | 2 | 5 | 1 |
| GENOMICS_CANNABIS | 7 | 4 | 1 | 1 |
| GEO_OPENSTREETMAP | 10 | 10 | 0 | 4 |
| GEO_OPENSTREETMAP_BOUNDARIES | 26 | 12 | 1 | 1 |
| GEO_OPENSTREETMAP_CENSUS_PLACES | 67 | 10 | 1 | 1 |
| GEO_OPENSTREETMAP_WORLDPOP | 11 | 10 | 1 | 1 |
| GITHUB_REPOS | 6 | 2 | 3 | 2 |
| GITHUB_REPOS_DATE | 38 | 34 | 3 | 5 |
| GOOGLE_ADS | 2 | 2 | 0 | 2 |
| GOOGLE_TRENDS | 4 | 0 | 2 | 0 |
| GOOG_BLOCKCHAIN | 7 | 2 | 2 | 1 |
| HTAN_2 | 94 | 1 | 6 | 0 |
| HUMAN_GENOME_VARIANTS | 8 | 4 | 0 | 2 |
| IDC | 16 | 7 | 4 | 3 |
| IOWA_LIQUOR_SALES | 1 | 0 | 1 | 0 |
| META_KAGGLE | 29 | 0 | 5 | 0 |
| NEW_YORK_CITIBIKE_1 | 117 | 2 | 13 | 0 |
| NOAA_DATA | 218 | 0 | 75 | 0 |
| NOAA_DATA_PLUS | 234 | 2 | 77 | 0 |
| NOAA_GLOBAL_FORECAST_SYSTEM | 11 | 11 | 0 | 1 |
| NOAA_PORTS | 18 | 2 | 3 | 0 |
| OPEN_TARGETS_GENETICS_2 | 13 | 5 | 2 | 2 |
| PANCANCER_ATLAS_1 | 10 | 0 | 3 | 0 |
| PATENTS | 3 | 2 | 0 | 2 |
| PATENTSVIEW | 59 | 0 | 5 | 0 |
| PATENTS_GOOGLE | 4 | 3 | 0 | 2 |
| PATENTS_USPTO | 46 | 4 | 4 | 1 |
| PYPI | 2 | 2 | 0 | 2 |
| SAN_FRANCISCO_PLUS | 19 | 0 | 3 | 0 |
| STACKOVERFLOW | 16 | 0 | 2 | 0 |
| TCGA | 157 | 0 | 4 | 0 |
| TCGA_HG19_DATA_V0 | 33 | 0 | 2 | 0 |
| TCGA_HG38_DATA_V0 | 50 | 1 | 5 | 0 |
| TCGA_MITELMAN | 176 | 0 | 2 | 0 |
| THELOOK_ECOMMERCE | 7 | 0 | 7 | 0 |
| WIDE_WORLD_IMPORTERS | 31 | 0 | 6 | 0 |
| WORD_VECTORS_US | 3 | 1 | 2 | 1 |
| _1000_GENOMES | 3 | 1 | 1 | 1 |

### Nested Tables Used in Questions (by DB)

| Database | Nested Tables |
|---|---|
| CRYPTO | BLOCKS, INPUTS, LOGS, ORACLE_REQUESTS, OUTPUTS, TRANSACTIONS, TRANSITIONS |
| ETHEREUM_BLOCKCHAIN | CONTRACTS |
| GENOMICS_CANNABIS | MNPR01_201703 |
| GEO_OPENSTREETMAP | HISTORY_NODES, PLANET_FEATURES, PLANET_NODES, PLANET_RELATIONS |
| GEO_OPENSTREETMAP_BOUNDARIES | PLANET_WAYS |
| GEO_OPENSTREETMAP_CENSUS_PLACES | PLANET_FEATURES_POINTS |
| GEO_OPENSTREETMAP_WORLDPOP | PLANET_LAYERS |
| GITHUB_REPOS | LANGUAGES, SAMPLE_COMMITS |
| GITHUB_REPOS_DATE | LANGUAGES, _2017, _201706, _202204, _20230118 |
| GOOGLE_ADS | CREATIVE_STATS, REMOVED_CREATIVE_STATS |
| GOOG_BLOCKCHAIN | LOGS |
| HUMAN_GENOME_VARIANTS | _1000_GENOMES_PHASE_3_OPTIMIZED_SCHEMA_VARIANTS_20150220, _1000_GENOMES_PHASE_3_VARIANTS_20150220 |
| IDC | DICOM_ALL, QUANTITATIVE_MEASUREMENTS, SEGMENTATIONS |
| NOAA_GLOBAL_FORECAST_SYSTEM | NOAA_GFS0P25 |
| OPEN_TARGETS_GENETICS_2 | STUDIES, VARIANT_DISEASE |
| PATENTS | CPC_DEFINITION, PUBLICATIONS |
| PATENTS_GOOGLE | ABS_AND_EMB, PUBLICATIONS |
| PATENTS_USPTO | PUBLICATIONS |
| PYPI | DISTRIBUTION_METADATA, FILE_DOWNLOADS |
| WORD_VECTORS_US | GLOVE_VECTORS |
| _1000_GENOMES | VARIANTS |

### Nested Instances (use ≥1 nested table) — 95 total

| Database | # | Instance IDs |
|---|---|---|
| CRYPTO | 15 | sf_bq005, sf_bq057, sf_bq065, sf_bq068, sf_bq083, sf_bq092, sf_bq093, sf_bq135, sf_bq136, sf_bq195, sf_bq256, sf_bq292, sf_bq334, sf_bq335, sf_bq444 |
| PATENTS | 15 | sf_bq026, sf_bq027, sf_bq029, sf_bq033, sf_bq091, sf_bq099, sf_bq209, sf_bq210, sf_bq211, sf_bq212, sf_bq213, sf_bq215, sf_bq221, sf_bq222, sf_bq223 |
| IDC | 14 | sf_bq069, sf_bq070, sf_bq321, sf_bq323, sf_bq324, sf_bq345, sf_bq346, sf_bq347, sf_bq390, sf_bq417, sf_bq421, sf_bq422, sf_bq455, sf_bq456 |
| GEO_OPENSTREETMAP | 6 | sf_bq017, sf_bq131, sf_bq253, sf_bq254, sf_bq348, sf_bq349 |
| GITHUB_REPOS_DATE | 6 | sf_bq182, sf_bq191, sf_bq192, sf_bq217, sf_bq224, sf_bq295 |
| GITHUB_REPOS | 5 | sf_bq036, sf_bq193, sf_bq248, sf_bq255, sf_bq359 |
| PATENTS_GOOGLE | 4 | sf_bq127, sf_bq214, sf_bq216, sf_bq247 |
| _1000_GENOMES | 4 | sf_bq451, sf_bq452, sf_bq453, sf_bq454 |
| WORD_VECTORS_US | 3 | sf_bq458, sf_bq459, sf_bq460 |
| GENOMICS_CANNABIS | 2 | sf_bq020, sf_bq107 |
| GOOGLE_ADS | 2 | sf_bq412, sf_bq423 |
| GOOG_BLOCKCHAIN | 2 | sf_bq058, sf_bq416 |
| HUMAN_GENOME_VARIANTS | 2 | sf_bq037, sf_bq415 |
| PATENTS_USPTO | 2 | sf_bq207, sf_bq420 |
| ETHEREUM_BLOCKCHAIN | 1 | sf_bq450 |
| GEO_OPENSTREETMAP_BOUNDARIES | 1 | sf_bq056 |
| GEO_OPENSTREETMAP_CENSUS_PLACES | 1 | sf_bq289 |
| GEO_OPENSTREETMAP_WORLDPOP | 1 | sf_bq250 |
| NOAA_GLOBAL_FORECAST_SYSTEM | 1 | sf_bq291 |
| OPEN_TARGETS_GENETICS_2 | 1 | sf_bq325 |
| PYPI | 1 | sf_bq251 |
| NETHERLANDS_OPEN_MAP_DATA | 2 | sf009, sf013 |
| BRAZE_USER_EVENT_DEMO_DATASET | 1 | sf018 |
| GLOBAL_GOVERNMENT | 1 | sf003 |
| US_REAL_ESTATE | 1 | sf037 |
| WEATHER__ENVIRONMENT | 1 | sf012 |

### Flat-Only Instances (no nested tables) — 112 total

| Database | # | Instance IDs |
|---|---|---|
| THELOOK_ECOMMERCE | 19 | sf_bq014, sf_bq188, sf_bq189, sf_bq190, sf_bq197, sf_bq258, sf_bq259, sf_bq260, sf_bq261, sf_bq262, sf_bq263, sf_bq264, sf_bq265, sf_bq266, sf_bq271, sf_bq272, sf_bq273, sf_bq333, sf_bq361 |
| GITHUB_REPOS | 10 | sf_bq100, sf_bq101, sf_bq180, sf_bq194, sf_bq225, sf_bq233, sf_bq249, sf_bq252, sf_bq375, sf_bq377 |
| PANCANCER_ATLAS_1 | 6 | sf_bq153, sf_bq154, sf_bq156, sf_bq157, sf_bq158, sf_bq159 |
| CRYPTO | 5 | sf_bq080, sf_bq184, sf_bq340, sf_bq341, sf_bq342 |
| META_KAGGLE | 5 | sf_bq160, sf_bq167, sf_bq171, sf_bq331, sf_bq380 |
| CENSUS_BUREAU_ACS_2 | 4 | sf_bq007, sf_bq073, sf_bq410, sf_bq429 |
| DEPS_DEV_V1 | 4 | sf_bq016, sf_bq028, sf_bq062, sf_bq063 |
| TCGA | 4 | sf_bq043, sf_bq044, sf_bq147, sf_bq148 |
| TCGA_MITELMAN | 4 | sf_bq166, sf_bq170, sf_bq175, sf_bq176 |
| WIDE_WORLD_IMPORTERS | 4 | sf_bq370, sf_bq371, sf_bq372, sf_bq373 |
| NEW_YORK_CITIBIKE_1 | 3 | sf_bq050, sf_bq358, sf_bq426 |
| PATENTSVIEW | 3 | sf_bq052, sf_bq128, sf_bq246 |
| TCGA_HG38_DATA_V0 | 3 | sf_bq141, sf_bq152, sf_bq155 |
| DEATH | 2 | sf_bq072, sf_bq118 |
| ETHEREUM_BLOCKCHAIN | 2 | sf_bq012, sf_bq187 |
| GOOG_BLOCKCHAIN | 2 | sf_bq084, sf_bq226 |
| GOOGLE_TRENDS | 2 | sf_bq104, sf_bq411 |
| HTAN_2 | 2 | sf_bq163, sf_bq164 |
| NOAA_DATA_PLUS | 2 | sf_bq071, sf_bq236 |
| STACKOVERFLOW | 2 | sf_bq121, sf_bq307 |
| AUSTIN | 1 | sf_bq283 |
| IDC | 1 | sf_bq320 |
| IOWA_LIQUOR_SALES | 1 | sf_bq219 |
| NOAA_DATA | 1 | sf_bq117 |
| NOAA_PORTS | 1 | sf_bq276 |
| SAN_FRANCISCO_PLUS | 1 | sf_bq294 |
| TCGA_HG19_DATA_V0 | 1 | sf_bq150 |
| OPEN_TARGETS_PLATFORM_1 | 3 | sf_bq095, sf_bq350, sf_bq379 |
| FINANCE__ECONOMICS | 3 | sf002, sf006, sf044 |
| US_REAL_ESTATE | 2 | sf008, sf010 |
| OPEN_TARGETS_PLATFORM_2 | 1 | sf_bq078 |
| STACKOVERFLOW_PLUS | 1 | sf_bq015 |
| GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI | 1 | sf001 |
| BRAZE_USER_EVENT_DEMO_DATASET | 1 | sf035 |
| AMAZON_VENDOR_ANALYTICS__SAMPLE_DATASET | 1 | sf029 |
| US_ADDRESSES__POI | 1 | sf040 |
| YES_ENERGY__SAMPLE_DATA | 1 | sf041 |
| CENSUS_GALAXY__ZIP_CODE_TO_BLOCK_GROUP_SAMPLE | 1 | sf011 |
| CENSUS_GALAXY__AIML_MODEL_DATA_ENRICHMENT_SAMPLE | 1 | sf014 |

---

## Snowflake Schema Stats (All Tables)

| Metric | Count | % |
|---|---|---|
| Total tables | 2,326 | 100% |
| Flat tables | 2,072 | 89.1% |
| Nested tables (VARIANT/ARRAY/OBJECT) | 254 | 10.9% |
| Databases with ≥1 nested table | 31 / 58 | 53% |

---

## Snowflake Database Sizes (Compressed Storage)

> [!NOTE]
> Sizes are Snowflake's **compressed columnar storage**. Actual uncompressed/JSON size is typically 5-10x larger.
> Total across all databases: **~32.4 TB compressed**, **682 billion rows**.

### Databases Used in Questions (sorted by size)

| Database | Qs | Tbls | Rows | GB | To Use? |
|---|---|---|---|---|---|
| STACKOVERFLOW | 2 | 16 | 637M | 78.0 | [ ] |
| OPEN_TARGETS_GENETICS_2 | 1 | 13 | 4.1B | 212.2 | [ ] |
| GITHUB_REPOS_DATE | 6 | 38 | 528M | 168.8 | [ ] |
| CRYPTO | 20 | 39 | 159M | 30.4 | [ ] |
| PATENTS_USPTO | 2 | 46 | 670M | 25.3 | [ ] |
| NOAA_GLOBAL_FORECAST | 1 | 11 | 218M | 24.5 | [ ] |
| NOAA_DATA_PLUS | 2 | 234 | 742M | 21.6 | [ ] |
| NOAA_DATA | 1 | 218 | 742M | 20.4 | [ ] |
| GEO_OSM_BOUNDARIES | 1 | 26 | 217M | 17.5 | [ ] |
| GEO_OSM_CENSUS | 1 | 67 | 217M | 16.7 | [ ] |
| GEO_OPENSTREETMAP | 6 | 10 | 217M | 16.3 | [ ] |
| GEO_OSM_WORLDPOP | 1 | 11 | 217M | 16.3 | [ ] |
| GOOG_BLOCKCHAIN | 4 | 7 | 58M | 7.3 | [ ] |
| META_KAGGLE | 5 | 29 | 345M | 7.1 | [ ] |
| NY_CITIBIKE_1 | 3 | 117 | 233M | 6.1 | [ ] |
| PATENTSVIEW | 3 | 59 | 126M | 4.5 | [ ] |
| DEPS_DEV_V1 | 4 | 10 | 80M | 3.5 | [ ] |
| PYPI | 1 | 2 | 87M | 3.0 | [ ] |
| TCGA_MITELMAN | 4 | 176 | 136M | 2.8 | [ ] |
| TCGA | 4 | 157 | 133M | 2.7 | [ ] |
| GOOGLE_TRENDS | 2 | 4 | 498M | 2.6 | [ ] |
| WORD_VECTORS_US | 3 | 3 | 2.4M | 2.6 | [ ] |
| CENSUS_ACS_2 | 4 | 296 | 4.3M | 2.5 | [ ] |
| PATENTS | 15 | 3 | 823K | 2.2 | [ ] |
| PATENTS_GOOGLE | 4 | 4 | 1.2M | 1.7 | [ ] |
| TCGA_HG38 | 3 | 50 | 100M | 1.7 | [ ] |
| IDC | 15 | 16 | 4.8M | 1.6 | [ ] |
| SF_PLUS | 1 | 19 | 43M | 1.6 | [ ] |
| NOAA_PORTS | 1 | 18 | 903K | 1.2 | [ ] |
| IOWA_LIQUOR | 1 | 1 | 30M | 1.0 | [ ] |
| HTAN_2 | 2 | 94 | 25M | 0.9 | [ ] |
| TCGA_HG19 | 1 | 33 | 51M | 0.5 | [ ] |
| _1000_GENOMES | 4 | 3 | 42K | 0.5 | [ ] |
| ETHEREUM_BLOCKCHAIN | 3 | 7 | 3.6M | 0.2 | [ ] |
| PANCANCER_ATLAS_1 | 6 | 10 | 19M | 0.2 | [ ] |
| GITHUB_REPOS | 15 | 6 | 7.6M | 0.2 | [ ] |
| AUSTIN | 1 | 10 | 5.9M | 0.2 | [ ] |
| DEATH | 2 | 28 | 18M | 0.1 | [ ] |
| THELOOK_ECOMMERCE | 19 | 7 | 3.3M | 0.1 | [ ] |
| GENOMICS_CANNABIS | 2 | 7 | 570K | 0.1 | [ ] |
| HUMAN_GENOME_VAR | 2 | 8 | 235K | 0.1 | [ ] |
| GOOGLE_ADS | 2 | 2 | 481K | 0.1 | [ ] |
| WIDE_WORLD_IMP | 4 | 31 | 1.1M | 0.0 | [ ] |
| OPEN_TARGETS_PLATFORM_1 | 3 | 27 | 95.3M | 5.7 | [ ] |
| OPEN_TARGETS_PLATFORM_2 | 1 | 29 | 95.4M | 5.7 | [ ] |
| STACKOVERFLOW_PLUS | 1 | 24 | 745.4M | 93.3 | [ ] |
| FINANCE__ECONOMICS | 3 | 0 | 0 | 0.0 | [ ] |
| US_REAL_ESTATE | 3 | 0 | 0 | 0.0 | [ ] |
| BRAZE_USER_EVENT_DEMO | 2 | 0 | 0 | 0.0 | [ ] |
| NETHERLANDS_OPEN_MAP | 2 | N/A | N/A | N/A | [ ] |
| GLOBAL_WEATHER__CLIMATE | 1 | 0 | 0 | 0.0 | [ ] |
| GLOBAL_GOVERNMENT | 1 | 0 | 0 | 0.0 | [ ] |
| WEATHER__ENVIRONMENT | 1 | 0 | 0 | 0.0 | [ ] |
| AMAZON_VENDOR_ANALYTICS | 1 | N/A | N/A | N/A | [ ] |
| US_ADDRESSES__POI | 1 | 0 | 0 | 0.0 | [ ] |
| YES_ENERGY__SAMPLE_DATA | 1 | 0 | 0 | 0.0 | [ ] |
| CENSUS_GALAXY__ZIP_CODE | 1 | 8 | 2.2M | 0.0 | [ ] |
| CENSUS_GALAXY__AIML_MODEL | 1 | 8 | 2.2M | 0.0 | [ ] |
| **TOTAL (58 DBs)** | **207** | **~2,042** | **~11.5B** | **811.6+** | **-** |

> [!NOTE]
> There is a discrepancy between `NL_questions.jsonl` (189 questions) and [spider2-lite-gold-tables.jsonl](file:///Users/arjun.nr/Spider-dataset/mine/Spider2/methods/gold-tables/spider2-lite-gold-tables.jsonl) (184 questions). The following 5 instances are missing from the gold tables file entirely, but are requested in the evaluation pipeline:
> - `sf_bq350`, `sf_bq379`, `sf_bq095` (using `OPEN_TARGETS_PLATFORM_1`)
> - `sf_bq078` (using `OPEN_TARGETS_PLATFORM_2`)
> - `sf_bq015` (using `STACKOVERFLOW_PLUS`)
>
> They have been added to the totals above.

> [!TIP]
> **What about `sf018`, `sf044`, etc.?**
> There are **18 pure Snowflake instances** (formatted exactly like `sf018`) in `NL_questions.jsonl`. 
> Unlike the `sf_bq` gap, **ALL 18 of these instances have perfect mappings in [spider2-lite-gold-tables.jsonl](file:///Users/arjun.nr/Spider-dataset/mine/Spider2/methods/gold-tables/spider2-lite-gold-tables.jsonl)** (0 are missing). These 18 queries hit 12 distinct databases (mostly Marketplace sample data like `FINANCE__ECONOMICS`, `US_REAL_ESTATE`, `BRAZE_USER_EVENT_DEMO_DATASET`, etc.), pushing the absolute total evaluated Snowflake-related queries to **207**.

### All Databases (full list)

| Database | Tables | Rows | Size (GB) |
|---|---|---|---|
| SNOWFLAKE_SAMPLE_DATA | 80 | 623,006,379,330 | 29,947.8 |
| GNOMAD | 71 | 987,103,251 | 453.0 |
| OPEN_TARGETS_GENETICS_2 | 13 | 4,129,628,057 | 212.2 |
| OPEN_TARGETS_GENETICS_1 | 13 | 4,129,628,057 | 212.2 |
| GBIF | 1 | 3,025,858,163 | 190.9 |
| GITHUB_REPOS_DATE | 5,173 | 528,083,235 | 168.8 |
| HTAN_1 | 200 | 5,819,795,740 | 153.9 |
| PANCANCER_ATLAS_2 | 21 | 8,852,666,073 | 126.0 |
| STACKOVERFLOW_PLUS | 24 | 745,472,735 | 93.3 |
| STACKOVERFLOW | 16 | 637,122,277 | 78.0 |
| NEW_YORK_GHCN | 288 | 4,516,559,135 | 64.2 |
| NEW_YORK_NOAA | 119 | 1,801,492,788 | 55.4 |
| NEW_YORK_GEO | 38 | 1,627,689,681 | 53.1 |
| NEW_YORK | 22 | 1,627,506,185 | 51.9 |
| FHIR_SYNTHEA | 17 | 433,945,069 | 44.3 |
| NEW_YORK_PLUS | 43 | 2,126,642,588 | 33.8 |
| CRYPTO | 39 | 158,782,141 | 30.4 |
| CMS_DATA | 52 | 2,181,075,029 | 26.8 |
| EPA_HISTORICAL_AIR_QUALITY | 32 | 2,437,725,581 | 25.6 |
| OPENAQ | 33 | 2,443,320,195 | 25.6 |
| PATENTS_USPTO | 46 | 669,659,537 | 25.3 |
| NOAA_GLOBAL_FORECAST_SYSTEM | 11 | 218,374,323 | 24.5 |
| NOAA_DATA_PLUS | 234 | 742,276,079 | 21.6 |
| NOAA_DATA | 218 | 742,092,583 | 20.4 |
| FEC | 486 | 551,049,355 | 19.7 |
| GEO_OPENSTREETMAP_BOUNDARIES | 26 | 217,321,044 | 17.5 |
| GEO_OPENSTREETMAP_CENSUS_PLACES | 67 | 217,197,254 | 16.7 |
| GEO_OPENSTREETMAP | 10 | 217,137,548 | 16.3 |
| GEO_OPENSTREETMAP_WORLDPOP | 11 | 217,152,521 | 16.3 |
| SEC_QUARTERLY_FINANCIALS | 10 | 535,426,823 | 15.5 |
| EBI_CHEMBL | 785 | 522,698,820 | 15.6 |
| GHCN_D | 266 | 2,889,052,950 | 12.3 |
| CHICAGO | 2 | 219,833,989 | 8.9 |
| USFS_FIA | 13 | 100,191,674 | 7.9 |
| CPTAC_PDC | 79 | 376,808,796 | 7.5 |
| GOOG_BLOCKCHAIN | 7 | 57,977,736 | 7.3 |
| META_KAGGLE | 29 | 344,683,452 | 7.1 |
| HACKER_NEWS | 1 | 41,855,814 | 6.3 |
| NEW_YORK_CITIBIKE_1 | 117 | 233,111,096 | 6.1 |
| OPEN_TARGETS_PLATFORM_2 | 29 | 95,468,766 | 5.7 |
| OPEN_TARGETS_PLATFORM_1 | 27 | 95,374,614 | 5.7 |
| PATENTSVIEW | 59 | 126,201,879 | 4.5 |
| CENSUS_BUREAU_ACS_1 | 351 | 4,414,432 | 3.5 |
| DEPS_DEV_V1 | 10 | 80,030,681 | 3.5 |
| NOAA_GSOD | 97 | 173,986,603 | 3.5 |
| TCGA_BIOCLIN_V0 | 80 | 33,627,152 | 3.3 |
| LIBRARIES_IO | 7 | 89,106,214 | 3.1 |
| FDA | 83 | 981,978 | 3.0 |
| COVID19_OPEN_WORLD_BANK | 23 | 65,056,026 | 3.0 |
| PYPI | 2 | 86,867,269 | 3.0 |
| COVID19_OPEN_DATA | 2 | 44,907,165 | 2.8 |
| TCGA_MITELMAN | 176 | 135,541,478 | 2.8 |
| TCGA | 157 | 132,611,047 | 2.7 |
| GOOGLE_TRENDS | 4 | 498,463,514 | 2.6 |
| WORD_VECTORS_US | 3 | 2,427,368 | 2.6 |
| CENSUS_BUREAU_ACS_2 | 296 | 4,267,221 | 2.5 |
| DIMENSIONS_AI_COVID19 | 6 | 1,399,321 | 2.4 |
| PATENTS | 3 | 822,932 | 2.2 |
| ECOMMERCE | 17 | 45,654,003 | 2.0 |
| NPPES | 20 | 14,534,949 | 1.9 |
| COVID19_SYMPTOM_SEARCH | 6 | 10,557,796 | 1.9 |
| PATENTS_GOOGLE | 4 | 1,244,366 | 1.7 |
| TCGA_HG38_DATA_V0 | 50 | 99,538,340 | 1.7 |
| USDA_NASS_AGRICULTURE | 11 | 76,997,959 | 1.6 |
| IDC | 16 | 4,823,128 | 1.6 |
| SAN_FRANCISCO_PLUS | 20 | 43,298,246 | 1.6 |
| OPEN_IMAGES | 4 | 103,933,912 | 1.6 |
| BLS | 143 | 18,207,063 | 1.5 |
| SAN_FRANCISCO | 9 | 118,187,859 | 1.3 |
| NOAA_PORTS | 18 | 903,330 | 1.2 |
| IOWA_LIQUOR_SALES | 1 | 30,082,002 | 1.0 |
| IOWA_LIQUOR_SALES_PLUS | 3 | 33,290,963 | 1.0 |
| GA4 | 92 | 4,295,584 | 1.0 |
| COVID19_USA | 285 | 9,289,501 | 2.0 |
| HTAN_2 | 94 | 24,926,156 | 0.9 |
| IRS_990 | 18 | 5,517,314 | 0.6 |
| TCGA_HG19_DATA_V0 | 33 | 51,298,241 | 0.5 |
| _1000_GENOMES | 3 | 41,771 | 0.5 |
| GA360 | 366 | 903,653 | 0.4 |
| NCAA_INSIGHTS | 17 | 5,791,626 | 0.3 |
| NCAA_BASKETBALL | 10 | 5,768,154 | 0.3 |
| GOOGLE_DEI | 140 | 18,024,101 | 0.3 |
| CENSUS_BUREAU_USA | 14 | 4,566,484 | 0.3 |
| ETHEREUM_BLOCKCHAIN | 7 | 3,556,490 | 0.2 |
| COVID19_JHU_WORLD_BANK | 25 | 24,413,793 | 0.2 |
| FIREBASE | 114 | 5,700,000 | 0.2 |
| PANCANCER_ATLAS_1 | 10 | 18,945,946 | 0.2 |
| GITHUB_REPOS | 6 | 7,617,607 | 0.2 |
| AUSTIN | 10 | 5,912,689 | 0.2 |
| DEATH | 28 | 18,358,312 | 0.1 |
| THELOOK_ECOMMERCE | 7 | 3,339,950 | 0.1 |
| GENOMICS_CANNABIS | 7 | 569,808 | 0.1 |
| CYMBAL_INVESTMENTS | 1 | 1,222,562 | 0.1 |
| THE_MET | 3 | 801,761 | 0.1 |
| NHTSA_TRAFFIC_FATALITIES_PLUS | 123 | 8,982,194 | 0.3 |
| NHTSA_TRAFFIC_FATALITIES | 108 | 6,038,588 | 0.1 |
| HUMAN_GENOME_VARIANTS | 8 | 234,739 | 0.1 |
| LONDON | 2 | 13,522,851 | 0.1 |
| MLB | 3 | 772,725 | 0.1 |
| GOOGLE_ADS | 2 | 480,945 | 0.1 |
| USA_NAMES | 2 | 11,863,956 | 0.1 |
| BRAZILIAN_E_COMMERCE | 10 | 1,583,873 | 0.1 |
| E_COMMERCE | 11 | 1,559,764 | 0.1 |
| ELECTRONIC_SALES | 9 | 1,550,922 | 0.0 |
| CENSUS_GALAXY (×2) | 8 | 2,262,153 | 0.0 |
| DELIVERY_CENTER | 7 | 1,154,523 | 0.0 |
| EU_SOCCER | 8 | 222,803 | 0.0 |
| MODERN_DATA | 17 | 1,068,993 | 0.0 |
| WIDE_WORLD_IMPORTERS | 31 | 1,057,452 | 0.0 |
| CENSUS_BUREAU_INTERNATIONAL | 8 | 3,469,833 | 0.0 |
| COVID19_NYT | 4 | 3,330,494 | 0.0 |
| F1 | 29 | 1,941,243 | 0.0 |
| ECLIPSE_MEGAMOVIE | 7 | 200,878 | 0.0 |
| CITY_LEGISLATION | 15 | 792,379 | 0.0 |
| SUNROOF_SOLAR | 2 | 68,456 | 0.0 |
| CALIFORNIA_TRAFFIC_COLLISION | 4 | 471,571 | 0.0 |
| COMPLEX_ORACLE | 10 | 1,064,608 | 0.0 |
| BANK_SALES_TRADING | 19 | 1,054,949 | 0.0 |
| WORLD_BANK | 21 | 20,148,861 | 0.2 |
| ADVENTUREWORKS | 13 | 168,686 | 0.0 |
| BASEBALL | 2 | 120,178 | 0.0 |
| BBC | 1 | 2,225 | 0.0 |
| TARGETOME_REACTOME | 9 | 495,226 | 0.0 |
| MITELMAN | 19 | 2,930,435 | 0.0 |
| IMDB_MOVIES | 7 | 75,898 | 0.0 |
| DB_IMDB | 13 | 154,676 | 0.0 |
| SDOH | 294 | 4,417,166 | 1.4 |
| IPL | 8 | 293,471 | 0.0 |
| WWE | 10 | 578,899 | 0.0 |
| FINANCE__ECONOMICS | 0 | 0 | 0.0 |
| US_REAL_ESTATE | 0 | 0 | 0.0 |
| BRAZE_USER_EVENT_DEMO | 0 | 0 | 0.0 |
| NETHERLANDS_OPEN_MAP | N/A | N/A | N/A |
| GLOBAL_WEATHER__CLIMATE | 0 | 0 | 0.0 |
| GLOBAL_GOVERNMENT | 0 | 0 | 0.0 |
| WEATHER__ENVIRONMENT | 0 | 0 | 0.0 |
| AMAZON_VENDOR_ANALYTICS | N/A | N/A | N/A |
| US_ADDRESSES__POI | 0 | 0 | 0.0 |
| YES_ENERGY__SAMPLE_DATA | 0 | 0 | 0.0 |

---

## Implications for SQL++ Pipeline

1. **~48% of sf_bq questions need UNNEST/array handling** — the SQL++ prompt must handle Snowflake VARIANT → Couchbase nested JSON patterns
2. **CRYPTO, PATENTS, IDC account for 44/89 (49%)** of nested instances — focusing schema quality on these 3 databases has highest ROI
3. **Only 43 nested tables matter** out of 254 — schema generation can prioritize these
4. **Total dataset is ~32.4 TB compressed** — loading everything into Couchbase is not feasible; focus on the 43 question-relevant databases
