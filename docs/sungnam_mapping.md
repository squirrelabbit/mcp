Sungnam Service Data Mapping (Draft)

Scope
- Source files: data/sungnam_*.csv
- Target tables: gold_activity, gold_demographics
- Granularity: monthly (STD_YM)

Common rules
- Time key: use STD_YM (YYYYMM) -> date bucket YYYY-MM-01
- Spatial key:
  - HCODE (10-digit emd) for service_* tables
  - SGNG_CD (5-digit sig) for unique_pop
- Foot traffic: numeric sum of relevant population columns
- Demographics: map sex + age_group into gold_demographics
- Inflow fields are ignored for now (aggregated across inflow)

Table rules
1) sungnam_service_inflow_pop_(std_ym)
- Columns: STD_YM, TIME, INFLOW_CD, HCODE, H_POP, W_POP, V_POP
- gold_activity:
  - spatial_key = HCODE
  - foot_traffic = H_POP + W_POP + V_POP
  - aggregate across TIME and INFLOW_CD
- gold_demographics: none
- source = sungnam_inflow

2) sungnam_service_sex_age_pop_(std_ym)
- Columns: STD_YM, TIME, SEX_AGE, HCODE, H_POP, W_POP, V_POP
- gold_activity:
  - spatial_key = HCODE
  - foot_traffic = H_POP + W_POP + V_POP
  - aggregate across TIME and SEX_AGE
- gold_demographics:
  - sex, age_group from SEX_AGE (e.g., m_0009 -> male + 0009)
  - value = H_POP + W_POP + V_POP
  - aggregate across TIME
- source = sungnam_sex_age

3) sungnam_service_pcell_sex_age_pop_(std_ym)
- Columns: STD_YM, HCODE, M_*, W_*, X_COORD, Y_COORD
- gold_activity:
  - spatial_key = HCODE
  - foot_traffic = sum(M_*) + sum(W_*)
- gold_demographics:
  - sex = male/female based on M_/W_ prefix
  - age_group = suffix of column (e.g., M_0009 -> 0009)
  - value = column value
- spatial coords:
  - lat = Y_COORD, lon = X_COORD
- source = sungnam_pcell_sex_age

4) sungnam_service_pcell_pop_(std_ym)
- Columns: STD_YM, HCODE, TIME_00..TIME_23, X_COORD, Y_COORD
- gold_activity:
  - spatial_key = HCODE
  - foot_traffic = sum(TIME_00..TIME_23)
- gold_demographics: none
- spatial coords:
  - lat = Y_COORD, lon = X_COORD
- source = sungnam_pcell

5) sungnam_unique_pop_(std_ym)
- Columns: STD_YM, SGNG_CD, INFLOW_CD, M_*, W_*
- gold_activity:
  - spatial_key = SGNG_CD
  - foot_traffic = sum(M_*) + sum(W_*)
  - aggregate across INFLOW_CD
- gold_demographics:
  - sex = male/female based on M_/W_ prefix
  - age_group = suffix of column (e.g., M_70U -> 70U)
  - value = column value
- source = sungnam_unique

Notes
- Current insight views sum across sources in gold_activity. If multiple sources overlap on the same
  spatial_key/date, values will be combined. Keep sources distinct to avoid accidental mixing.
