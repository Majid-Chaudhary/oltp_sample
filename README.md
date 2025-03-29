# OLTP Sample
Oltp sample databases and faker data, which will be used for dimensional data modelling, ETL and OLAP.

# Table: public.settings (retail DB)

| Column Name | Data Type     | Constraints                 | Description               |
|-------------|---------------|-----------------------------|---------------------------|
| setting_id  | SERIAL        | PRIMARY KEY                 | Auto-generated unique ID for each setting. |
| name        | VARCHAR(100)  | NOT NULL, UNIQUE            | Unique name that identifies the setting. |
| value       | INT           | NOT NULL                    | Integer value associated with the setting. Meaning depends on the setting name. |

## Sample Data

| setting_id | name              | value   |
|------------|-------------------|---------|
| 1          | batch_size        | 100000  |
| 2          | pause_seconds     | 60      |
| 3          | first_load        | 0       |
| 4          | continous_loading | 1       |

## Setting Descriptions
- **batch_size**: Defines how many rows the script inserts in a single batch during data loading.
- **pause_seconds**: Specifies how long (in seconds) the script should wait before starting the next batch.
- **first_load**: Tells the script whether to load base (dimensional) data before inserting fact data:
  - Set to `1` for the initial run to load base tables like customers, cities, warehouses, etc.
  - After the first load, change it to `0` to prevent duplicate inserts or constraint violations in future runs.
  - Important for synthetic or test data where base and fact data are inserted together.
- **continous_loading**: If set to `1`, the script will continuously insert data in batches in a loop. If set to `0`, it performs a single pass.

## Notes
- This table is used to configure behavior of ETL/ELT scripts dynamically.
- Unique `name` values ensure each setting is distinct.
- `value` provides numeric input to control script execution logic.

