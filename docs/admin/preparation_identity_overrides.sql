CREATE TABLE IF NOT EXISTS 00_governance.warcraftlogs_admin.preparation_identity_overrides (
  id STRING NOT NULL,
  mode STRING NOT NULL,
  source_character STRING,
  target_character STRING,
  characters STRING,
  display_name STRING,
  enabled BOOLEAN NOT NULL,
  notes STRING,
  updated_by STRING,
  updated_at TIMESTAMP
)
USING DELTA;

MERGE INTO 00_governance.warcraftlogs_admin.preparation_identity_overrides AS target
USING (
  SELECT
    'temitiri-yevie' AS id,
    'replace' AS mode,
    'Temitiri' AS source_character,
    'Yevie' AS target_character,
    CAST(NULL AS STRING) AS characters,
    'Temitiri' AS display_name,
    TRUE AS enabled,
    'Playing Yevie this tier' AS notes,
    current_user() AS updated_by,
    current_timestamp() AS updated_at
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO 00_governance.warcraftlogs_admin.preparation_identity_overrides AS target
USING (
  SELECT
    'budgetgoku-mleko-pool' AS id,
    'pool' AS mode,
    CAST(NULL AS STRING) AS source_character,
    CAST(NULL AS STRING) AS target_character,
    'Budgetgoku|Mleko' AS characters,
    'Budgetgoku' AS display_name,
    TRUE AS enabled,
    'Switches between both characters' AS notes,
    current_user() AS updated_by,
    current_timestamp() AS updated_at
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
