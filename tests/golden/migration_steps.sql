-- The released migration steps, per table shape with the default name.
-- A released step never changes: a later release adds a step instead,
-- because revisions and scripts written from a step must stay valid.

-- broker VarChar: step 1
-- creates table: yes
-- columns: id, task_id, task_name, message, labels, created_at
-- indexes: taskiq_messages_id_idx hash (id)
CREATE TABLE IF NOT EXISTS taskiq_messages (id SERIAL NOT NULL PRIMARY KEY, task_id VarChar NOT NULL, task_name VARCHAR NOT NULL, message BYTEA NOT NULL, labels JSONB NOT NULL, created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW());
CREATE INDEX IF NOT EXISTS taskiq_messages_id_idx ON taskiq_messages USING HASH (id);

-- broker VarChar: step 2
-- creates table: no
-- columns: channel, not_before
-- indexes: taskiq_messages_not_before_idx btree (channel, not_before, id)
ALTER TABLE taskiq_messages
  ADD COLUMN IF NOT EXISTS channel VARCHAR,
  ADD COLUMN IF NOT EXISTS not_before TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();
CREATE INDEX IF NOT EXISTS taskiq_messages_not_before_idx
  ON taskiq_messages (channel, not_before, id);

-- broker Text: step 1
-- creates table: yes
-- columns: id, task_id, task_name, message, labels, created_at
-- indexes: taskiq_messages_id_idx hash (id)
CREATE TABLE IF NOT EXISTS taskiq_messages (id SERIAL NOT NULL PRIMARY KEY, task_id Text NOT NULL, task_name VARCHAR NOT NULL, message BYTEA NOT NULL, labels JSONB NOT NULL, created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW());
CREATE INDEX IF NOT EXISTS taskiq_messages_id_idx ON taskiq_messages USING HASH (id);

-- broker Text: step 2
-- creates table: no
-- columns: channel, not_before
-- indexes: taskiq_messages_not_before_idx btree (channel, not_before, id)
ALTER TABLE taskiq_messages
  ADD COLUMN IF NOT EXISTS channel VARCHAR,
  ADD COLUMN IF NOT EXISTS not_before TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();
CREATE INDEX IF NOT EXISTS taskiq_messages_not_before_idx
  ON taskiq_messages (channel, not_before, id);

-- broker Uuid: step 1
-- creates table: yes
-- columns: id, task_id, task_name, message, labels, created_at
-- indexes: taskiq_messages_id_idx hash (id)
CREATE TABLE IF NOT EXISTS taskiq_messages (id SERIAL NOT NULL PRIMARY KEY, task_id Uuid NOT NULL, task_name VARCHAR NOT NULL, message BYTEA NOT NULL, labels JSONB NOT NULL, created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW());
CREATE INDEX IF NOT EXISTS taskiq_messages_id_idx ON taskiq_messages USING HASH (id);

-- broker Uuid: step 2
-- creates table: no
-- columns: channel, not_before
-- indexes: taskiq_messages_not_before_idx btree (channel, not_before, id)
ALTER TABLE taskiq_messages
  ADD COLUMN IF NOT EXISTS channel VARCHAR,
  ADD COLUMN IF NOT EXISTS not_before TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();
CREATE INDEX IF NOT EXISTS taskiq_messages_not_before_idx
  ON taskiq_messages (channel, not_before, id);

-- result_backend VarChar: step 1
-- creates table: yes
-- columns: task_id, result, created_at
-- indexes: taskiq_results_task_id_idx hash (task_id)
CREATE TABLE IF NOT EXISTS taskiq_results (task_id VarChar NOT NULL PRIMARY KEY, result BYTEA NOT NULL, created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW());
CREATE INDEX IF NOT EXISTS taskiq_results_task_id_idx ON taskiq_results USING HASH (task_id);

-- result_backend Text: step 1
-- creates table: yes
-- columns: task_id, result, created_at
-- indexes: taskiq_results_task_id_idx hash (task_id)
CREATE TABLE IF NOT EXISTS taskiq_results (task_id Text NOT NULL PRIMARY KEY, result BYTEA NOT NULL, created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW());
CREATE INDEX IF NOT EXISTS taskiq_results_task_id_idx ON taskiq_results USING HASH (task_id);

-- result_backend Uuid: step 1
-- creates table: yes
-- columns: task_id, result, created_at
-- indexes: taskiq_results_task_id_idx hash (task_id)
CREATE TABLE IF NOT EXISTS taskiq_results (task_id Uuid NOT NULL PRIMARY KEY, result BYTEA NOT NULL, created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW());
CREATE INDEX IF NOT EXISTS taskiq_results_task_id_idx ON taskiq_results USING HASH (task_id);

-- scheduler_source: step 1
-- creates table: yes
-- columns: id, task_name, schedule, updated_at, created_at
-- indexes: taskiq_schedulers_id_idx hash (id); taskiq_schedulers_task_name_idx hash (task_name)
CREATE TABLE IF NOT EXISTS taskiq_schedulers (id UUID NOT NULL PRIMARY KEY, task_name VARCHAR(100) NOT NULL, schedule JSONB NOT NULL, updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW());
CREATE INDEX IF NOT EXISTS taskiq_schedulers_id_idx ON taskiq_schedulers USING HASH (id);
CREATE INDEX IF NOT EXISTS taskiq_schedulers_task_name_idx ON taskiq_schedulers USING HASH (task_name);
