CREATE TABLE IF NOT EXISTS pipeline_runs (
  id SERIAL PRIMARY KEY,
  job_name TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('STARTED','SUCCESS','FAILED')),
  start_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  end_time TIMESTAMPTZ,
  input_path TEXT,
  output_path TEXT,
  records_in BIGINT,
  records_out BIGINT,
  message TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_job_name ON pipeline_runs(job_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_start_time ON pipeline_runs(start_time);
