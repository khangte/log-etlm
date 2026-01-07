# spark_job/main.py
# spark-submit 진입점
# event_ingest_job.run_event_ingest로 위임한다.

from .fact.jobs.ingest_job import run_event_ingest


if __name__ == "__main__":
    run_event_ingest()
