.PHONY: help up down reset logs submit-pipeline submit-tiering submit-medallion verify verify-medallion status smoke

TIERING_JOBS ?= 2

help:
	@echo "Usage:"
	@echo "  make up               Start infra (storage/catalog/Fluss/Flink/Trino)"
	@echo "  make submit-pipeline  Submit the Postgres -> Fluss CDC pipeline job"
	@echo "  make submit-tiering   Submit the Fluss -> Iceberg tiering service jobs (TIERING_JOBS, default 2)"
	@echo "  make submit-medallion Submit the bronze -> silver and silver -> gold Flink SQL jobs"
	@echo "  make down             Stop and remove containers (keeps data)"
	@echo "  make reset            Stop, wipe all volumes, and rebuild from scratch"
	@echo "  make logs             Tail all service logs"
	@echo ""
	@echo "  make verify           Row-count check + live insert/update/delete test (bronze)"
	@echo "  make verify-medallion Reconcile gold tables against Postgres + live retraction test"
	@echo "  make status           Flink job states"
	@echo "  make smoke            Full end-to-end smoke test (up + submit + verify)"

# ── infrastructure ────────────────────────────────────────────────────────────

up:
	cp -n .env.example .env 2>/dev/null || true
	docker compose up -d
	@echo ""
	@echo "Infra starting. Once polaris-bootstrap and fluss-coordinator are up, run:"
	@echo "  make submit-pipeline"
	@echo "  make submit-tiering"
	@echo ""
	@echo "  Flink UI:      http://localhost:8082"
	@echo "  Trino UI:      http://localhost:8080"
	@echo "  Polaris:       http://localhost:8181"
	@echo "  SeaweedFS S3:  http://localhost:8333 (Admin UI: :23646)"

down:
	docker compose down

reset:
	docker compose down -v
	@echo "All volumes wiped. Run 'make up' to rebuild from scratch."

logs:
	docker compose logs -f

# ── pipeline jobs ────────────────────────────────────────────────────────────

submit-pipeline:
	docker compose exec -e SOURCE_PG_USER=$${SOURCE_PG_USER:-ecommerce} \
		-e SOURCE_PG_PASSWORD=$${SOURCE_PG_PASSWORD:-ecommerce} \
		-e SOURCE_PG_DB=$${SOURCE_PG_DB:-ecommerce} \
		flink-jobmanager /opt/flink-cdc-pipelines/run-cdc-pipeline.sh

submit-tiering:
	@for i in $$(seq 1 $(TIERING_JOBS)); do \
		docker compose exec flink-jobmanager /opt/flink-cdc-pipelines/run-tiering-service.sh $$i; \
	done

# Guarded: a second run would otherwise start duplicate silver/gold jobs.
submit-medallion:
	@if curl -s http://localhost:8082/jobs/overview | python3 -c \
		"import json,sys; j=json.load(sys.stdin)['jobs']; sys.exit(0 if any(x['name'].startswith('Medallion:') and x['state']=='RUNNING' for x in j) else 1)"; then \
		echo "Medallion jobs already RUNNING -- not resubmitting (see make status)."; \
	else \
		docker compose exec flink-jobmanager /opt/flink-cdc-pipelines/run-medallion.sh; \
	fi

# ── testing ───────────────────────────────────────────────────────────────────

status:
	@curl -s http://localhost:8082/jobs/overview | python3 -m json.tool

verify:
	./scripts/verify_pipeline.sh

verify-medallion:
	./scripts/verify_medallion.sh

smoke: up
	@echo "Waiting for infra to settle..."
	@sleep 60
	$(MAKE) submit-pipeline
	@sleep 30
	$(MAKE) submit-tiering
	$(MAKE) submit-medallion
	@sleep 90
	$(MAKE) verify
	$(MAKE) verify-medallion
