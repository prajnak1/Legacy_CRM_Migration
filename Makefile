.PHONY: db source control migrate resume frontend

db:
	docker compose up -d db

source:
	go run ./cmd/legacy-api

control:
	go run ./cmd/migrator serve

migrate:
	go run ./cmd/migrator migrate

resume:
	go run ./cmd/migrator migrate --resume

frontend:
	cd web && npm start
