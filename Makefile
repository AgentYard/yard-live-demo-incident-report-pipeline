build:
	docker build -t yard-live-demo-incident-report-pipeline:latest .

run:
	docker run --env-file .env -p 9000:9000 yard-live-demo-incident-report-pipeline:latest

test:
	docker run --rm yard-live-demo-incident-report-pipeline:latest python -c "print('smoke test passed')"

health:
	curl -f http://localhost:9000/health
