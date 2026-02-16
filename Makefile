# Makefile for CAT Demo Project

.PHONY: help local-up local-down local-topics local-generate aws-deploy aws-destroy aws-topics aws-generate clean

help:
	@echo "CAT Demo Project - Available Commands:"
	@echo ""
	@echo "Local Development:"
	@echo "  make local-up        - Start local Docker environment"
	@echo "  make local-down      - Stop local Docker environment"
	@echo "  make local-topics    - Create Kafka topics locally"
	@echo "  make local-generate  - Run event generator locally"
	@echo "  make local-spark     - Run Spark job locally"
	@echo ""
	@echo "AWS Deployment:"
	@echo "  make aws-deploy      - Deploy infrastructure to AWS"
	@echo "  make aws-topics      - Create Kafka topics on AWS MSK"
	@echo "  make aws-spark       - Deploy Spark job to EMR Serverless"
	@echo "  make aws-generate    - Start event generator on AWS"
	@echo "  make aws-destroy     - Destroy AWS infrastructure"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean           - Clean up local artifacts"
	@echo "  make docs            - Build documentation"
	@echo "  make test            - Run tests"

# Local Development
local-up:
	@echo "Starting local Docker environment..."
	cd local && docker-compose up -d
	@echo "Waiting for services to be ready..."
	sleep 30
	@echo "Services ready!"
	@echo "Kafka UI: http://localhost:8080"
	@echo "Spark UI: http://localhost:4040"

local-down:
	@echo "Stopping local Docker environment..."
	cd local && docker-compose down

local-topics:
	@echo "Creating Kafka topics..."
	KAFKA_BROKER=localhost:9092 ./tools/create_topics.sh

local-generate:
	@echo "Running event generator..."
	cd services/event_generator && \
	python generator.py --bootstrap-servers localhost:9092 --mode normal --duration 60 --rate 2.0

local-spark:
	@echo "Running Spark job locally..."
	docker exec -it cat-spark spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
		--conf spark.sql.streaming.checkpointLocation=/opt/spark-checkpoints/lifecycle-job \
		/opt/spark-apps/lifecycle_streaming.py

# AWS Deployment
aws-deploy:
	@echo "Deploying to AWS..."
	cd terraform/envs/dev && \
	terraform init && \
	terraform apply

aws-topics:
	@echo "Creating Kafka topics on AWS MSK..."
	@BOOTSTRAP_SERVERS=$$(cd terraform/envs/dev && terraform output -raw msk_bootstrap_servers) && \
	KAFKA_BROKER=$$BOOTSTRAP_SERVERS ./tools/create_topics.sh

aws-spark:
	@echo "Deploying Spark job to EMR Serverless..."
	cd terraform/envs/dev && bash deploy_spark_job.sh

aws-generate:
	@echo "Starting event generator on AWS..."
	@API_URL=$$(cd terraform/envs/dev && terraform output -raw api_gateway_url) && \
	curl -X POST $$API_URL/generator/start \
		-H "Content-Type: application/json" \
		-d '{"mode": "normal", "duration": 300, "rate": 2.0}'

aws-destroy:
	@echo "WARNING: This will destroy all AWS resources!"
	@echo "Press Ctrl+C to cancel, or Enter to continue..."
	@read confirm
	cd terraform/envs/dev && terraform destroy

# Utilities
clean:
	@echo "Cleaning up local artifacts..."
	rm -rf local/data/*
	rm -rf local/checkpoints/*
	rm -rf spark/lifecycle_job/__pycache__
	rm -rf services/event_generator/__pycache__
	rm -rf services/operator_api/__pycache__
	find . -name "*.pyc" -delete
	find . -name ".DS_Store" -delete

docs:
	@echo "Documentation available in docs/"
	@echo "Blog post available in blog/posts/"

test:
	@echo "Running tests..."
	@echo "No tests implemented yet. See docs/10-exercises.md for test exercises."

# Quick start commands
quickstart-local: local-up local-topics
	@echo ""
	@echo "Local environment ready!"
	@echo "Next steps:"
	@echo "  1. Run Spark job: make local-spark"
	@echo "  2. Generate events: make local-generate"
	@echo "  3. View Kafka UI: http://localhost:8080"

quickstart-aws: aws-deploy aws-topics aws-spark aws-generate
	@echo ""
	@echo "AWS environment ready and generating events!"
	@echo "Next steps:"
	@echo "  1. View logs: aws logs tail /aws/emr-serverless/applications/\$\$EMR_APP_ID --follow"
	@echo "  2. Query lifecycles: See docs/08-observe-and-query.md"
	@echo "  3. Run demo: See docs/07-run-demo.md"
