.PHONY: up down reset install test unit pipeline tf-init tf-apply tf-destroy

up:        ; docker compose up -d
down:      ; docker compose down
reset:     ; docker compose down -v && docker compose up -d
install:   ; python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
test:      ; pytest                       # all tests against $$AWS_ENDPOINT_URL (Floci)
unit:      ; pytest tests/test_01_iam_s3.py tests/test_02_sqs_sns.py tests/test_03_data_services.py
pipeline:  ; python -m src.bootstrap      # create everything with boto3
tf-init:   ; cd terraform && terraform init
tf-apply:  ; cd terraform && terraform apply -auto-approve
tf-destroy:; cd terraform && terraform destroy -auto-approve
