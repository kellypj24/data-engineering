# Validate Terraform configuration
validate:
    cd terraform && terraform validate

# Plan Terraform changes
plan:
    cd terraform && terraform plan

# Apply Terraform changes
apply:
    cd terraform && terraform apply

# Run Terraform tests
test:
    cd terraform && terraform test
