# Validate Terraform configuration
validate:
    cd {{source_directory()}}/terraform && terraform validate

# Plan Terraform changes
plan:
    cd {{source_directory()}}/terraform && terraform plan

# Apply Terraform changes
apply:
    cd {{source_directory()}}/terraform && terraform apply

# Run Terraform tests
test:
    cd {{source_directory()}}/terraform && terraform test
