# Validate Terraform configuration
validate:
    cd {{justfile_directory()}}/terraform && terraform validate

# Plan Terraform changes
plan:
    cd {{justfile_directory()}}/terraform && terraform plan

# Apply Terraform changes
apply:
    cd {{justfile_directory()}}/terraform && terraform apply

# Run Terraform tests
test:
    cd {{justfile_directory()}}/terraform && terraform test
