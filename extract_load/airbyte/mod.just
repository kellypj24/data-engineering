terraform_dir := source_directory() / "terraform"

# Full bootstrap: install prereqs, start Airbyte, capture credentials, init Terraform
setup:
    #!/usr/bin/env bash
    set -euo pipefail

    echo "==> Checking prerequisites..."

    if ! command -v brew &>/dev/null; then
        echo "Error: Homebrew is required. Install from https://brew.sh" >&2
        exit 1
    fi

    if ! command -v terraform &>/dev/null; then
        echo "==> Installing Terraform via Homebrew..."
        brew install hashicorp/tap/terraform
    fi

    if ! command -v abctl &>/dev/null; then
        echo "==> Installing abctl via Homebrew..."
        brew install airbytehq/tap/abctl
    fi

    echo "==> Starting Airbyte (this may take a few minutes on first run)..."
    abctl local install

    echo "==> Capturing credentials..."
    just airbyte::credentials

    echo "==> Initializing Terraform..."
    terraform -chdir="{{ terraform_dir }}" init

    echo ""
    echo "Setup complete! Next steps:"
    echo "  1. Open http://localhost:8006 and copy your workspace ID from Settings"
    echo "  2. Add workspace_id to {{ terraform_dir }}/terraform.tfvars"
    echo "  3. Run: just airbyte::plan"

# Capture abctl credentials and write terraform.tfvars
credentials:
    #!/usr/bin/env bash
    set -euo pipefail

    echo "==> Reading credentials from abctl..."
    creds_output=$(abctl local credentials 2>&1)

    client_id=$(echo "$creds_output" | grep -i "client-id" | awk '{print $NF}')
    client_secret=$(echo "$creds_output" | grep -i "client-secret" | awk '{print $NF}')

    if [[ -z "$client_id" || -z "$client_secret" ]]; then
        echo "Error: Could not parse credentials from abctl output:" >&2
        echo "$creds_output" >&2
        exit 1
    fi

    tfvars_file="{{ terraform_dir }}/terraform.tfvars"

    # Preserve workspace_id if it exists in current tfvars
    workspace_id=""
    if [[ -f "$tfvars_file" ]]; then
        workspace_id=$(grep -E '^workspace_id' "$tfvars_file" | sed 's/.*= *"//' | sed 's/"//' || true)
    fi

    printf 'airbyte_client_id     = "%s"\nairbyte_client_secret = "%s"\n' "$client_id" "$client_secret" > "$tfvars_file"

    if [[ -n "$workspace_id" ]]; then
        echo "workspace_id          = \"$workspace_id\"" >> "$tfvars_file"
    else
        echo "# workspace_id        = \"your-workspace-id\"  # Get from Airbyte UI → Settings" >> "$tfvars_file"
    fi

    echo "Credentials written to $tfvars_file"

# Validate Terraform configuration
validate:
    terraform -chdir="{{ terraform_dir }}" validate

# Plan Terraform changes
plan:
    terraform -chdir="{{ terraform_dir }}" plan

# Apply Terraform changes
apply:
    terraform -chdir="{{ terraform_dir }}" apply

# Run Terraform tests
test:
    terraform -chdir="{{ terraform_dir }}" test
