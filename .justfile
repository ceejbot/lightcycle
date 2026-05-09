
# List available recipes.
help:
    @just -l

# Run all tests using nextest.
test:
    cargo nextest run

# Run the same checks we run in CI. Requires nightly.
ci: test
    cargo clippy
    cargo +nightly fmt --check

fmt:
    cargo +nightly fmt

# Ask for clippy's opinion.
lint:
    cargo clippy --fix
    cargo +nightly fmt

# Install required tools
setup:
    brew tap ceejbot/tap
    brew install fzf cargo-nextest tomato semver-bump

# Build docs and open them in your browser.
docs:
    @cargo doc --no-deps --open

# Tag a new version for release with changelog update.
version BUMP:
    #!/usr/bin/env bash
    set -e
    current=$(tomato get package.version Cargo.toml)
    version=$(semver-bump {{ BUMP }} "$current")
    # Update version in Cargo.toml
    tomato set package.version "$version" Cargo.toml &> /dev/null
    git add Cargo.toml
    git commit -m "v${version}"
    git tag "v${version}"
