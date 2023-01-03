
# List available recipes.
help:
	@just -l

# Run all tests with nextest
test:
	@cargo nextest run

# Lint and stuff.
@ci:
	cargo fmt
	cargo clippy --all-targets --fix --allow-dirty --allow-staged
	cargo nextest run

# Build docs and open them in your browser.
docs:
	@cargo doc --no-deps --open

# Cargo install required tools like `nextest`
@install-tools:
	cargo install cargo-nextest
	cargo install tomato

# Use tomato to set the crate version to the passed in version, commit,
# and create a git tag `v{version}`. Will not act if there are uncommitted
# changes extant.

# Set the crate version and tag the repo to match.
tag VERSION:
	#!/usr/bin/env bash
	status=$(git status --porcelain)
	if [ "$status" != ""  ]; then
		echo "There are uncommitted changes! Cowardly refusing to act."
		exit 1
	fi
	tomato set package.version {{VERSION}} Cargo.toml
	# update the lock file
	cargo check
	git commit Cargo.toml Cargo.lock -m "v{{VERSION}}"
	git tag "v{{VERSION}}"
	echo "Release tagged for version v{{VERSION}}"
