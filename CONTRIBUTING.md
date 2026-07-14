# Contributing to TeaQL

First off, thank you for considering contributing to TeaQL! It's people like you that make TeaQL such a great tool.

## Code of Conduct

By participating in this project, you are expected to uphold our [Code of Conduct](CODE_OF_CONDUCT.md).

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check the existing issues as you might find out that you don't need to create one. When you are creating a bug report, please include as many details as possible:
* Use a clear and descriptive title.
* Describe the exact steps which reproduce the problem.
* Provide specific examples to demonstrate the steps.

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, please provide:
* A clear and descriptive title.
* A step-by-step description of the suggested enhancement.
* Explain why this enhancement would be useful.

### Pull Requests

1. Fork the repository and create your branch from `main`.
2. If you've added code that should be tested, add tests.
3. Ensure the test suite passes.
4. Make sure your code conforms to our coding standards.
5. Issue that pull request!

## Coding Standards & Requirements for Acceptable Contributions

To ensure consistency and quality, all contributions must adhere to the following requirements:

1. **Rust Formatting**: All code must be formatted using `cargo fmt`. Our CI pipeline enforces this.
2. **Clippy Lints**: Code must pass `cargo clippy` without any warnings.
3. **Tests**: Any new functionality MUST include corresponding tests. Existing tests must not break.
4. **Safety**: Avoid `unsafe` Rust code unless absolutely necessary and thoroughly documented.
5. **Commit Messages**: Use clear and descriptive commit messages. We recommend [Conventional Commits](https://www.conventionalcommits.org/).

## Development Setup

Please refer to the `README.md` for instructions on how to build and run the project locally.
