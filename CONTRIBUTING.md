# Contributing

Start with the [documentation](https://eabz.github.io/op-p2p-indexer/docs/) and the engineering [roadmap](docs/roadmap.md). The project welcomes indexer and analytics workloads, reproducible bug reports, operational evidence and focused improvements.

## Propose or report

Search existing issues first. Include the release/commit, chain, role, expected behavior and minimal reproduction. For data problems, include block ranges/hashes and sanitized logs. Do not include config secrets, API keys, private peer identities or credentials.

Discuss broad architectural changes in an issue before implementation. Explain the user outcome and tradeoffs, and record accepted decisions in `docs/decisions.md`. Maintainers review and merge changes; substantial changes should have a public rationale. Response times are not guaranteed.

## Send a change

Keep pull requests focused and describe what changed, why and how you verified it. Preserve crate dependency boundaries documented in `CLAUDE.md`. Follow existing Rust style and workspace lints. The project currently verifies Rust changes through checks and running nodes rather than adding test modules.

For website/docs changes, follow `website/README.md`, build in strict mode, check local links and verify mobile/desktop behavior. Documentation must state its release baseline. Do not present planned features or a one-off benchmark as production guarantees.

## Rights and decisions

Contributors retain copyright and submit work under the MIT license. No copyright assignment is required by this guide. Submitting work does not grant merge rights, ownership of the project or a promise that it will be accepted. The project is currently maintainer-led; governance proposals and their rationale should be discussed publicly.

Report vulnerabilities through [SECURITY.md](SECURITY.md), rather than publishing exploit details in an ordinary issue.
