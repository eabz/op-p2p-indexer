# Security policy

This early-stage project has no security audit or guaranteed support window. Use an explicitly selected release, restrict API access and review the documented trust boundaries before processing settlement-sensitive data.

## Report a vulnerability

Use the repository's **Security → Report a vulnerability** private reporting route if available. If the private reporting button is unavailable, open an issue asking the maintainer for a private contact **without publishing the vulnerability or exploit details**. No dedicated security mailbox or response-time commitment is established yet.

In a private report, include affected versions, chain/role, impact, reproduction and any suggested mitigation. Remove credentials and personal information. Coordinate public disclosure with maintainers after a fix or mitigation is available.

## Deployment basics

The API has optional bearer authentication and no built-in TLS. Keep it on loopback or a private network, or terminate TLS with an appropriate proxy. Protect TOML config files and object-store credentials. Cryptographic data integrity checks are not EVM execution validation; safe/finalized labels have the limits described in the trust guide.
