# Product direction

## Audience and job

Start with developer teams building indexers and analytics. Their job is to obtain a useful dataset, keep it correct as receipts and reorganizations arrive, and recover without rebuilding their pipeline. The website should help them decide whether this data source fits that job and reach a small successful read quickly.

The promise is operational control: history and live events from a source the team can inspect and run. Explain the workflow and its limits rather than the author's implementation choices. Keep installation and documentation as the primary actions. Show a real example before asking readers to understand crate architecture.

## Distinctive value to prove

- Independently operable peer ingestion: runtime collection without an L1/L2 RPC dependency.
- One application-facing data source for historical ranges and live updates.
- Explicit integrity and trust boundaries rather than an unexplained “verified” label.
- Local and fleet deployment paths that preserve the application's control of its data.
- Open code, inspectable protocol and reproducible evidence for decisions.

These are product properties to validate with users, not demonstrated competitive advantages over named alternatives. No comparison or speed claim should be published without equivalent workloads and documented methodology.

## Website and docs boundary

The landing page explains outcomes, shows the first read, and links to evidence. Task guides cover install, build, operate and trust. Engineering specs remain available without dominating the first experience. Public instructions select a release; development changes are clearly separated. Markdown sources, protocol downloads and agent indexes remain available without a browser session.

The public project is MIT-licensed and maintainer-led. Contributor copyright, license, proposal/review process and decision rationale are documented. Ownership arrangements remain open; the website must not assert community ownership before a defined structure is adopted.

## A possible hosted service

Validate demand for managed operation before building accounts or billing. A hosted offering could sell useful coverage, predictable recovery, provisioning and support around the same open protocol. It should keep documented export and self-hosting paths so users can move their workloads. Do not promise an SLA before sustained reliability evidence exists.

Interview pilot teams using one bounded dataset. Record time to first query, receipt completeness, reorg handling, restore/resume effort, operating cost and support burden. Decide whether teams value managed service, an easier self-hosting path, or both. A sign-up funnel is not evidence of a working service.

## Acceptance and next gates

Website acceptance: clear install/docs actions, functional clipboard, responsive layout, accessible controls, useful search, valid links, release-aligned installer/reference, downloadable examples and readable agent exports.

Documentation acceptance: a small data path with prerequisites and observable success, explicit limits/finality, operational recovery guidance and preserved reference/history. Local fixture checks validate the example contract but do not substitute for a real-chain run.

Production acceptance remains a separate operational effort: fresh Ubuntu setup, complete chain coverage, safe-head reconciliation, claim selection, exporter continuity, consumer failover, backup restore and sustained peer serving. Keep status tied to recorded evidence and update the public readiness page as each gate is demonstrated.
