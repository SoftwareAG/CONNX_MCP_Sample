## Security Policy

### Overview

This repository contains a reference implementation of an MCP (Model Context Protocol) server for accessing CONNX databases. It is intended for demonstrations, proof-of-concepts, and learning purposes.

While security best practices are followed, this project is not a hardened production system. Users are responsible for evaluating and extending the security controls to meet their own organizational and regulatory requirements.

⸻

### Supported Versions

Only the latest version on the main branch is supported.

Security fixes will be applied to main only.
No backporting is provided.

⸻

### Security Design Principles

This project follows these guiding principles:
	•	Safe by default
	•	Least privilege
	•	Explicit enablement for destructive actions
	•	No trust in AI-generated SQL
	•	Defense in depth

⸻

### Authentication & Credentials
- Database credentials are supplied only via environment variables
- .env files are supported for local development
- Credentials are never logged
- No credentials are committed to source control

Recommended:
- Use OS-level environment variables in production
- Use secret managers (Azure Key Vault, AWS Secrets Manager, etc.)

⸻

### Database Access Controls

Read Operations
	•	query_connx allows SELECT statements only
	•	Multi-statement execution is blocked
	•	Semicolons are rejected to prevent batching
	•	Queries must produce a result set

Write Operations
	•	Writes are disabled by default
	•	Enabling writes requires:

CONNX_ALLOW_WRITES=true


	•	Only INSERT, UPDATE, and DELETE operations are permitted
	•	Single-statement enforcement applies to all write queries

⸻

SQL Injection Protection

This project does not rely on regex-based SQL sanitization.

Instead, it enforces safety using:
	•	Parameterized queries for user-supplied values
	•	Strict query classification (SELECT vs non-SELECT)
	•	Single-statement enforcement
	•	Purpose-built tools (e.g., find_customers) instead of raw SQL

Regex sanitization is intentionally avoided, as it is brittle and unsafe when used as a primary defense mechanism.

⸻

Logging & Observability
	•	SQL text is never logged
	•	Each query is logged using a hashed fingerprint
	•	Logs include:
	•	Operation type
	•	Row count (where applicable)
	•	Error metadata (no sensitive data)

Example:

Query OK fp=3a1c9f82a2d1 rows=42


⸻

MCP Host Trust Boundary

This MCP server trusts the MCP host to:
	•	Obtain user consent before invoking tools
	•	Restrict which MCP servers are available
	•	Manage user authentication and authorization

The server itself does not implement:
	•	User authentication
	•	Role-based access control
	•	Rate limiting

These must be handled by the MCP host or surrounding infrastructure.

⸻

Network Security

Recommendations for production deployments:
	•	Run the MCP server on the same host as the MCP client when possible
	•	Use VPNs or private networks for database access
	•	Restrict outbound connectivity using firewall rules
	•	Ensure CONNX endpoints are not publicly accessible

⸻

Denial of Service Considerations

This reference implementation does not include:
	•	Rate limiting
	•	Query cost estimation
	•	Timeout enforcement per query

For production use, consider:
	•	Query execution timeouts
	•	Result row limits
	•	MCP host-level rate controls

⸻

Vulnerability Reporting

If you discover a security issue:
	1.	Do not open a public GitHub issue
	2.	Contact the repository maintainer directly
	3.	Provide:
	•	Description of the issue
	•	Steps to reproduce
	•	Potential impact

Reported issues will be reviewed and addressed on a best-effort basis.

⸻

Disclaimer

This project is provided as-is, without warranty of any kind.

It is intended as:
	•	A learning resource
	•	A reference implementation
	•	A starting point for secure MCP server development

It is not intended to replace enterprise-grade security controls.

⸻

Recommended Next Steps

For teams building on this example:
	•	Add authentication and authorization
	•	Implement rate limiting
	•	Introduce query whitelisting
	•	Integrate audit logging
	•	Perform a security review before production use
