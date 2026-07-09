## Security Policy

### Overview

This repository contains a reference implementation of an MCP (Model Context Protocol) server for accessing CONNX databases. It is intended for demonstrations, proof-of-concepts, and learning purposes.

While security best practices are followed, this project is not a hardened production system. Users are responsible for evaluating and extending the security controls to meet their own organizational and regulatory requirements.

---

### Supported Versions

Only the latest version on the main branch is supported.

Security fixes will be applied to main only.
No backporting is provided.

---

### Security Design Principles

This project follows these guiding principles:
* 	Safe by default
* 	Least privilege
* 	Read-only data access
* 	No trust in AI-generated SQL
* 	Defense in depth

---

### Authentication & Credentials
- Database credentials are supplied only via environment variables
- .env files are supported for local development
- Credentials are never logged
- No credentials are committed to source control

Recommended:
- Use OS-level environment variables in production
- Use secret managers (Azure Key Vault, AWS Secrets Manager, etc.)

---

### Database Access Controls

Guardrails should be implemented in more than one layer:

1. **CONNX CDD and database permissions**
2. **MCP server code**
3. **MCP host configuration (for example, Claude Desktop)**

The strongest controls belong in the first two layers. The MCP host is useful for setup and routing, but it should not be treated as the primary enforcement boundary for data protection.

Read Operations
* 	query_connx allows SELECT statements only
* 	Multi-statement execution is blocked
* 	Semicolons are rejected to prevent batching
* 	Queries must produce a result set
*   Purpose-built tools use fixed SQL patterns for common lookups and counts

Write Operations
* 	INSERT, UPDATE, and DELETE tools are not exposed by this server
* 	The MCP interface is intentionally read-only to prevent accidental data changes
* 	Use database credentials with SELECT-only permissions where possible
*   Configure CONNX and the target database so writes are blocked even if a server-side bug is introduced

---

SQL Injection Protection

This project does not rely on regex-based SQL sanitization.

Instead, it enforces safety using:
* 	Parameterized queries for user-supplied values
* 	Strict query classification (SELECT vs non-SELECT)
* 	Single-statement enforcement
* 	Purpose-built tools (e.g., find_customers) instead of raw SQL
*   Query result limits to reduce accidental over-fetching

Regex sanitization is intentionally avoided, as it is brittle and unsafe when used as a primary defense mechanism.

---

Logging & Observability
* 	SQL text is never logged
* 	Each query is logged using a hashed fingerprint
*   Logs include:
  * - Operation type
* 	- Row count (where applicable)
* 	- Error metadata (no sensitive data)

Example:

Query OK fp=3a1c9f82a2d1 rows=42


---

Guardrail Placement

**Recommended enforcement order**

1. **Database and CONNX configuration**
   * Use read-only accounts wherever possible
   * Restrict exposed objects in the CDD to only the datasets and tables required
   * Keep permissions limited to the smallest practical schema and object set

2. **MCP server code**
   * Enforce read-only behavior in code, even when the backend is already read-only
   * Validate raw SQL entry points before execution
   * Prefer purpose-built tools over open-ended SQL for common user tasks
   * Cap result sizes and fail safely on invalid requests
   * Add structured audit logging without logging raw SQL text or secrets

3. **Claude Desktop or another MCP host**
   * Use the host to decide which servers are available to a user
   * Use the host to provide environment variables and process isolation
   * Use the host to steer the model toward safer tools
   * Do not rely on the host as the only guardrail

**Why the MCP host is not enough**

Claude Desktop can help reduce exposure by controlling which MCP servers are launched and by keeping the runtime isolated to a local process. That is useful, but it is still a soft boundary:

* 	The host does not replace database permissions
* 	The host does not make unsafe server code safe
* 	The host should not be the only place that decides whether a query is acceptable

For that reason, the server should assume the host may pass through any valid tool invocation and should enforce its own rules accordingly.

---

MCP Host Trust Boundary

This MCP server trusts the MCP host to:
* 	Obtain user consent before invoking tools
* 	Restrict which MCP servers are available
* 	Manage user authentication and authorization

The server itself does not implement:
* 	User authentication
* 	Role-based access control
* 	Rate limiting

These must be handled by the MCP host or surrounding infrastructure.

At the same time, the server should still enforce its own safety constraints around query shape, execution mode, and result size. Host controls and server controls are complementary, not interchangeable.

---

Network Security

Recommendations for production deployments:
* 	Run the MCP server on the same host as the MCP client when possible
* 	Use VPNs or private networks for database access
* 	Restrict outbound connectivity using firewall rules
* 	Ensure CONNX endpoints are not publicly accessible

---

Denial of Service Considerations

This reference implementation does not include:
* 	Rate limiting
* 	Query cost estimation
* 	Timeout enforcement per query

For production use, consider:
*	Query execution timeouts
* 	Result row limits
* 	MCP host-level rate controls
*   Server-side concurrency controls
*   Per-tool execution budgets for expensive operations

---

Current Repository Behavior

At the time of writing, the repository already includes several useful server-side guardrails:

* 	`query_connx` in both `connx_server.py` and `connx_server_adabas.py` rejects non-SELECT statements
* 	Raw query entry points reject semicolons and multi-statement batches
* 	Purpose-built tools use fixed SQL with parameter binding for user-provided values
* 	Queries are logged by fingerprint rather than by raw SQL text
* 	Result sets are capped with `CONNX_MAX_ROWS`

These controls are appropriate for a reference implementation, but they should be treated as a starting point rather than a complete production posture.

One important design note: some raw-SQL guardrails are currently enforced at the `query_connx` tool layer rather than inside the lowest execution helper. For production-strength deployments, consider centralizing more of that validation in shared execution paths so future tools cannot bypass it accidentally.

---

Recommended Guardrails for Future Hardening

For teams extending this server, the next useful controls are:

* 	Table and schema allowlists for raw SQL tools
* 	Centralized query validation shared by all execution paths
* 	Server-side rate limiting and concurrency control
* 	Query timeout enforcement at the cursor or connection layer
* 	Per-tool row limits that are stricter than a single global default where appropriate
* 	Structured audit events for tool name, query fingerprint, row count, and failure reason
* 	Authentication and authorization in the surrounding runtime if multiple users share the same deployment

For higher-trust environments, prefer replacing open-ended SQL tools with narrower business tools wherever practical.

---

Vulnerability Reporting

If you discover a security issue:
* 	Do not open a public GitHub issue
* 	Contact the repository maintainer directly
* 	Provide:
*    -  Description of the issue
* 	-	Steps to reproduce
* 	-	Potential impact

Reported issues will be reviewed and addressed on a best-effort basis.

---

Disclaimer

This project is provided as-is, without warranty of any kind.

It is intended as:
* 	A learning resource
* 	A reference implementation
* 	A starting point for secure MCP server development

It is not intended to replace enterprise-grade security controls.

---

Recommended Next Steps

For teams building on this example:
* 	Add authentication and authorization
* 	Implement rate limiting
* 	Introduce query whitelisting
* 	Integrate audit logging
* 	Perform a security review before production use
