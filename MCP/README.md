# Model Context Protocol (MCP): A Comprehensive Overview

## Introduction
The Model Context Protocol (MCP) is an open-source standard developed by Anthropic and launched in November 2024. It enables AI models and applications to securely connect to and interact with external data sources, tools, and workflows through a standardized interface. MCP acts as a universal "USB-C" port for AI, allowing seamless integrations without the need for custom code for each connection. This protocol builds on existing concepts like tool use and function calling but standardizes them, reducing the fragmentation in AI integrations. By providing access to live, real-world data, MCP empowers large language models (LLMs) like Claude to perform tasks, deliver accurate insights, and handle actions that extend beyond their original training data.

MCP supports connections to various external systems, including local files, databases, search engines, calendars, content repositories (e.g., Google Drive, Notion), collaboration tools (e.g., Slack), code repositories (e.g., GitHub, Git), and even creative software (e.g., Figma, Blender). Pre-built MCP servers are available for many of these systems, accelerating adoption. Early adopters include companies like Block, Apollo, Zed, Replit, Codeium, and Sourcegraph, which use MCP to enhance AI agents in development tools and enterprise systems.

## Purpose and Benefits
MCP addresses the challenge of AI models being isolated from real-time data and external capabilities. It enables LLMs to:
- Access current data from diverse sources.
- Perform actions on behalf of users, such as querying databases or sending emails.
- Utilize specialized tools and workflows without custom integrations.

Key benefits include:
- **Standardization**: Developers build against a single protocol, eliminating the need for separate connectors for each AI model or data source.
- **Efficiency**: Reduces latency and costs by optimizing data transfer and query execution.
- **Scalability**: Supports composable integrations, allowing AI agents to handle complex tasks across multiple systems.
- **Enhanced Capabilities**: Enables real-time analytics, natural language processing (e.g., translating queries to SQL), and cross-system operations.

In enterprise contexts, MCP facilitates secure access to structured data, empowering AI agents to deliver context-aware insights directly from live sources.

## Architecture and Components
MCP's architecture facilitates bidirectional communication between AI applications and external systems using JSON-RPC 2.0 messages. It draws inspiration from the Language Server Protocol (LSP) and includes the following core components:

- **MCP Host**: The AI application or environment (e.g., an AI-powered IDE, conversational assistant like Claude, or desktop app) that contains the LLM. This is the primary user interaction point, where the host processes requests that may require external data or tools.

- **MCP Client**: Embedded within the host, the client acts as a connector. It translates the LLM's requests into MCP-compatible formats, discovers available servers, and handles responses from servers to the LLM.

- **MCP Server**: An external service that exposes data, tools, or capabilities to the client. Servers connect to backend systems (e.g., databases, APIs) and format responses for the LLM. They provide diverse functionalities and can be built or used off-the-shelf.

### Transport Layer
Communication occurs over stateful connections using JSON-RPC 2.0. Common transport methods include:
- **Standard Input/Output (stdio)**: Ideal for local resources, enabling fast, synchronous messaging.
- **Server-Sent Events (SSE)**: Suited for remote resources, supporting efficient, real-time streaming.

The protocol involves capability negotiation between clients and servers to ensure compatibility.

## Building Blocks
MCP servers expose capabilities through three primary building blocks, which standardize how AI applications interact with external systems:

| Feature   | Explanation                                                                 | Examples                          | Who Controls It |
|-----------|-----------------------------------------------------------------------------|-----------------------------------|-----------------|
| **Tools** | Active functions that the LLM can invoke based on user requests. These can perform actions like writing to databases, calling APIs, or modifying files. Hosts must obtain user consent before invocation. | Search flights, send messages, create calendar events | Model (LLM decides when to call) |
| **Resources** | Passive, read-only data sources providing context, such as file contents, database schemas, or API documentation. | Retrieve documents, access knowledge bases, read calendars | Application (host manages access) |
| **Prompts** | Pre-built templates or workflows that guide the LLM in using tools and resources effectively. | Plan a vacation, summarize meetings, draft an email | User (selects or customizes) |

Additionally, clients may offer features back to servers, such as sampling (recursive LLM interactions), roots (filesystem boundaries), or elicitation (requests for more user info), all requiring user approval.

## How MCP Works
At its core, MCP allows an LLM to request assistance from external systems to fulfill user queries. The process involves discovery, invocation, execution, and response.

### Simplified Workflow Example
Consider a user query: "Find the latest sales report in our database and email it to my manager."

1. **Request and Discovery**: The LLM recognizes it needs external access (e.g., database query and email sending). Via the MCP client, it discovers available servers and relevant tools, such as `database_query` and `email_sender`.

2. **Tool Invocation**: The LLM generates a structured request. The client sends it to the appropriate server (e.g., first invoking `database_query` with the report details).

3. **External Action and Response**: The server translates the request (e.g., into a secure SQL query), executes it on the backend system, retrieves the data, and returns it in a formatted response to the client.

4. **Subsequent Actions**: With the data, the LLM invokes the next tool (e.g., `email_sender`), and the server confirms completion.

5. **Final Response**: The LLM replies to the user: "I have found the latest sales report and emailed it to your manager."

This bidirectional flow ensures efficient, secure interactions. Real-world examples include generating web apps from Figma designs, analyzing data across multiple databases via natural language, or creating 3D models in Blender for printing.

## MCP for Databases
In database contexts, MCP servers act as connectors to structured data across systems like PostgreSQL, MySQL, Oracle, MongoDB, Snowflake, and others. AI applications can query, analyze, and retrieve data as if from a single source, without custom integrations. Features include:
- Natural language to SQL translation.
- Cross-database querying.
- Real-time analytics.

Queries execute efficiently at the source, minimizing data transfer.

## Security Features
MCP prioritizes enterprise-grade security through:
- **User Consent and Control**: Explicit approval required for data access, tool invocations, and operations. Hosts provide clear UIs for review.
- **Data Privacy**: Access controls prevent unauthorized transmission of user data or resources.
- **Tool Safety**: Descriptions are treated as untrusted; consent is mandatory for executions that could run arbitrary code.
- **Additional Measures**: Role-based access controls (RBAC), query monitoring, auditing, secure credential management, and best practices like sandboxing.

## Recent Advancements (as of 2025)
By 2025, MCP has seen widespread adoption, with thousands of servers and SDKs available in major languages. A key innovation is code execution with MCP, which treats tools as code APIs for more efficient AI agents. This reduces token usage (e.g., by 98% in complex tasks) through progressive tool discovery, context-efficient results, advanced control flows (e.g., loops), privacy-preserving operations (e.g., data tokenization), and state persistence. It enables agents to handle large-scale integrations without bloating context windows, improving latency and cost.