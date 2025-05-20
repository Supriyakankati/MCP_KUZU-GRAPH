const express = require("express");
const { McpServer } = require("@modelcontextprotocol/sdk/server/mcp.js");
const { StreamableHTTPServerTransport } = require("@modelcontextprotocol/sdk/server/streamableHttp.js");
const kuzu = require("kuzu");
const path = require("path");
const fs = require("fs");

const app = express();
app.use(express.json());

const server = new McpServer({ name: "kuzu", version: "1.0.0" });

// Connect to Kuzu
const dbPath = process.argv[2] || process.env.KUZU_DB_PATH;
if (!dbPath || !fs.existsSync(dbPath)) {
  console.error("❌ Kuzu DB path invalid or missing.");
  process.exit(1);
}
const resolvedPath = path.resolve(dbPath);
const db = new kuzu.Database(resolvedPath);
const conn = new kuzu.Connection(db);
global.conn = conn;

// Tools
server.tool("query", { cypher: "string" }, async ({ cypher }) => {
  const result = await conn.query(cypher);
  const rows = await result.getAll();
  result.close();
  return {
    content: [{ type: "text", text: JSON.stringify(rows, null, 2) }]
  };
});

// Prompt
server.prompt("generateKuzuCypher", { question: "string" }, async ({ question }) => {
  return {
    messages: [{
      role: "user",
      content: { type: "text", text: question }
    }]
  };
});

// ✅ Use streamable HTTP transport
const transport = new StreamableHTTPServerTransport(app, "/message");
server.connect(transport);

// Start Express
const PORT = 3002;
app.listen(PORT, () => {
  console.log(`✅ MCP server running at http://localhost:${PORT}/message`);
});