import { spawn } from "child_process";

export async function run(input, sdk) {
  // Helper function to convert byte array/object to string
  function bytesToString(data) {
    if (typeof data === 'string') return data;
    if (Array.isArray(data)) {
      return data.map(byte => String.fromCharCode(byte)).join('');
    }
    if (data && typeof data === 'object') {
      // Handle object with numeric keys (character codes)
      const keys = Object.keys(data).filter(k => !isNaN(parseInt(k))).sort((a, b) => parseInt(a) - parseInt(b));
      if (keys.length > 0) {
        return keys.map(k => String.fromCharCode(data[k])).join('');
      }
      // Handle data property
      if (data.data) {
        return bytesToString(data.data);
      }
      // Handle value property
      if (data.value) {
        return bytesToString(data.value);
      }
    }
    return "";
  }
  
  let filterClause = "";
  
  // Try to extract the query from different input formats
  if (typeof input === 'string') {
    filterClause = input;
    sdk.console.log("[delete_by_httpql] Received query as string");
  } else if (input && typeof input === 'object') {
    // Try data property (from Trim node)
    if (input.data) {
      filterClause = bytesToString(input.data);
      sdk.console.log("[delete_by_httpql] Extracted query from input.data");
    }
    // If not found, try direct bytes object
    if (!filterClause) {
      filterClause = bytesToString(input);
      if (filterClause) {
        sdk.console.log("[delete_by_httpql] Extracted query from direct byte array");
      }
    }
    // Try trim property
    if (!filterClause && input.trim) {
      filterClause = bytesToString(input.trim);
      sdk.console.log("[delete_by_httpql] Extracted query from input.trim");
    }
    // Try value property
    if (!filterClause && input.value) {
      filterClause = bytesToString(input.value);
      sdk.console.log("[delete_by_httpql] Extracted query from input.value");
    }
  }
  
  if (!filterClause) {
    sdk.console.error("[delete_by_httpql] No HTTPQL query provided.");
    sdk.console.error(`[delete_by_httpql] Input type: ${typeof input}`);
    return "❌ No HTTPQL query provided. Please enter a valid HTTPQL query in the Trim node.";
  }

  sdk.console.log(`[delete_by_httpql] Using HTTPQL query: ${filterClause}`);

  async function sqliteRun(dbFile, sql, description = "") {
    return new Promise((resolve, reject) => {
      const proc = spawn("sqlite3", [dbFile]);
      let stdout = "";
      let stderr = "";
      proc.stdout.on("data", (chunk) => {
        stdout += chunk.toString();
      });
      proc.stderr.on("data", (chunk) => {
        stderr += chunk.toString();
      });
      proc.on("close", (code) => {
        if (code !== 0) {
          reject(
            new Error(
              `${description || sql.substring(0, 50)}: ${stderr.trim() || stdout.trim()}`,
            ),
          );
        } else {
          const trimmed = stdout.trim();
          if (description && trimmed.includes("rows_modified=")) {
            const changesMatch = trimmed.match(/rows_modified=(\d+)/);
            if (changesMatch) {
              sdk.console.log(
                `[${description}] rows modified: ${changesMatch[1]}`,
              );
            }
          }
          resolve(trimmed);
        }
      });
      proc.on("error", (err) => reject(err));
      proc.stdin.write(sql);
      proc.stdin.end();
    });
  }

  async function executeSingle(dbFile, sql, description) {
    const wrappedSql = `${sql.trim()}\nSELECT 'rows_modified=' || changes();`;
    return await sqliteRun(dbFile, wrappedSql, description);
  }

  async function executeWithTransaction(dbFile, operations, dbName) {
    try {
      await executeSingle(dbFile, "BEGIN IMMEDIATE;", `${dbName} - BEGIN`);
    } catch (error) {
      sdk.console.error(
        `[${dbName}] Failed to start transaction: ${error.message}`,
      );
      throw error;
    }

    for (let i = 0; i < operations.length; i++) {
      const op = operations[i];
      try {
        await executeSingle(dbFile, op.sql, `${dbName} - ${op.description}`);
      } catch (error) {
        sdk.console.error(`[${dbName}] Operation failed: ${op.description}`);
        sdk.console.error(`[${dbName}] Error: ${error.message}`);

        try {
          await executeSingle(dbFile, "ROLLBACK;", `${dbName} - ROLLBACK`);
          sdk.console.log(`[${dbName}] Transaction rolled back successfully`);
        } catch (rollbackError) {
          sdk.console.error(
            `[${dbName}] Rollback failed: ${rollbackError.message}`,
          );
        }
        throw error;
      }
    }

    try {
      await executeSingle(dbFile, "COMMIT;", `${dbName} - COMMIT`);
    } catch (error) {
      if (error.message.includes("no transaction is active")) {
        sdk.console.log(
          `[${dbName}] Transaction already auto-committed (success)`,
        );
      } else {
        throw error;
      }
    }
  }

  async function checkpointWAL(dbPath, dbRawPath) {
    sdk.console.log("[delete_by_httpql] Forcing WAL checkpoint...");

    const checkpointSql = "PRAGMA wal_checkpoint(TRUNCATE);";

    try {
      const mainResult = await sqliteRun(dbPath, checkpointSql);
      sdk.console.log(`[delete_by_httpql] Main DB checkpoint: ${mainResult}`);
    } catch (error) {
      sdk.console.warn(
        `[delete_by_httpql] Main DB checkpoint failed: ${error.message}`,
      );
    }

    try {
      const rawResult = await sqliteRun(dbRawPath, checkpointSql);
      sdk.console.log(`[delete_by_httpql] Raw DB checkpoint: ${rawResult}`);
    } catch (error) {
      sdk.console.warn(
        `[delete_by_httpql] Raw DB checkpoint failed: ${error.message}`,
      );
    }
  }

  async function vacuumDatabases(dbPath, dbRawPath) {
    sdk.console.log(
      "[delete_by_httpql] Vacuuming databases to reclaim disk space...",
    );

    try {
      await executeSingle(dbPath, "VACUUM;", "Vacuum main database");
      sdk.console.log("[delete_by_httpql] Main database vacuum complete");
    } catch (error) {
      sdk.console.warn(
        `[delete_by_httpql] Main database vacuum failed: ${error.message}`,
      );
    }

    try {
      await executeSingle(dbRawPath, "VACUUM;", "Vacuum raw database");
      sdk.console.log("[delete_by_httpql] Raw database vacuum complete");
    } catch (error) {
      sdk.console.warn(
        `[delete_by_httpql] Raw database vacuum failed: ${error.message}`,
      );
    }
  }

  async function getDbSize(path) {
    try {
      const result = await sqliteRun(
        path,
        "SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size();",
      );
      return parseInt(result) || 0;
    } catch {
      return 0;
    }
  }

  // ── 2. Get project paths ─────────────────────────────────────────────────
  const project = await sdk.projects.getCurrent();
  if (!project) {
    sdk.console.error("[delete_by_httpql] No active project. Aborting.");
    return;
  }

  const projectPath = project.getPath();
  const dbPath = `${projectPath}/database.caido`;
  const dbRawPath = `${projectPath}/database_raw.caido`;
  const projectId = project.getId();

  sdk.console.log(`[delete_by_httpql] Project: ${projectId}`);
  sdk.console.log(`[delete_by_httpql] DB: ${dbPath}`);
  sdk.console.log(`[delete_by_httpql] Raw DB: ${dbRawPath}`);

  // ── 3. Verify schema ─────────────────────────────────────────────────────
  const tables = await sqliteRun(dbPath, ".tables\n");
  if (!tables.includes("requests")) {
    sdk.console.error("[delete_by_httpql] FATAL: 'requests' table not found.");
    return;
  }

  // ── 4. Get database sizes before deletion ────────────────────────────────
  const beforeMainSize = await getDbSize(dbPath);
  const beforeRawSize = await getDbSize(dbRawPath);
  sdk.console.log(
    `[delete_by_httpql] Main DB size before: ${(beforeMainSize / 1024 / 1024).toFixed(2)} MB`,
  );
  sdk.console.log(
    `[delete_by_httpql] Raw DB size before: ${(beforeRawSize / 1024 / 1024).toFixed(2)} MB`,
  );

  // ── 5. Fetch requests matching the HTTPQL query using GraphQL ────────────
  sdk.console.log("[delete_by_httpql] Fetching requests matching HTTPQL query...");

  let matchedRequestIds = [];
  let cursor = null;
  let hasNextPage = true;

  while (hasNextPage) {
    try {
      const query = `
        query GetRequestsByFilter($filter: HTTPQL, $first: Int, $after: String) {
          requests(first: $first, after: $after, filter: $filter) {
            edges {
              node {
                id
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
      `;
      
      const variables = {
        filter: filterClause,
        first: 100,
        after: cursor
      };
      
      const result = await sdk.graphql.execute(query, variables);
      
      const edges = result?.data?.requests?.edges || result?.requests?.edges || [];
      const pageInfo = result?.data?.requests?.pageInfo || result?.requests?.pageInfo || {};
      
      const ids = edges.map(edge => parseInt(edge.node.id));
      matchedRequestIds = matchedRequestIds.concat(ids);
      
      hasNextPage = pageInfo.hasNextPage || false;
      cursor = pageInfo.endCursor || null;
      
      sdk.console.log(`[delete_by_httpql] Fetched ${ids.length} requests (total: ${matchedRequestIds.length})`);
    } catch (error) {
      sdk.console.error(`[delete_by_httpql] GraphQL query failed: ${error.message}`);
      sdk.console.error(`[delete_by_httpql] HTTPQL query may be invalid: ${filterClause}`);
      return;
    }
  }

  if (matchedRequestIds.length === 0) {
    sdk.console.log("[delete_by_httpql] No requests match the HTTPQL query. Nothing to delete.");
    return;
  }

  sdk.console.log(`[delete_by_httpql] Requests matching query: ${matchedRequestIds.length}`);

  // ── 6. Get additional data for matched requests using SQLite ─────────────
  const idsPlaceholder = matchedRequestIds.join(",");
  
  const requestsData = await sqliteRun(
    dbPath,
    `SELECT id, host, raw_id, IFNULL(response_id, '') FROM requests WHERE id IN (${idsPlaceholder});\n`,
  );

  const allRequests = requestsData
    .split("\n")
    .map((line) => {
      const parts = line.split("|");
      return {
        id: parseInt(parts[0]),
        host: parts[1] || "",
        raw_id: parts[2] ? parseInt(parts[2]) : null,
        response_id: parts[3] ? parseInt(parts[3]) : null,
      };
    })
    .filter((r) => !isNaN(r.id));

  const requestIds = allRequests.map((r) => r.id);
  const rawIds = allRequests.map((r) => r.raw_id).filter((id) => id != null);
  const responseIds = allRequests
    .map((r) => r.response_id)
    .filter((id) => id != null);

  // ── 7. Collect response raw_ids BEFORE deleting ──────────────────────────
  let responseRawIds = [];
  if (responseIds.length > 0) {
    try {
      const respRawOut = await sqliteRun(
        dbPath,
        `SELECT raw_id FROM responses WHERE id IN (${responseIds.join(",")});\n`,
      );
      if (respRawOut) {
        responseRawIds = respRawOut
          .split("\n")
          .map((l) => parseInt(l.trim()))
          .filter((n) => !isNaN(n));
      }
    } catch (error) {
      sdk.console.warn(
        `[delete_by_httpql] Could not fetch response raw_ids: ${error.message}`,
      );
    }
  }

  const allRequestRawIds = [...new Set(rawIds.map(Number))];
  const allResponseRawIds = [...new Set(responseRawIds.map(Number))];

  sdk.console.log(
    `[delete_by_httpql] Request IDs to delete: ${requestIds.length}`,
  );
  sdk.console.log(
    `[delete_by_httpql] Response IDs to delete: ${responseIds.length}`,
  );
  sdk.console.log(
    `[delete_by_httpql] requests_raw IDs to delete: ${allRequestRawIds.length} entries`,
  );
  sdk.console.log(
    `[delete_by_httpql] responses_raw IDs to delete: ${allResponseRawIds.length} entries`,
  );

  // ── 8. Delete from database.caido ────────────────────────────────────────
  const ids = requestIds.join(",");
  const respIds = responseIds.length > 0 ? responseIds.join(",") : null;

  sdk.console.log("[delete_by_httpql] === Starting database.caido deletions ===");

  const mainOperations = [];

  if (requestIds.length > 0) {
    mainOperations.push(
      { sql: `UPDATE sitemap_entries SET request_id = NULL WHERE request_id IN (${ids});`, description: "NULL sitemap_entries" },
      { sql: `UPDATE requests SET response_id = NULL, metadata_id = NULL WHERE id IN (${ids});`, description: "NULL requests FK columns" },
      { sql: `DELETE FROM scoped_requests WHERE request_id IN (${ids});`, description: "Delete scoped_requests" },
      { sql: `DELETE FROM scoped_intercept_entries WHERE intercept_entry_id IN (SELECT id FROM intercept_entries WHERE request_id IN (${ids}));`, description: "Delete scoped_intercept_entries" },
      { sql: `DELETE FROM intercept_entries WHERE request_id IN (${ids});`, description: "Delete intercept_entries" },
      { sql: `DELETE FROM automate_entry_requests WHERE request_id IN (${ids});`, description: "Delete automate_entry_requests" },
      { sql: `DELETE FROM findings WHERE request_id IN (${ids});`, description: "Delete findings" },
      { sql: `UPDATE replay_entries SET request_id = NULL WHERE request_id IN (${ids});`, description: "NULL replay_entries" },
      { sql: `DELETE FROM requests WHERE id IN (${ids});`, description: "Delete requests" },
      { sql: `DELETE FROM requests_metadata WHERE id NOT IN (SELECT metadata_id FROM requests WHERE metadata_id IS NOT NULL);`, description: "Delete orphaned requests_metadata" }
    );
  }

  if (respIds) {
    mainOperations.push({
      sql: `DELETE FROM responses WHERE id IN (${respIds});`,
      description: "Delete responses",
    });
  }

  try {
    await executeWithTransaction(dbPath, mainOperations, "database.caido");
    sdk.console.log(
      `[delete_by_httpql] database.caido: deleted ${requestIds.length} request(s), ${responseIds.length} response(s).`,
    );
  } catch (error) {
    sdk.console.error(`[delete_by_httpql] database.caido deletion failed: ${error.message}`);
  }

  // ── 9. Delete from database_raw.caido ────────────────────────────────────
  sdk.console.log("[delete_by_httpql] === Starting database_raw.caido deletions ===");

  const rawOperations = [];

  if (allRequestRawIds.length > 0) {
    rawOperations.push({
      sql: `DELETE FROM requests_raw WHERE id IN (${allRequestRawIds.join(",")});`,
      description: "Delete requests_raw",
    });
  }

  if (allResponseRawIds.length > 0) {
    rawOperations.push({
      sql: `DELETE FROM responses_raw WHERE id IN (${allResponseRawIds.join(",")});`,
      description: "Delete responses_raw",
    });
  }

  if (rawOperations.length > 0) {
    try {
      await executeWithTransaction(dbRawPath, rawOperations, "database_raw.caido");
      sdk.console.log(
        `[delete_by_httpql] database_raw.caido: deleted ${allRequestRawIds.length} requests_raw blob(s), ` +
          `${allResponseRawIds.length} responses_raw blob(s).`,
      );
    } catch (error) {
      sdk.console.error(`[delete_by_httpql] database_raw.caido deletion failed: ${error.message}`);
    }
  } else {
    sdk.console.log("[delete_by_httpql] No raw entries to delete.");
  }

  // ── 10. Force WAL checkpoint and vacuum ───────────────────────────────────
  await checkpointWAL(dbPath, dbRawPath);
  await vacuumDatabases(dbPath, dbRawPath);

  // ── 11. Show final sizes ─────────────────────────────────────────────────
  const afterMainSize = await getDbSize(dbPath);
  const afterRawSize = await getDbSize(dbRawPath);

  sdk.console.log(
    `[delete_by_httpql] Main DB size after: ${(afterMainSize / 1024 / 1024).toFixed(2)} MB (freed: ${((beforeMainSize - afterMainSize) / 1024 / 1024).toFixed(2)} MB)`,
  );
  sdk.console.log(
    `[delete_by_httpql] Raw DB size after: ${(afterRawSize / 1024 / 1024).toFixed(2)} MB (freed: ${((beforeRawSize - afterRawSize) / 1024 / 1024).toFixed(2)} MB)`,
  );

  sdk.console.log(
    `[delete_by_httpql] ✓ Done. Deleted ${requestIds.length} requests matching the HTTPQL query.`,
  );
  
  return `✅ Deleted ${requestIds.length} requests matching HTTPQL query: "${filterClause.substring(0, 100)}${filterClause.length > 100 ? '...' : ''}". Please reselect your workspace (switch projects and switch back) to see the changes.`;
}