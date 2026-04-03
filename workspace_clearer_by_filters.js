import { spawn } from "child_process";

export async function run({ request }, sdk) {
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
    sdk.console.log("[delete_by_filter] Forcing WAL checkpoint...");

    const checkpointSql = "PRAGMA wal_checkpoint(TRUNCATE);";

    try {
      const mainResult = await sqliteRun(dbPath, checkpointSql);
      sdk.console.log(`[delete_by_filter] Main DB checkpoint: ${mainResult}`);
    } catch (error) {
      sdk.console.warn(
        `[delete_by_filter] Main DB checkpoint failed: ${error.message}`,
      );
    }

    try {
      const rawResult = await sqliteRun(dbRawPath, checkpointSql);
      sdk.console.log(`[delete_by_filter] Raw DB checkpoint: ${rawResult}`);
    } catch (error) {
      sdk.console.warn(
        `[delete_by_filter] Raw DB checkpoint failed: ${error.message}`,
      );
    }
  }

  async function vacuumDatabases(dbPath, dbRawPath) {
    sdk.console.log(
      "[delete_by_filter] Vacuuming databases to reclaim disk space...",
    );

    try {
      await executeSingle(dbPath, "VACUUM;", "Vacuum main database");
      sdk.console.log("[delete_by_filter] Main database vacuum complete");
    } catch (error) {
      sdk.console.warn(
        `[delete_by_filter] Main database vacuum failed: ${error.message}`,
      );
    }

    try {
      await executeSingle(dbRawPath, "VACUUM;", "Vacuum raw database");
      sdk.console.log("[delete_by_filter] Raw database vacuum complete");
    } catch (error) {
      sdk.console.warn(
        `[delete_by_filter] Raw database vacuum failed: ${error.message}`,
      );
    }
  }

  // ── 1. Get all filter presets using GraphQL ───────────────────────────────
  sdk.console.log("[delete_by_filter] Fetching filter presets...");

  let selectedPreset = null;
  let filterPresets = [];

  try {
    const result = await sdk.graphql.execute(`
      query filterPresets {
        filterPresets {
          id
          alias
          name
          clause
        }
      }
    `);
    
    // Extract presets from the response
    filterPresets = result?.data?.filterPresets || [];
    
    if (filterPresets.length === 0) {
      sdk.console.warn("[delete_by_filter] No filter presets found.");
      sdk.console.warn("[delete_by_filter] Please create a filter preset first (Intercept tab → Filter icon → Save as preset)");
      return "⚠️ No filter presets found. Please create a filter preset first.";
    }
    
    sdk.console.log(`[delete_by_filter] Found ${filterPresets.length} filter preset(s):`);
    filterPresets.forEach((preset) => {
      sdk.console.log(`  - ${preset.name} (alias: ${preset.alias || 'none'})`);
      sdk.console.log(`    Clause: ${preset.clause.substring(0, 100)}...`);
    });
    
    // Use the first preset (you can modify to select by name/alias)
    selectedPreset = filterPresets[0];
    sdk.console.log(`[delete_by_filter] Using filter preset: "${selectedPreset.name}"`);
    sdk.console.log(`[delete_by_filter] Filter clause: ${selectedPreset.clause}`);
    
  } catch (error) {
    sdk.console.error(`[delete_by_filter] Failed to fetch filter presets: ${error.message}`);
    return;
  }

  // ── 2. Get project paths ─────────────────────────────────────────────────
  const project = await sdk.projects.getCurrent();
  if (!project) {
    sdk.console.error("[delete_by_filter] No active project. Aborting.");
    return;
  }

  const projectPath = project.getPath();
  const dbPath = `${projectPath}/database.caido`;
  const dbRawPath = `${projectPath}/database_raw.caido`;
  const projectId = project.getId();

  sdk.console.log(`[delete_by_filter] Project: ${projectId}`);
  sdk.console.log(`[delete_by_filter] DB: ${dbPath}`);
  sdk.console.log(`[delete_by_filter] Raw DB: ${dbRawPath}`);

  // ── 3. Verify schema ─────────────────────────────────────────────────────
  const tables = await sqliteRun(dbPath, ".tables\n");
  if (!tables.includes("requests")) {
    sdk.console.error("[delete_by_filter] FATAL: 'requests' table not found.");
    return;
  }

  // ── 4. Get database sizes before deletion ────────────────────────────────
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

  const beforeMainSize = await getDbSize(dbPath);
  const beforeRawSize = await getDbSize(dbRawPath);
  sdk.console.log(
    `[delete_by_filter] Main DB size before: ${(beforeMainSize / 1024 / 1024).toFixed(2)} MB`,
  );
  sdk.console.log(
    `[delete_by_filter] Raw DB size before: ${(beforeRawSize / 1024 / 1024).toFixed(2)} MB`,
  );

  // ── 5. Fetch requests matching the filter using GraphQL ───────────────────
  sdk.console.log("[delete_by_filter] Fetching requests matching filter...");

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
        filter: selectedPreset.clause,
        first: 100,
        after: cursor
      };
      
      const result = await sdk.graphql.execute(query, variables);
      
      // Handle response structure
      const edges = result?.data?.requests?.edges || result?.requests?.edges || [];
      const pageInfo = result?.data?.requests?.pageInfo || result?.requests?.pageInfo || {};
      
      const ids = edges.map(edge => parseInt(edge.node.id));
      matchedRequestIds = matchedRequestIds.concat(ids);
      
      hasNextPage = pageInfo.hasNextPage || false;
      cursor = pageInfo.endCursor || null;
      
      sdk.console.log(`[delete_by_filter] Fetched ${ids.length} requests (total: ${matchedRequestIds.length})`);
    } catch (error) {
      sdk.console.error(`[delete_by_filter] GraphQL query failed: ${error.message}`);
      sdk.console.error(`[delete_by_filter] Filter clause may be invalid: ${selectedPreset.clause}`);
      return;
    }
  }

  if (matchedRequestIds.length === 0) {
    sdk.console.log("[delete_by_filter] No requests match the selected filter. Nothing to delete.");
    return;
  }

  sdk.console.log(`[delete_by_filter] Requests matching filter: ${matchedRequestIds.length}`);

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
        `[delete_by_filter] Could not fetch response raw_ids: ${error.message}`,
      );
    }
  }

  const allRequestRawIds = [...new Set(rawIds.map(Number))];
  const allResponseRawIds = [...new Set(responseRawIds.map(Number))];

  sdk.console.log(
    `[delete_by_filter] Request IDs: ${requestIds.length}`,
  );
  sdk.console.log(
    `[delete_by_filter] Response IDs: ${responseIds.length}`,
  );
  sdk.console.log(
    `[delete_by_filter] requests_raw IDs: ${allRequestRawIds.length} entries`,
  );
  sdk.console.log(
    `[delete_by_filter] responses_raw IDs: ${allResponseRawIds.length} entries`,
  );

  // ── 8. Delete from database.caido ────────────────────────────────────────
  const ids = requestIds.join(",");
  const respIds = responseIds.length > 0 ? responseIds.join(",") : null;

  sdk.console.log("[delete_by_filter] === Starting database.caido deletions ===");

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
      `[delete_by_filter] database.caido: deleted ${requestIds.length} request(s), ${responseIds.length} response(s).`,
    );
  } catch (error) {
    sdk.console.error(`[delete_by_filter] database.caido deletion failed: ${error.message}`);
  }

  // ── 9. Delete from database_raw.caido ────────────────────────────────────
  sdk.console.log("[delete_by_filter] === Starting database_raw.caido deletions ===");

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
        `[delete_by_filter] database_raw.caido: deleted ${allRequestRawIds.length} requests_raw blob(s), ` +
          `${allResponseRawIds.length} responses_raw blob(s).`,
      );
    } catch (error) {
      sdk.console.error(`[delete_by_filter] database_raw.caido deletion failed: ${error.message}`);
    }
  } else {
    sdk.console.log("[delete_by_filter] No raw entries to delete.");
  }

  // ── 10. Force WAL checkpoint and vacuum ───────────────────────────────────
  await checkpointWAL(dbPath, dbRawPath);
  await vacuumDatabases(dbPath, dbRawPath);

  // ── 11. Show final sizes ─────────────────────────────────────────────────
  const afterMainSize = await getDbSize(dbPath);
  const afterRawSize = await getDbSize(dbRawPath);

  sdk.console.log(
    `[delete_by_filter] Main DB size after: ${(afterMainSize / 1024 / 1024).toFixed(2)} MB (freed: ${((beforeMainSize - afterMainSize) / 1024 / 1024).toFixed(2)} MB)`,
  );
  sdk.console.log(
    `[delete_by_filter] Raw DB size after: ${(afterRawSize / 1024 / 1024).toFixed(2)} MB (freed: ${((beforeRawSize - afterRawSize) / 1024 / 1024).toFixed(2)} MB)`,
  );

  sdk.console.log(
    `[delete_by_filter] ✓ Done. ${requestIds.length} request(s) removed. `,
  );
  
  return `✅ Deleted ${requestIds.length} requests matching filter preset "${selectedPreset.name}". Please reselect your workspace (switch projects and switch back) to see the changes.`;
}