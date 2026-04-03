import { spawn } from "child_process";
import path from "path";

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

  // ── 1. Get currently active filter presets from config.db ─────────────────
  sdk.console.log("[delete_by_filter] Reading active filters from config.db...");

  let selectedPresets = [];
  let activeFilterGlobalIds = [];
  let combinedClause = "";

  try {
    // Get current project path
    const project = await sdk.projects.getCurrent();
    if (!project) {
      sdk.console.error("[delete_by_filter] No active project. Aborting.");
      return;
    }

    const projectPath = project.getPath();
    
    // Go up 2 levels from project path to find Caido config directory
    const configDir = path.resolve(projectPath, "../../");
    const configDbPath = `${configDir}/config.db`;
    
    sdk.console.log(`[delete_by_filter] Project path: ${projectPath}`);
    sdk.console.log(`[delete_by_filter] Config DB path: ${configDbPath}`);
    
    // Query the user_settings table
    const configResult = await sqliteRun(
      configDbPath,
      "SELECT data FROM user_settings LIMIT 1;\n"
    );
    
    if (!configResult) {
      throw new Error("Could not read user_settings from config.db");
    }
    
    // Parse the JSON data
    const userData = JSON.parse(configResult);
    const projects = userData.projects;
    
    const currentProjectId = project.getId();
    
    sdk.console.log(`[delete_by_filter] Current project ID: ${currentProjectId}`);
    
    const projectSettings = projects[currentProjectId];
    
    if (!projectSettings) {
      throw new Error(`No settings found for project ${currentProjectId}`);
    }
    
    // Check in intercept.filter.advanced (most reliable for active filters)
    if (projectSettings.intercept?.filter?.advanced) {
      const advancedFilters = projectSettings.intercept.filter.advanced;
      sdk.console.log(`[delete_by_filter] Found ${advancedFilters.length} advanced filters in intercept`);
      
      const presetFilters = advancedFilters.filter(f => typeof f === 'string' && f.startsWith("gid://FilterPreset/"));
      if (presetFilters.length > 0) {
        activeFilterGlobalIds = presetFilters;
        sdk.console.log(`[delete_by_filter] Found ${activeFilterGlobalIds.length} active filter(s) in intercept:`);
        activeFilterGlobalIds.forEach(id => sdk.console.log(`  - ${id}`));
      }
    }
    
    // If no filters found in intercept, check in history.filter.advanced
    if (activeFilterGlobalIds.length === 0 && projectSettings.history?.filter?.advanced) {
      const advancedFilters = projectSettings.history.filter.advanced;
      sdk.console.log(`[delete_by_filter] Found ${advancedFilters.length} advanced filters in history`);
      
      const presetFilters = advancedFilters.filter(f => typeof f === 'string' && f.startsWith("gid://FilterPreset/"));
      if (presetFilters.length > 0) {
        activeFilterGlobalIds = presetFilters;
        sdk.console.log(`[delete_by_filter] Found ${activeFilterGlobalIds.length} active filter(s) in history:`);
        activeFilterGlobalIds.forEach(id => sdk.console.log(`  - ${id}`));
      }
    }
    
    // If no filters found, check in sitemap.filter.advanced
    if (activeFilterGlobalIds.length === 0 && projectSettings.sitemap?.filter?.advanced) {
      const advancedFilters = projectSettings.sitemap.filter.advanced;
      sdk.console.log(`[delete_by_filter] Found ${advancedFilters.length} advanced filters in sitemap`);
      
      const presetFilters = advancedFilters.filter(f => typeof f === 'string' && f.startsWith("gid://FilterPreset/"));
      if (presetFilters.length > 0) {
        activeFilterGlobalIds = presetFilters;
        sdk.console.log(`[delete_by_filter] Found ${activeFilterGlobalIds.length} active filter(s) in sitemap:`);
        activeFilterGlobalIds.forEach(id => sdk.console.log(`  - ${id}`));
      }
    }
    
    if (activeFilterGlobalIds.length === 0) {
      sdk.console.warn("[delete_by_filter] No active filter presets found in config.db.");
      sdk.console.warn("[delete_by_filter] Please enable at least one filter preset in the UI first.");
      return "⚠️ No active filter presets found. Please enable filter presets in the Caido UI first.";
    }
    
    // Extract numeric IDs from global IDs
    const filterIds = [];
    for (const globalId of activeFilterGlobalIds) {
      const filterIdMatch = globalId.match(/gid:\/\/FilterPreset\/(\d+)/);
      if (filterIdMatch) {
        filterIds.push(filterIdMatch[1]);
      } else {
        sdk.console.warn(`[delete_by_filter] Could not extract ID from: ${globalId}`);
      }
    }
    
    sdk.console.log(`[delete_by_filter] Extracted filter IDs: ${filterIds.join(", ")}`);
    
    // Fetch all filter presets to get the full details
    const presetsResult = await sdk.graphql.execute(`
      query filterPresets {
        filterPresets {
          id
          alias
          name
          clause
        }
      }
    `);
    
    const allFilterPresets = presetsResult?.data?.filterPresets || [];
    sdk.console.log(`[delete_by_filter] Found ${allFilterPresets.length} total filter presets`);
    
    // Find all matching presets
    for (const filterId of filterIds) {
      const preset = allFilterPresets.find(p => String(p.id) === String(filterId));
      if (preset) {
        selectedPresets.push(preset);
        sdk.console.log(`[delete_by_filter] ✓ Active filter: "${preset.name}" (alias: ${preset.alias}, id: ${preset.id})`);
        sdk.console.log(`[delete_by_filter]   Clause: ${preset.clause.substring(0, 100)}...`);
      } else {
        sdk.console.warn(`[delete_by_filter] Filter preset with ID ${filterId} not found`);
      }
    }
    
    if (selectedPresets.length === 0) {
      throw new Error("No matching filter presets found");
    }
    
    // Combine all filter clauses with AND
    if (selectedPresets.length === 1) {
      combinedClause = selectedPresets[0].clause;
    } else {
      const clauses = selectedPresets.map(p => `(${p.clause})`);
      combinedClause = clauses.join(" AND ");
    }
    
    sdk.console.log(`[delete_by_filter] Combined filter clause (${selectedPresets.length} filters): ${combinedClause.substring(0, 200)}...`);
    
  } catch (error) {
    sdk.console.error(`[delete_by_filter] Failed to get active filters: ${error.message}`);
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

  // ── 5. Fetch ALL requests from SQLite ────────────────────────────────────
  sdk.console.log("[delete_by_filter] Fetching all requests from database...");

  const allRequestsRaw = await sqliteRun(
    dbPath,
    "SELECT id, host, raw_id, IFNULL(response_id, '') FROM requests;\n",
  );

  if (!allRequestsRaw) {
    sdk.console.log("[delete_by_filter] No requests in DB. Nothing to do.");
    return;
  }

  const allRequests = allRequestsRaw
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

  const allIds = new Set(allRequests.map(r => r.id));
  sdk.console.log(`[delete_by_filter] Total requests in database: ${allIds.size}`);

  // ── 6. Fetch requests that MATCH the combined filter (what filters SHOW) ──
  sdk.console.log("[delete_by_filter] Fetching requests that match the active filters...");

  let shownIds = new Set();
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
        filter: combinedClause,
        first: 100,
        after: cursor
      };
      
      const result = await sdk.graphql.execute(query, variables);
      
      const edges = result?.data?.requests?.edges || result?.requests?.edges || [];
      const pageInfo = result?.data?.requests?.pageInfo || result?.requests?.pageInfo || {};
      
      edges.forEach(edge => shownIds.add(parseInt(edge.node.id)));
      
      hasNextPage = pageInfo.hasNextPage || false;
      cursor = pageInfo.endCursor || null;
      
      sdk.console.log(`[delete_by_filter] Fetched ${edges.length} matching requests (total: ${shownIds.size})`);
    } catch (error) {
      sdk.console.error(`[delete_by_filter] GraphQL query failed: ${error.message}`);
      return;
    }
  }

  // ── 7. Calculate IDs to delete (requests that do NOT match the filter) ────
  const idsToDelete = [...allIds].filter(id => !shownIds.has(id));
  
  sdk.console.log(`[delete_by_filter] Filter shows: ${shownIds.size} requests`);
  sdk.console.log(`[delete_by_filter] Filter hides: ${idsToDelete.length} requests (these will be deleted)`);

  if (idsToDelete.length === 0) {
    sdk.console.log("[delete_by_filter] No requests to delete. Nothing to do.");
    return;
  }

  // ── 8. Get full data for requests to delete ──────────────────────────────
  const requestsToDelete = allRequests.filter(r => idsToDelete.includes(r.id));

  const requestIds = requestsToDelete.map(r => r.id);
  const rawIds = requestsToDelete.map(r => r.raw_id).filter(id => id != null);
  const responseIds = requestsToDelete.map(r => r.response_id).filter(id => id != null);

  // ── 9. Collect response raw_ids BEFORE deleting ──────────────────────────
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
    `[delete_by_filter] Request IDs to delete: ${requestIds.length}`,
  );
  sdk.console.log(
    `[delete_by_filter] Response IDs to delete: ${responseIds.length}`,
  );
  sdk.console.log(
    `[delete_by_filter] requests_raw IDs to delete: ${allRequestRawIds.length} entries`,
  );
  sdk.console.log(
    `[delete_by_filter] responses_raw IDs to delete: ${allResponseRawIds.length} entries`,
  );

  // ── 10. Delete from database.caido ───────────────────────────────────────
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

  // ── 11. Delete from database_raw.caido ───────────────────────────────────
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

  // ── 12. Force WAL checkpoint and vacuum ───────────────────────────────────
  await checkpointWAL(dbPath, dbRawPath);
  await vacuumDatabases(dbPath, dbRawPath);

  // ── 13. Show final sizes ─────────────────────────────────────────────────
  const afterMainSize = await getDbSize(dbPath);
  const afterRawSize = await getDbSize(dbRawPath);

  sdk.console.log(
    `[delete_by_filter] Main DB size after: ${(afterMainSize / 1024 / 1024).toFixed(2)} MB (freed: ${((beforeMainSize - afterMainSize) / 1024 / 1024).toFixed(2)} MB)`,
  );
  sdk.console.log(
    `[delete_by_filter] Raw DB size after: ${(afterRawSize / 1024 / 1024).toFixed(2)} MB (freed: ${((beforeRawSize - afterRawSize) / 1024 / 1024).toFixed(2)} MB)`,
  );

  // ── 14. Return summary ────────────────────────────────────────────────────
  const filterNames = selectedPresets.map(p => p.name).join(", ");
  sdk.console.log(
    `[delete_by_filter] ✓ Done. Deleted ${requestIds.length} requests that were HIDDEN by active filter(s): ${filterNames}.`,
  );
  
  return `✅ Deleted ${requestIds.length} requests that were hidden by active filter(s): ${filterNames}. Please reselect your workspace (switch projects and switch back) to see the changes.`;
}