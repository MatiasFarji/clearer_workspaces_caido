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

    // Try to commit, but don't error if already auto-committed
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
    sdk.console.log("[workspace_clearer] Forcing WAL checkpoint...");

    const checkpointSql = "PRAGMA wal_checkpoint(TRUNCATE);";

    try {
      const mainResult = await sqliteRun(dbPath, checkpointSql);
      sdk.console.log(`[workspace_clearer] Main DB checkpoint: ${mainResult}`);
    } catch (error) {
      sdk.console.warn(
        `[workspace_clearer] Main DB checkpoint failed: ${error.message}`,
      );
    }

    try {
      const rawResult = await sqliteRun(dbRawPath, checkpointSql);
      sdk.console.log(`[workspace_clearer] Raw DB checkpoint: ${rawResult}`);
    } catch (error) {
      sdk.console.warn(
        `[workspace_clearer] Raw DB checkpoint failed: ${error.message}`,
      );
    }
  }

  async function vacuumDatabases(dbPath, dbRawPath) {
    sdk.console.log(
      "[workspace_clearer] Vacuuming databases to reclaim disk space...",
    );

    try {
      await executeSingle(dbPath, "VACUUM;", "Vacuum main database");
      sdk.console.log("[workspace_clearer] Main database vacuum complete");
    } catch (error) {
      sdk.console.warn(
        `[workspace_clearer] Main database vacuum failed: ${error.message}`,
      );
    }

    try {
      await executeSingle(dbRawPath, "VACUUM;", "Vacuum raw database");
      sdk.console.log("[workspace_clearer] Raw database vacuum complete");
    } catch (error) {
      sdk.console.warn(
        `[workspace_clearer] Raw database vacuum failed: ${error.message}`,
      );
    }
  }

  // ── 1. Load scopes ───────────────────────────────────────────────────────
  const scopes = await sdk.scope.getAll();
  if (!scopes || scopes.length === 0) {
    sdk.console.warn("[workspace_clearer] No scopes configured. Aborting.");
    return;
  }

  const allowPatterns = scopes.flatMap((s) => s.allowlist);
  const denyPatterns = scopes.flatMap((s) => s.denylist);

  if (allowPatterns.length === 0 && denyPatterns.length === 0) {
    sdk.console.warn("[workspace_clearer] Scopes have no rules. Aborting.");
    return;
  }

  sdk.console.log(
    `[workspace_clearer] Allowlist (${allowPatterns.length}): ${allowPatterns.join(", ")}`,
  );
  sdk.console.log(
    `[workspace_clearer] Denylist  (${denyPatterns.length}): ${denyPatterns.join(", ")}`,
  );

  // ── 2. Get project paths ─────────────────────────────────────────────────
  const project = await sdk.projects.getCurrent();
  if (!project) {
    sdk.console.error("[workspace_clearer] No active project. Aborting.");
    return;
  }

  const projectPath = project.getPath();
  const dbPath = `${projectPath}/database.caido`;
  const dbRawPath = `${projectPath}/database_raw.caido`;
  const projectId = project.getId();

  sdk.console.log(`[workspace_clearer] Project: ${projectId}`);
  sdk.console.log(`[workspace_clearer] DB:      ${dbPath}`);
  sdk.console.log(`[workspace_clearer] Raw DB:  ${dbRawPath}`);

  // ── 3. Verify schema ─────────────────────────────────────────────────────
  const tables = await sqliteRun(dbPath, ".tables\n");
  sdk.console.log(
    `[workspace_clearer] Tables found: ${tables.split(/\s+/).filter(Boolean).join(", ")}`,
  );
  if (!tables.includes("requests")) {
    sdk.console.error("[workspace_clearer] FATAL: 'requests' table not found.");
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
    `[workspace_clearer] Main DB size before: ${(beforeMainSize / 1024 / 1024).toFixed(2)} MB`,
  );
  sdk.console.log(
    `[workspace_clearer] Raw DB size before: ${(beforeRawSize / 1024 / 1024).toFixed(2)} MB`,
  );

  // ── 5. Fetch all requests ────────────────────────────────────────────────
  const rawOutput = await sqliteRun(
    dbPath,
    "SELECT id, host, raw_id, IFNULL(response_id, '') FROM requests;\n",
  );

  if (!rawOutput) {
    sdk.console.log("[workspace_clearer] No requests in DB. Nothing to do.");
    return;
  }

  const allRequests = rawOutput
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

  sdk.console.log(`[workspace_clearer] Total requests: ${allRequests.length}`);

  // ── 6. Scope matching ────────────────────────────────────────────────────
  function patternToRegex(p) {
    const escaped = p
      .replace(/[.+?^${}()|[\]\\]/g, "\\$&")
      .replace(/\*/g, ".*");
    return new RegExp(`^${escaped}$`, "i");
  }

  const allowRx = allowPatterns.map(patternToRegex);
  const denyRx = denyPatterns.map(patternToRegex);

  function isInScope(host) {
    const passAllow =
      allowRx.length === 0 || allowRx.some((re) => re.test(host));
    const passDeny = denyRx.length === 0 || !denyRx.some((re) => re.test(host));
    return passAllow && passDeny;
  }

  // ── 7. Filter out-of-scope ───────────────────────────────────────────────
  const outOfScope = allRequests.filter((r) => !isInScope(r.host));

  if (outOfScope.length === 0) {
    sdk.console.log(
      "[workspace_clearer] All requests are in scope. Nothing to delete.",
    );
    return;
  }

  sdk.console.log(
    `[workspace_clearer] Out-of-scope: ${outOfScope.length} request(s)`,
  );

  const requestIds = outOfScope.map((r) => r.id);
  const rawIds = outOfScope.map((r) => r.raw_id).filter((id) => id != null);
  const responseIds = outOfScope
    .map((r) => r.response_id)
    .filter((id) => id != null);

  // ── 8. Collect response raw_ids BEFORE deleting ──────────────────────────
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
        `[workspace_clearer] Could not fetch response raw_ids: ${error.message}`,
      );
    }
  }

  const allRequestRawIds = [...new Set(rawIds.map(Number))];
  const allResponseRawIds = [...new Set(responseRawIds.map(Number))];

  sdk.console.log(
    `[workspace_clearer] Request IDs:       ${requestIds.length}`,
  );
  sdk.console.log(
    `[workspace_clearer] Response IDs:      ${responseIds.length}`,
  );
  sdk.console.log(
    `[workspace_clearer] requests_raw IDs:  ${allRequestRawIds.length} entries`,
  );
  sdk.console.log(
    `[workspace_clearer] responses_raw IDs: ${allResponseRawIds.length} entries`,
  );

  // ── 9. Delete from database.caido ────────────────────────────────────────
  const ids = requestIds.join(",");
  const respIds = responseIds.length > 0 ? responseIds.join(",") : null;

  sdk.console.log(
    "[workspace_clearer] === Starting database.caido deletions ===",
  );

  const mainOperations = [];

  if (requestIds.length > 0) {
    mainOperations.push(
      {
        sql: `UPDATE sitemap_entries SET request_id = NULL WHERE request_id IN (${ids});`,
        description: "NULL sitemap_entries",
      },
      {
        sql: `UPDATE requests SET response_id = NULL, metadata_id = NULL WHERE id IN (${ids});`,
        description: "NULL requests FK columns",
      },
      {
        sql: `DELETE FROM scoped_requests WHERE request_id IN (${ids});`,
        description: "Delete scoped_requests",
      },
      {
        sql: `DELETE FROM scoped_intercept_entries WHERE intercept_entry_id IN (SELECT id FROM intercept_entries WHERE request_id IN (${ids}));`,
        description: "Delete scoped_intercept_entries",
      },
      {
        sql: `DELETE FROM intercept_entries WHERE request_id IN (${ids});`,
        description: "Delete intercept_entries",
      },
      {
        sql: `DELETE FROM automate_entry_requests WHERE request_id IN (${ids});`,
        description: "Delete automate_entry_requests",
      },
      {
        sql: `DELETE FROM findings WHERE request_id IN (${ids});`,
        description: "Delete findings",
      },
      {
        sql: `UPDATE replay_entries SET request_id = NULL WHERE request_id IN (${ids});`,
        description: "NULL replay_entries",
      },
      {
        sql: `DELETE FROM requests WHERE id IN (${ids});`,
        description: "Delete requests",
      },
      {
        sql: `DELETE FROM requests_metadata WHERE id NOT IN (SELECT metadata_id FROM requests WHERE metadata_id IS NOT NULL);`,
        description: "Delete orphaned requests_metadata",
      },
    );
  }

  if (respIds) {
    mainOperations.push({
      sql: `DELETE FROM responses WHERE id IN (${respIds});`,
      description: "Delete responses",
    });
  }

  // Run with transaction
  try {
    await executeWithTransaction(dbPath, mainOperations, "database.caido");
    sdk.console.log(
      `[workspace_clearer] database.caido: deleted ${requestIds.length} request(s), ${responseIds.length} response(s).`,
    );
  } catch (error) {
    sdk.console.error(
      `[workspace_clearer] database.caido deletion failed: ${error.message}`,
    );
  }

  // ── 10. Delete from database_raw.caido ───────────────────────────────────
  sdk.console.log(
    "[workspace_clearer] === Starting database_raw.caido deletions ===",
  );

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
      // Use transaction for raw deletions too
      await executeWithTransaction(
        dbRawPath,
        rawOperations,
        "database_raw.caido",
      );
      sdk.console.log(
        `[workspace_clearer] database_raw.caido: deleted ${allRequestRawIds.length} requests_raw blob(s), ` +
          `${allResponseRawIds.length} responses_raw blob(s).`,
      );
    } catch (error) {
      sdk.console.error(
        `[workspace_clearer] database_raw.caido deletion failed: ${error.message}`,
      );
    }
  } else {
    sdk.console.log("[workspace_clearer] No raw entries to delete.");
  }

  // ── 11. Force WAL checkpoint to persist changes ──────────────────────────
  await checkpointWAL(dbPath, dbRawPath);

  // ── 12. Vacuum databases to reclaim disk space ───────────────────────────
  await vacuumDatabases(dbPath, dbRawPath);

  // ── 13. Show final sizes ─────────────────────────────────────────────────
  const afterMainSize = await getDbSize(dbPath);
  const afterRawSize = await getDbSize(dbRawPath);

  sdk.console.log(
    `[workspace_clearer] Main DB size after: ${(afterMainSize / 1024 / 1024).toFixed(2)} MB (freed: ${((beforeMainSize - afterMainSize) / 1024 / 1024).toFixed(2)} MB)`,
  );
  sdk.console.log(
    `[workspace_clearer] Raw DB size after: ${(afterRawSize / 1024 / 1024).toFixed(2)} MB (freed: ${((beforeRawSize - afterRawSize) / 1024 / 1024).toFixed(2)} MB)`,
  );

  sdk.console.log(
    `[workspace_clearer] ✓ Done. ${requestIds.length} request(s) removed. `
  );

  
}
