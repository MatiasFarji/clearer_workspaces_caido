# 🗑️ Caido Workspace Cleaner Workflows

A collection of powerful cleanup workflows for the [Caido](https://caido.io) security assessment platform. These workflows help you efficiently remove requests from your workspace based on various criteria.

## ⚠️ DESTRUCTIVE OPERATIONS - READ CAREFULLY ⚠️

> **🔴 CRITICAL WARNING 🔴**
> 
> These workflows **PERMANENTLY DELETE** requests and their associated responses from your Caido workspace.
> 
> **THIS ACTION IS IRREVERSIBLE!**
> 
> - ✅ **ALWAYS CREATE A BACKUP BEFORE RUNNING ANY WORKFLOW**
> - ✅ **VERIFY YOUR SCOPES/FILTERS BEFORE EXECUTION**
> - ✅ **TEST ON A SMALL DATASET FIRST**
> - ❌ **DO NOT RUN WITHOUT A BACKUP**


## 💾 How to Backup Your Caido Project

Caido has a built-in backup feature that's quick and easy:

1. **Open the Workspace tab** from the left-hand menu
2. **Find your project** in the Projects list
3. **Click the `⋮` (three dots)** next to your project name
4. **Select "Create backup"** from the dropdown menu

> **Note:** A notification will appear when the backup starts, and another when it's complete


**🔄 IMPORTANT: Reselect Workspace Required**
 
After running any workflow, you **MUST reselect your workspace** to see the changes in the Caido UI.
 
**How to reselect:**
1. Go to the **Workspace tab**
2. Double Click on the same project

*Why? Caido caches workspace data in memory. Reselecting the workspace forces a full cache refresh, displaying the updated state.*


## 📋 Available Workflows

| Workflow | Workflow Type | Description | Status | 
|----------|----------|-------------|--------|
| **Clearer Workspaces By Scope** | Convert | Deletes requests outside configured scopes | ✅ Available |
| **Delete by Filter Preset** | Convert | Deletes requests matching a saved filter preset | ✅ Available |
| **Delete by HTTPQL** | Convert | Deletes requests matching custom HTTPQL query | ✅ Available |

## 🚀 Installation

1. **Download the workflow JSON file** from this repository
   - `clearer-workspaces-by-scope.json`
   - `delete-by-filter-preset.json` (coming soon)
   - `delete-by-httpql.json` (coming soon)

2. **Open Caido** and navigate to the **Workflows tab**

3. **Click the "Import" button** in the top-left corner of the Workflows tab

4. **Select the downloaded JSON file** from your computer


## ▶️ How to Execute

These workflows are designed for **manual execution only**. They do not run automatically.

### Steps to Run a Workflow:

1. **Go to the Workflows tab** in Caido

2. **Click on the "Active" tab** to see your enabled workflows

3. **Select the workflow** you want to run (click on it)

4. **Click the "Run" button** in the right-middle margin of the workflow editor

5. **Confirm the execution** when prompted

6. **Monitor the logs** in the output panel to see deletion progress
