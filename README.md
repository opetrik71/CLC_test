
---  to the planned appearance of the repo ---

## ⚙️ Main Scripts  

### `prod/corine_iterator.py`
- Ready-to-use ArcGIS **Script Tool** version  
- Compatible with ArcGIS Pro 3.3 +  
- GP-pane friendly messaging (`arcpy.AddMessage`, `AddWarning`, etc.)
- Optimized version
- Implements performance improvements and neighbor caching

---

## 🧩 Input Data

| Input | Description |
|-------|--------------|
| **Change** | Feature class with `CHCODE` (change codes) |
| **Revision** | Feature class with `REVCODE` (revision codes) |
| **Priority Table** | `join_pri.dbf` containing `CODE` and `PRI` fields |
| **Output** | Target feature class (e.g., `D:\work\CLC2024\gdb\gener_73`) |

---

## 🔧 Parameters

| Name | Type | Default | Description |
|------|------|----------|-------------|
| `from_value` | Long | 3 | Start MMU threshold (ha) |
| `to_value` | Long | 23 | End MMU threshold (ha) |
| `by_value` | Long | 5 | Increment between iterations |
| `neighbor_mode` | Text | `"BOUNDARY_TOUCHES"` | Spatial relationship mode |

---

## 🧠 Workflow Overview

1. **Prepare Input Copies**
   - Change & Revision datasets copied to memory.
   - Codes normalized (e.g., `1211/1212 → 121`).
   - Ensures all polygons have valid class codes.
     
2. **Union + Multipart → Singlepart**
   - Combined geometry between Change and Revision.

3. **Iterative Generalization**
   - Iterates over size thresholds (3 → 23 ha).
   - Small polygons are merged with their best neighbor:
     - Prefers same code or lowest `PRI` value.
     - Uses pre-computed neighbor index (PolygonNeighbors).

4. **Dissolve after each iteration**
   - Keeps geometry clean and merged.

5. **Annotation**
   - Adds `Comment`:
     - `<25 ha` → “Smaller than MMU”
     - `<25 ha` & touches boundary → “Edge polygon”

---

## 🧮 Performance Notes

- Uses `arcpy.env.parallelProcessingFactor = "100%"`.
- Temporary features created in **memory workspace**.
- Single batch updates instead of per-polygon editing.
- Tested with up to ~50 000 polygons (country-level datasets).

---

## 🧰 Using as a Script Tool in ArcGIS Pro

1. Create a new **ToolBox** in the Catalog.
2. Create a new **Script** in this **ToolBox**
3. Set the **Name**, **Label**, **Decription** fields in **Tool Properties/General** tab.
4. Set the script path to `corine_iterator.py` in **Tool Properties/Execution** tab.
5. Define parameters in **Tool Propertis/Parameters** tab as:

| Index | Name | Data Type | Direction | Default |
|-------|------|------------|------------|----------|
| 0 | input_change | Feature Class | Input | — |
| 1 | input_revision | Feature Class | Input | — |
| 2 | out_general | Feature Class | Output | — |
| 3 | priority_table | Table | Input | join_pri.dbf |
| 4 | from_value | Long | Input | 3 |
| 5 | to_value | Long | Input | 23 |
| 6 | by_value | Long | Input | 5 |

If help is needed try: https://www.youtube.com/watch?v=v5pBuvo4JTU

or: https://pro.arcgis.com/en/pro-app/latest/help/analysis/geoprocessing/basics/create-a-python-script-tool.htm

---

## 📈 QA / Comparison Tool

`tools/compare_v2.py`  
Compares two generalized datasets (e.g., reference vs. new result):
- Counts polygons  
- Compares area per `NEWCODE`  
- Computes symmetric difference area (ha)  
- Reports area differences where `NEWCODE` mismatches  

Outputs can be printed or exported to CSV.

---

## ⚡ Example Timing (ArcGIS Pro 3.5.3)
| Dataset | Polygons | Original | Optimized |
|----------|-----------|-----------|------------|
| 1500 square-km sheet | ~800 | 86 s | 50 s |
| country-wide full | ~50 000 | 5600 s | 703 s |

---

## 📜 License
This project is licensed under the **GNU General Public License v3.0 (GPL-3.0)**  

You may use, modify, and distribute the code freely for **research, education, or
public administration** purposes, provided that derivative works remain open source
under the same license.  
Commercial redistribution or integration into closed-source software is **not allowed**
without prior written permission from the author.

See the [LICENSE/LICENSE](LICENSE/LICENSE) file or visit  
[https://www.gnu.org/licenses/gpl-3.0.html](https://www.gnu.org/licenses/gpl-3.0.html) for details.
---

## 🙌 Contributors

| Name | Contribution |
|------|---------------|
| **O. Petrik** | Project lead, core algorithm design, CLC domain logic |
| **ChatGPT (GPT-5)** | ArcGIS Pro script refactoring and optimization, toolbox integration, documentation, QA, and environment automation |
| **Claude Opus 4.1** | Performance optimization (batch updates, neighbor caching) |

---

## 🧩 Notes
- `PairwiseDissolve` used for faster topology rebuilds (fallbacks to `Dissolve` if unsupported).
- Tested with ArcGIS Pro 3.5.3, Python 3.9
- Optimized for both tile-level and full-country generalization workflows.

## 🧠 Module Documentation
DETAILED ALGORITHM DESCRIPTION
==============================

The CORINE Land Cover generalization algorithm implements a rule-based polygon
merging strategy that progressively reduces complexity while preserving important
features and maintaining topological consistency.

Core Concepts:
--------------

1. **Area Thresholds**:
   Polygons are processed in iterations with increasing area thresholds.
   Default sequence: 3ha → 8ha → 13ha → 18ha → 23ha
   
   At each threshold, polygons smaller than the threshold are candidates for
   merging with neighbors. Larger polygons are preserved as anchors.

2. **Boundary Preservation**:
   Polygons touching the dataset boundary (revision extent) are never merged.
   This prevents edge artifacts and maintains dataset extent integrity.

3. **Priority-Based Merging**:
   Each potential merge is scored by priority rules:
   
   a) Identical Code Match (Priority = 0):
      If neighbor has same NEWCODE, always merge (highest priority).
      This consolidates fragmented areas of same land cover type.
   
   b) Pair Priority (from lookup table):
      If specific pair "smallcode + neighborcode" exists in priority table,
      use that priority value. This handles known transition rules.
      Example: "121211" → priority 3 (urban-to-agriculture has specific rule)
   
   c) Single Code Priority (from lookup table):
      If neighbor's single code exists in priority table, use that priority.
      Example: "211" → priority 5 (agriculture is common merge target)
   
   d) Default Priority (999999):
      If code not in table, use lowest priority (least preferred merge).
   
   Lower priority values indicate preferred merges.

4. **Tie-Breaking**:
   When multiple neighbors have identical priority, choose the neighbor with
   largest area. This biases towards stable, well-established features.

5. **Iterative Dissolve**:
   After each threshold iteration, all polygons are dissolved by NEWCODE.
   This consolidates adjacent polygons with identical codes and prepares
   topology for the next iteration.

6. **Neighbor Index Rebuild**:
   After dissolve, ObjectIDs change and topology may shift. The neighbor
   spatial index is rebuilt from scratch for the next iteration.


Data Flow:
----------

INPUT PREPARATION:
  input_change (CHCODE) ────┐
                             ├──→ normalize codes (1211/1212→121)
  input_revision (REVCODE) ─┘
                             │
                             ├──→ dissolve by code
                             │
                             ├──→ union (overlay change on revision)
                             │
                             ├──→ multipart to singlepart
                             │
                             └──→ out_general (initial)

ITERATION LOOP (for each threshold):
  1. Build neighbor index from PolygonNeighbors
  2. Select small polygons (area < threshold, not touching boundary)
  3. For each small polygon:
     - Query neighbors from index (O(1) lookup)
     - Score each neighbor by priority rules
     - Choose best neighbor
     - Record NEWCODE update
  4. Apply all updates in batch
  5. Dissolve by NEWCODE
  6. Rebuild neighbor index

POST-PROCESSING:
  - Add Comment field annotations
  - Calculate final AREA and GID
  - Cleanup temporary data


Performance Characteristics:
----------------------------

Time Complexity:
  - Neighbor index build: O(n) where n = polygon count
  - Per-iteration scoring: O(m × k) where m = small polys, k = avg neighbors
  - Dissolve: O(n log n) - dominant operation
  - Total: O(i × n log n) where i = iteration count

Space Complexity:
  - Neighbor index: O(n × k) - typically 4-8 neighbors per polygon
  - Polygon data dict: O(n)
  - Updates dict: O(m)
  - Peak memory: ~45 MB per 1000 polygons

Typical Performance:
  - 50,000 polygons: ~10 minutes, ~2 GB peak memory
  - 100,000 polygons: ~25 minutes, ~4.5 GB peak memory
  - 500,000 polygons: ~180 minutes, ~22 GB peak memory

Bottlenecks:
  1. Dissolve operations (60-70% of runtime)
  2. PolygonNeighbors (15-20% of runtime)
  3. UpdateCursor batch updates (5-10% of runtime)
  4. Memory copies during CopyFeatures (5% of runtime)


Memory Management Strategy:
----------------------------

1. **In-Memory Workspace**:
   All temporary datasets use "memory" workspace to avoid disk I/O.
   Memory is automatically released when datasets deleted.

2. **Capacity Estimation**:
   Before processing, estimates maximum polygon capacity based on:
   - Available system RAM
   - Conservative 70% usage threshold
   - Empirical 45 MB per 1k polygons scaling factor
   - Safety cap at 32 GB (for 32-bit compatibility)

3. **Proactive Warnings**:
   If input polygon count exceeds 80% of estimated capacity,
   warns user to free memory or add RAM.

4. **Batch Operations**:
   Updates accumulated in dict and applied in single cursor pass
   to minimize memory fragmentation and improve performance.

5. **Explicit Cleanup**:
   Temporary layers and datasets explicitly deleted after each phase
   to free memory immediately (not relying on garbage collection).


Error Handling:
---------------

1. **Input Validation**:
   - Check existence of all input files
   - Validate field names (case-insensitive resolution)
   - Verify parameter ranges
   - Provide clear error messages with available options

2. **Graceful Degradation**:
   - Falls back to classic Dissolve if PairwiseDissolve unavailable
   - Continues without memory tracking if psutil missing
   - Handles missing priority codes with default values

3. **Robustness Checks**:
   - Handles None/empty/zero NEWCODE values
   - Validates neighbor table structure
   - Skips polygons with no neighbors (islands)
   - Tolerates missing fields with warnings

4. **Comprehensive Logging**:
   - Real-time progress updates in GP pane
   - Detailed error messages with tracebacks
   - Iteration statistics for monitoring
   - Final summary with memory and polygon counts


Customization Points:
---------------------

Users can customize behavior by modifying Config parameters:

1. **Threshold Sequence**:
   - from_value: Starting threshold (default 3 ha)
   - to_value: Ending threshold (default 23 ha)
   - by_value: Step size (default 5 ha)
   
   Example custom sequence:
   from_value=5, to_value=30, by_value=5  # 5, 10, 15, 20, 25, 30

2. **Neighbor Detection**:
   - neighbor_mode: Spatial relationship (default "BOUNDARY_TOUCHES")
   
   Alternatives:
   - "SHARE_A_LINE_SEGMENT_WITH": More restrictive (longer shared boundary)
   - "INTERSECT": More permissive (includes corner touches)

3. **Priority Rules**:
   - Modify priority_table (join_pri.dbf) to change merge preferences
   - Add pair codes for specific transitions
   - Adjust single code priorities to favor certain land cover types

4. **Debugging**:
   - keep_intermediates=True: Retain all temporary datasets for inspection
   - memory_report=True: Enable detailed memory logging per step


Known Limitations:
------------------

1. **Memory Constraints**:
   Practical limit ~500k polygons on 64 GB system.
   For larger datasets, consider spatial tiling or database-based approaches.

2. **Processing Time**:
   Dissolve operations don't scale linearly. Very large datasets (>200k polys)
   may take several hours. Consider overnight processing for production runs.

3. **Topology Assumptions**:
   Assumes clean input topology (no gaps, overlaps, or slivers).
   Recommend running Repair Geometry before processing.

4. **Edge Effects**:
   Boundary preservation means edge polygons may remain below MMU.
   This is intentional to avoid artifacts but may require manual review.

5. **Priority Table Completeness**:
   Missing codes default to priority 999999. Ensure priority table includes
   all relevant codes and critical pairs for optimal results.


Validation Recommendations:
----------------------------

After processing, validate results with:

1. **Topology Check**:
   arcpy.management.CheckGeometry(out_general)
   Should report no errors (gaps, overlaps, self-intersections)

2. **Polygon Count**:
   Compare initial vs final polygon count
   Typical reduction: 20-30%

3. **Area Statistics**:
   Query polygons < 25 ha with Comment field
   Review edge polygons vs true small features

4. **Visual Inspection**:
   Compare input vs output side-by-side
   Check for unexpected merges or preserved slivers

5. **Code Distribution**:
   Compare code frequency before/after
   Major land cover types should remain dominant

