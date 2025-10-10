
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
