
---  to the planned appearance of the repo ---

## ⚙️ Main Scripts  

### `prod/corine_iterator.py`
- Ready-to-use ArcGIS **Script Tool** version  
- Compatible with ArcGIS Pro 3.3 +  
- GP-pane friendly messaging (`arcpy.AddMessage`, `AddWarning`, etc.)

### `dev/clc_generalizer_dev.py`
- Optimized / experimental version  
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
| `keep_intermediates` | Boolean | False | Keep temporary datasets |

---

## 🧠 Workflow Overview

1. **Prepare Input Copies**
   - Change & Revision datasets copied to memory.
   - Codes normalized (e.g., `1211/1212 → 121`).

2. **Union + Multipart → Singlepart**
   - Combined geometry between Change and Revision.

3. **Fill `NEWCODE` field**
   - Ensures all polygons have valid class codes.

4. **Iterative Generalization**
   - Iterates over size thresholds (3 → 23 ha).
   - Small polygons are merged with their best neighbor:
     - Prefers same code or lowest `PRI` value.
     - Uses pre-computed neighbor index (PolygonNeighbors).

5. **Dissolve after each iteration**
   - Keeps geometry clean and merged.

6. **Annotation**
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

1. Create a new **Script Tool** in a Toolbox.
2. Set the script path to `prod/clc_gener_tool.py`.
3. Define parameters as:

| Index | Name | Data Type | Direction | Default |
|-------|------|------------|------------|----------|
| 0 | input_change | Feature Class | Input | — |
| 1 | input_revision | Feature Class | Input | — |
| 2 | out_general | Feature Class | Output | — |
| 3 | priority_table | Table | Input | join_pri.dbf |
| 4 | from_value | Long | Input | 3 |
| 5 | to_value | Long | Input | 23 |
| 6 | by_value | Long | Input | 5 |

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
| 1:100.000 mapsheet | ~800 | 86 s | 50 s |
| country-wide full | ~50 000 | 5600 s | 703 s |

---

## 📜 License
This project is licensed under the **GNU General Public License v3.0 (GPL-3.0)**  
with an additional **non-commercial use restriction**.

You may use, modify, and distribute the code freely for **research, education, or
public administration** purposes, provided that derivative works remain open source
under the same license.  
Commercial redistribution or integration into closed-source software is **not allowed**
without prior written permission from the author.

See the [LICENSE](./LICENSE) file or visit  
[https://www.gnu.org/licenses/gpl-3.0.html](https://www.gnu.org/licenses/gpl-3.0.html) for details.
---

## 🙌 Contributors

| Name | Contribution |
|------|---------------|
| **O. Petrik** | Project lead, core algorithm design, CLC domain logic |
| **Claude Opus 4.1** | Performance optimization (batch updates, neighbor caching) |
| **ChatGPT (GPT-5)** | ArcGIS Pro script refactoring and optimization, toolbox integration, documentation, QA, and environment automation |


---

## 🧩 Notes
- `PairwiseDissolve` used for faster topology rebuilds (fallbacks to `Dissolve` if unsupported).
- Tested with ArcGIS Pro 3.5.3, Python 3.11 (arcpy 3.5).
- Optimized for both tile-level and full-country generalization workflows.
