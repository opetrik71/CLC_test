# -*- coding: utf-8 -*-
from __future__ import annotations
import arcpy, os, time, traceback
from dataclasses import dataclass


# -------------------- Logger (GP-pane friendly) --------------------
class Logger:
    @staticmethod
    def _gp_msg(txt: str):
        try:
            arcpy.AddMessage(txt)
        except Exception:
            print(txt)

    @staticmethod
    def _gp_warn(txt: str):
        try:
            arcpy.AddWarning(txt)
        except Exception:
            print("[WARN] " + txt)

    @staticmethod
    def _gp_err(txt: str):
        try:
            arcpy.AddError(txt)
        except Exception:
            print("[ERROR] " + txt)

    class Line:
        def __init__(self, title: str):
            self.title = title.rstrip(":")
            self.parts = []
            Logger._gp_msg(f"{self.title}: ")

        def add(self, piece: str):
            self.parts.append(piece)
            Logger._gp_msg(f"  - {piece}")

        def done(self):
            Logger._gp_msg(f"{self.title}: Done")

    def line(self, title: str) -> "Logger.Line":
        return Logger.Line(title)

    def msg(self, title: str, text: str = ""):
        Logger._gp_msg(f"{title}: {text}" if text else f"{title}:")

    def iter(self, msg: str):
        Logger._gp_msg(f"  {msg}")

    def warn(self, msg: str):
        Logger._gp_warn(msg)

    def error(self, msg: str):
        Logger._gp_err(msg)


# -------------------- Config --------------------
@dataclass
class Config:
    # Required inputs: FULL paths
    input_change: str
    input_revision: str
    out_general: str
    priority_table: str  # join_pri.dbf full path
    # Field names (if different)
    priority_code_field: str = "CODE"
    priority_pri_field: str = "PRI"
    # Iterations
    from_value: int = 3
    to_value: int = 23
    by_value: int = 5
    neighbor_mode: str = "BOUNDARY_TOUCHES"
    # Runtime
    keep_intermediates: bool = False


# -------------------- Main Class --------------------
class CorineGeneralizer:
    def __init__(self, cfg: Config, logger: Logger | None = None):
        self.cfg = cfg
        self.log = logger or Logger()

        # GDB/workspace from output feature class
        self.ws = self._extract_gdb_path(self.cfg.out_general)
        if not self.ws or not self.ws.lower().endswith(".gdb"):
            raise ValueError(f"Could not determine .gdb from out_general: {self.cfg.out_general}")

        # Use in-memory for temp features (MUCH FASTER!)
        self.change_copy = "memory/CopyFeatures"
        self.revision_copy = "memory/rev_copy"
        self.diss_c = "memory/diss_c"
        self.diss_r = "memory/diss_r"
        self.union_cr = "memory/union_cr"
        self.mtos_fc = "memory/MToS"
        self.dissolv_tmp = "memory/dissolv_new_code"

        # Keep these in GDB (needed for PolygonNeighbors)
        self.boundary_poly = os.path.join(self.ws, "diss_l")
        self.boundary_line = os.path.join(self.ws, "line")
        self.neigh_table = os.path.join(self.ws, "all_neighbors")

        # Neighbor index will be built once
        self.neighbor_index = {}

        # Fallback relations if primary neighbor_mode doesn't find candidates
        self._neighbor_fallbacks = ["SHARE_A_LINE_SEGMENT_WITH", "INTERSECT", "CONTAINS"]

    # ---------- Public ----------
    def run(self) -> str:
        t0 = time.time()
        try:
            setup = self.log.line("Setup")
            self._validate_inputs();
            setup.add("Inputs ok")
            self._setup_env();
            setup.add("Pro Environment")
            self._cleanup(False);
            setup.add("Cleanup remnants")
            setup.done()

            prep = self.log.line("Preparations")
            self._prepare(prep_line=prep);
            prep.done()

            self.log.msg("Iterator", "running")
            self._run_iterator()

            self.log.msg("Annotate", "final labels")
            self.annotate()

            if not self.cfg.keep_intermediates:
                final = self.log.line("Finalization")
                ld, fd = self._cleanup(False)
                final.add(f"cleanup layers ({ld})")
                final.add(f"cleanup files ({fd})")
                final.done()

            Logger._gp_msg(f"Result feature class: {self.cfg.out_general} - Done [{time.time() - t0:.2f}s]")
            return self.cfg.out_general

        finally:
            # Final cleanup even if exception occurs
            try:
                self._cleanup(False)
                Logger._gp_msg("Final cleanup done.")
            except Exception:
                pass

    # ---------- Validation / Environment ----------
    def _validate_inputs(self):
        for pth, nm in [(self.cfg.input_change, "input_change"),
                        (self.cfg.input_revision, "input_revision"),
                        (self.cfg.priority_table, "priority_table (join_pri.dbf)")]:
            if not arcpy.Exists(pth):
                raise FileNotFoundError(f"{nm} does not exist: {pth}")
        # Field names in PRI table (case-insensitive resolution)
        self.cfg.priority_code_field = self._resolve_field_name(self.cfg.priority_table, self.cfg.priority_code_field)
        self.cfg.priority_pri_field = self._resolve_field_name(self.cfg.priority_table, self.cfg.priority_pri_field)

        # Iter parameters: ensure valid
        if self.cfg.by_value is None or int(self.cfg.by_value) == 0:
            self.log.warn("Parameter 'by_value' was empty/0 → default 5 used.")
            self.cfg.by_value = 5
        if self.cfg.from_value is None:
            self.log.warn("Parameter 'from_value' was empty → default 3 used.")
            self.cfg.from_value = 3
        if self.cfg.to_value is None:
            self.log.warn("Parameter 'to_value' was empty → default 23 used.")
            self.cfg.to_value = 23

    def _setup_env(self):
        arcpy.env.workspace = self.ws
        arcpy.env.scratchWorkspace = self.ws
        arcpy.env.overwriteOutput = True
        arcpy.env.parallelProcessingFactor = "100%"  # Use all cores for better performance
        arcpy.env.qualifiedFieldNames = False
        arcpy.env.addOutputsToMap = False

    def _cleanup(self, verbose: bool = False) -> tuple[int, int]:
        layers = [
            "filled_ga_l", "select_sp_l", "l_l", "inside_area_l", "boundary_touch_l",
            "Neighbour Areas Layer", "fl_ai", "all_polys_lyr", "small_not_boundary_lyr", "boundary_lyr",
            "__affected__", "fl_anno"
        ]
        # Memory cleanup
        memory_items = [
            "memory/CopyFeatures", "memory/rev_copy", "memory/diss_c", "memory/diss_r",
            "memory/union_cr", "memory/MToS", "memory/dissolv_new_code"
        ]
        # GDB files
        gdb_files = [
            "diss_l", "line", "all_neighbors", "dissolv_new_code_sp"
        ]

        ld, fd = 0, 0
        # Clean layers
        for l in layers:
            try:
                if arcpy.Exists(l): arcpy.Delete_management(l); ld += 1
            except Exception as e:
                if verbose: Logger._gp_warn(f"[cleanup] layer err {l}: {e}")

        # Clean memory
        for m in memory_items:
            try:
                if arcpy.Exists(m): arcpy.Delete_management(m); fd += 1
            except Exception as e:
                if verbose: Logger._gp_warn(f"[cleanup] memory err {m}: {e}")

        # Clean GDB files
        for f in gdb_files:
            try:
                p = os.path.join(self.ws, f)
                if arcpy.Exists(p): arcpy.Delete_management(p); fd += 1
            except Exception as e:
                if verbose: Logger._gp_warn(f"[cleanup] file err {f}: {e}")

        return ld, fd

    # ---------- Preparation ----------
    def _prepare(self, prep_line: Logger.Line | None = None):
        # Change
        arcpy.management.CopyFeatures(self.cfg.input_change, self.change_copy)
        if "NEWCODE" not in [f.name for f in arcpy.ListFields(self.change_copy)]:
            arcpy.management.AddField(self.change_copy, "NEWCODE", "LONG")
        arcpy.management.CalculateField(
            self.change_copy, "NEWCODE", "121 if !CHCODE! in (1211,1212) else !CHCODE!", "PYTHON3"
        )
        if prep_line: prep_line.add("Change database")

        # Revision
        arcpy.management.CopyFeatures(self.cfg.input_revision, self.revision_copy)
        if "OLDCODE" not in [f.name for f in arcpy.ListFields(self.revision_copy)]:
            arcpy.management.AddField(self.revision_copy, "OLDCODE", "LONG")
        arcpy.management.CalculateField(
            self.revision_copy, "OLDCODE", "121 if !REVCODE! in (1211,1212) else !REVCODE!", "PYTHON3"
        )
        if prep_line: prep_line.add("Revision database")

        # Union (change + revision dissolves)
        arcpy.management.Dissolve(self.change_copy, self.diss_c, ["NEWCODE"], "", "SINGLE_PART", "DISSOLVE_LINES")
        arcpy.management.Dissolve(self.revision_copy, self.diss_r, ["OLDCODE"], "", "SINGLE_PART", "DISSOLVE_LINES")

        # Try PairwiseUnion for better performance
        try:
            arcpy.analysis.PairwiseUnion([self.diss_r, self.diss_c], self.union_cr)
        except:
            arcpy.analysis.Union([[self.diss_r, ""], [self.diss_c, ""]], self.union_cr, "NO_FID", "", "GAPS")

        if prep_line: prep_line.add("Union")

        # MToS
        arcpy.management.MultipartToSinglepart(self.union_cr, self.mtos_fc)
        if prep_line: prep_line.add("MToS")

        # NEWCODE robust fill (if empty/None/0 → OLDCODE)
        arcpy.management.CalculateField(
            self.mtos_fc,
            "NEWCODE",
            "!OLDCODE! if (!NEWCODE! is None or (isinstance(!NEWCODE!,str) and !NEWCODE!=='') or !NEWCODE! == 0) else !NEWCODE!",
            "PYTHON3"
        )
        with arcpy.da.UpdateCursor(self.mtos_fc, ["NEWCODE", "OLDCODE"]) as cur:
            for newc, oldc in cur:
                if (newc is None) or (newc == 0) or (isinstance(newc, str) and str(newc).strip() == ""):
                    cur.updateRow([oldc, oldc])

        # Generalized output (delete first if exists)
        if arcpy.Exists(self.cfg.out_general):
            arcpy.management.Delete(self.cfg.out_general)
        arcpy.management.Dissolve(self.mtos_fc, self.cfg.out_general, ["NEWCODE"], "", "SINGLE_PART", "DISSOLVE_LINES")
        if prep_line: prep_line.add("Generalized output")

        # Boundary line (from revision)
        if "diss" not in [f.name for f in arcpy.ListFields(self.diss_r)]:
            arcpy.management.AddField(self.diss_r, "diss", "LONG")
        arcpy.management.CalculateField(self.diss_r, "diss", "1", "PYTHON3")
        arcpy.management.Dissolve(self.diss_r, self.boundary_poly, ["diss"], "", "MULTI_PART", "DISSOLVE_LINES")
        arcpy.management.PolygonToLine(self.boundary_poly, self.boundary_line, "IDENTIFY_NEIGHBORS")
        if prep_line: prep_line.add("Boundary line")

        # AREA+GID (only calculate area once at start)
        self._ensure_gid_area(self.cfg.out_general, force_area=True)

    # ---------- ITERATOR ----------
    def _run_iterator(self):
        # Priority map with STRING key
        code_f = self._resolve_field_name(self.cfg.priority_table, self.cfg.priority_code_field)
        pri_f = self._resolve_field_name(self.cfg.priority_table, self.cfg.priority_pri_field)
        pri_map: dict[str, int] = {}
        with arcpy.da.SearchCursor(self.cfg.priority_table, [code_f, pri_f]) as cur:
            for code, pri in cur:
                if pri is None or code is None: continue
                key = self._code_to_str(code)
                try:
                    pri_map[key] = int(pri)
                except:
                    continue

        # Generate neighbors table ONCE at the beginning (HUGE performance gain)
        self.log.msg("Generating neighbor relationships", "(one-time computation)")
        if arcpy.Exists(self.neigh_table):
            arcpy.management.Delete(self.neigh_table)

        arcpy.analysis.PolygonNeighbors(
            self.cfg.out_general,
            self.neigh_table,
            "",
            "NO_AREA_OVERLAP",
            "BOTH_SIDES"  # Get both directions
        )

        # Build neighbor index in memory
        self.neighbor_index = self._build_neighbor_index(self.neigh_table)

        # Boundary layer
        boundary_lyr = "boundary_lyr"
        if arcpy.Exists(boundary_lyr): arcpy.management.Delete(boundary_lyr)
        arcpy.management.MakeFeatureLayer(self.boundary_line, boundary_lyr)

        # Iterations
        start = int(self.cfg.from_value)
        stop = int(self.cfg.to_value)
        step = int(self.cfg.by_value) if int(self.cfg.by_value) != 0 else 5

        for val in (range(start, stop + 1, step) if stop >= start else []):
            self._one_iteration_fast(val, boundary_lyr, pri_map)
            # Global dissolve after EACH iteration
            self._global_dissolve_and_refresh()
            # Rebuild neighbor index for next iteration
            if val < stop:  # Don't rebuild on last iteration
                self._rebuild_neighbor_index()

        self._ensure_gid_area(self.cfg.out_general, force_area=True)

    def _build_neighbor_index(self, neigh_table):
        """Build a dictionary of neighbors from the table"""
        index = {}
        fields = [f.name for f in arcpy.ListFields(neigh_table)]

        # Find the correct field names
        src_field = next((f for f in fields if 'src' in f.lower() and ('objectid' in f.lower() or 'fid' in f.lower())),
                         None)
        nbr_field = next((f for f in fields if 'nbr' in f.lower() and ('objectid' in f.lower() or 'fid' in f.lower())),
                         None)

        if not src_field or not nbr_field:
            self.log.warn(f"Could not find neighbor fields in {fields}")
            return index

        with arcpy.da.SearchCursor(neigh_table, [src_field, nbr_field]) as cursor:
            for src, nbr in cursor:
                if src != nbr:
                    index.setdefault(int(src), set()).add(int(nbr))
                    index.setdefault(int(nbr), set()).add(int(src))  # Both directions

        return index

    def _rebuild_neighbor_index(self):
        """Rebuild neighbor index after dissolve"""
        if arcpy.Exists(self.neigh_table):
            arcpy.management.Delete(self.neigh_table)

        arcpy.analysis.PolygonNeighbors(
            self.cfg.out_general,
            self.neigh_table,
            "",
            "NO_AREA_OVERLAP",
            "BOTH_SIDES"
        )

        self.neighbor_index = self._build_neighbor_index(self.neigh_table)

    def _one_iteration_fast(self, val: int, boundary_lyr: str, pri_map: dict[str, int]):
        # 1) Small layer (AREA < val)
        small_lyr = "small_not_boundary_lyr"
        if arcpy.Exists(small_lyr): arcpy.management.Delete(small_lyr)
        arcpy.management.MakeFeatureLayer(self.cfg.out_general, small_lyr, where_clause=f"AREA < {val}")

        # 2) Edge exclusion within small layer
        arcpy.management.SelectLayerByLocation(small_lyr, self.cfg.neighbor_mode, boundary_lyr,
                                               selection_type="NEW_SELECTION")
        arcpy.management.SelectLayerByLocation(small_lyr, self.cfg.neighbor_mode, boundary_lyr,
                                               selection_type="SWITCH_SELECTION")

        sel_count = int(arcpy.management.GetCount(small_lyr)[0]) if arcpy.Exists(small_lyr) else 0
        if sel_count == 0:
            self.log.iter(f"Iter({val}ha): selected=0 (skip)")
            return

        # Get all polygon data in one pass
        poly_data = {}
        with arcpy.da.SearchCursor(self.cfg.out_general, ["OBJECTID", "NEWCODE", "AREA"]) as cursor:
            for oid, code, area in cursor:
                poly_data[oid] = (self._code_to_str(code), float(area or 0.0))

        # Get small polygon OIDs
        small_oids = set()
        with arcpy.da.SearchCursor(small_lyr, ["OID@"]) as cursor:
            for (oid,) in cursor:
                small_oids.add(oid)

        # Collect ALL updates first (batch processing)
        all_updates = {}
        no_neighbor = 0
        non_numeric = 0

        for oid in small_oids:
            small_code, _small_area = poly_data.get(oid, (None, 0.0))

            # Use pre-computed neighbor index
            neighbors = self.neighbor_index.get(oid, set())

            if not neighbors:
                no_neighbor += 1
                continue

            # Evaluate neighbors
            candidates = []
            for nb_oid in neighbors:
                nb_code, nb_area = poly_data.get(nb_oid, (None, 0.0))
                if not nb_code:
                    continue

                # Scoring (identical code preference)
                identical = (small_code is not None and nb_code == small_code)
                if identical:
                    real_pri = 0
                else:
                    nb_pri_single = pri_map.get(nb_code, 999999)
                    pair_code = (small_code or "") + nb_code if small_code is not None else nb_code
                    real_pri = pri_map.get(pair_code, nb_pri_single)

                candidates.append((real_pri, -nb_area, nb_code))

            if not candidates:
                no_neighbor += 1
                continue

            candidates.sort()
            best_code = candidates[0][2]

            # Check if numeric
            try:
                new_val = int(best_code)
                all_updates[oid] = new_val
            except:
                non_numeric += 1

        # Single update cursor for ALL changes (much faster)
        updated = 0
        if all_updates:
            with arcpy.da.UpdateCursor(self.cfg.out_general, ["OBJECTID", "NEWCODE"]) as cursor:
                for oid, code in cursor:
                    if oid in all_updates:
                        cursor.updateRow([oid, all_updates[oid]])
                        updated += 1

        self._ensure_gid_area(self.cfg.out_general, force_area=False)  # Don't recalc area
        self.log.iter(
            f"Iter({val}ha): selected={sel_count}, updated={updated}, no_neighbor={no_neighbor}, non_numeric={non_numeric}")

    # ---------- Supporting ----------
    def _global_dissolve_and_refresh(self):
        """NEWCODE global dissolve, then overwrite out_general."""
        if arcpy.Exists(self.dissolv_tmp):
            arcpy.management.Delete(self.dissolv_tmp)

        # Try different PairwiseDissolve syntaxes
        success = False

        # Attempt 1: With all parameters
        if not success:
            try:
                arcpy.analysis.PairwiseDissolve(
                    self.cfg.out_general,
                    self.dissolv_tmp,
                    "NEWCODE",  # Single field as string
                    None,  # statistics_fields
                    "SINGLE_PART"
                )
                success = True
            except:
                pass

        # Attempt 2: Without multi_part parameter
        if not success:
            try:
                arcpy.analysis.PairwiseDissolve(
                    self.cfg.out_general,
                    self.dissolv_tmp,
                    "NEWCODE"
                )
                # Convert to single-part if needed
                sp_tmp = self.dissolv_tmp + "_sp"
                if arcpy.Exists(sp_tmp):
                    arcpy.management.Delete(sp_tmp)
                arcpy.management.MultipartToSinglepart(self.dissolv_tmp, sp_tmp)
                arcpy.management.Delete(self.dissolv_tmp)
                arcpy.management.Rename(sp_tmp, self.dissolv_tmp)
                success = True
            except:
                pass

        # Fallback to regular Dissolve
        if not success:
            arcpy.management.Dissolve(
                self.cfg.out_general,
                self.dissolv_tmp,
                ["NEWCODE"], "", "SINGLE_PART", "DISSOLVE_LINES"
            )

        # Replace out_general with dissolved result
        if arcpy.Exists(self.cfg.out_general):
            arcpy.management.Delete(self.cfg.out_general)
        arcpy.management.CopyFeatures(self.dissolv_tmp, self.cfg.out_general)
        self._ensure_gid_area(self.cfg.out_general, force_area=True)

    def annotate(self):
        """<25 ha → 'Smaller than MMU'; boundary touching → 'Edge polygon'; ≥25 ha: empty"""
        fc = self.cfg.out_general

        # Add Comment field if not exists
        if "Comment" not in [f.name for f in arcpy.ListFields(fc)]:
            arcpy.management.AddField(fc, "Comment", "TEXT", field_length=255)

        fl = "fl_anno"
        if arcpy.Exists(fl): arcpy.management.Delete(fl)
        arcpy.management.MakeFeatureLayer(fc, fl)

        # Smaller than MMU (<25 ha)
        arcpy.management.SelectLayerByAttribute(fl, "NEW_SELECTION", "AREA < 25")
        arcpy.management.CalculateField(fl, "Comment", "'Smaller than MMU'", "PYTHON3")

        # Edge polygons (<25 ha + touches boundary)
        arcpy.management.SelectLayerByAttribute(fl, "NEW_SELECTION", "AREA < 25")
        arcpy.management.SelectLayerByLocation(fl, "BOUNDARY_TOUCHES", self.boundary_line,
                                               selection_type="SUBSET_SELECTION")
        arcpy.management.CalculateField(fl, "Comment", "'Edge polygon'", "PYTHON3")

        arcpy.management.SelectLayerByAttribute(fl, "CLEAR_SELECTION")
        arcpy.management.Delete(fl)

    @staticmethod
    def _resolve_field_name(table: str, wanted: str) -> str:
        wl = wanted.lower()
        for f in (arcpy.ListFields(table) or []):
            if f.name.lower() == wl: return f.name
        raise ValueError(f"Field '{wanted}' not found in {table}. Avail: {[f.name for f in arcpy.ListFields(table)]}")

    @staticmethod
    def _code_to_str(code_val) -> str | None:
        """None, empty or whitespace → None; otherwise cleaned string."""
        if code_val is None:
            return None
        try:
            if isinstance(code_val, (int, float)):
                return str(int(code_val))
            s = str(code_val).strip()
            return s if s != "" else None
        except:
            return None

    @staticmethod
    def _ensure_gid_area(fc: str, force_area: bool = False):
        """Only calculate area when needed to save time"""
        names = [f.name for f in arcpy.ListFields(fc)]

        if "GID" not in names:
            arcpy.management.AddField(fc, "GID", "DOUBLE")
            arcpy.management.CalculateField(fc, "GID", "!OBJECTID!", "PYTHON3")

        if force_area or "AREA" not in names:
            if "AREA" not in names:
                arcpy.management.AddField(fc, "AREA", "DOUBLE")
            arcpy.management.CalculateField(fc, "AREA", "!shape.area@HECTARES!", "PYTHON3")

    @staticmethod
    def _extract_gdb_path(path: str) -> str:
        """Extract file GDB root from out_general full path, even for datasets."""
        try:
            p = arcpy.Describe(path).path
            while p and not p.lower().endswith(".gdb"):
                p = arcpy.Describe(p).path
            return p or ""
        except Exception:
            low = path.lower()
            idx = low.rfind(".gdb")
            if idx == -1:
                return ""
            return path[:idx + 4].rstrip("\\/")


# -------------------- Script Tool wrapper --------------------
def script_tool():
    """
    Script Tool parameters:
      0: input_change (FC)
      1: input_revision (FC)
      2: out_general (FC, derived/output)
      3: priority_table (DBF)
      4: from_value (int, optional; default 3)
      5: to_value   (int, optional; default 23)
      6: by_value   (int, optional; default 5)
    """
    p0 = arcpy.GetParameterAsText(0)  # change
    p1 = arcpy.GetParameterAsText(1)  # revision
    p2 = arcpy.GetParameterAsText(2)  # generalized (output path)
    p3 = arcpy.GetParameterAsText(3)  # join_pri.dbf
    p4 = arcpy.GetParameter(4)  # from (Long)
    p5 = arcpy.GetParameter(5)  # to   (Long)
    p6 = arcpy.GetParameter(6)  # by   (Long)

    cfg = Config(
        input_change=p0,
        input_revision=p1,
        out_general=p2,
        priority_table=p3,
        from_value=int(p4) if p4 not in (None, "") else 3,
        to_value=int(p5) if p5 not in (None, "") else 23,
        by_value=int(p6) if p6 not in (None, "") else 5,
        neighbor_mode="BOUNDARY_TOUCHES",
        keep_intermediates=False
    )

    result_fc = CorineGeneralizer(cfg).run()
    arcpy.SetParameterAsText(2, result_fc)


# -------------------- Entrypoint --------------------
if __name__ == "__main__":
    try:
        script_tool()
    except Exception:
        tb = traceback.format_exc()
        try:
            arcpy.AddError(tb)
        except Exception:
            print(tb)
        raise
