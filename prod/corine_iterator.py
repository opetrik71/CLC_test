# -*- coding: utf-8 -*-
from __future__ import annotations
import arcpy, os, time, traceback, gc
from dataclasses import dataclass

# ---- psutil (opcionális) ----
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


# -------------------- Logger (GP-pane friendly, streamelt blokkokkal) --------------------
class Logger:
    @staticmethod
    def _gp_msg(txt: str):
        try: arcpy.AddMessage(txt)
        except Exception: print(txt)

    @staticmethod
    def _gp_warn(txt: str):
        try: arcpy.AddWarning(txt)
        except Exception: print("[WARN] " + txt)

    @staticmethod
    def _gp_err(txt: str):
        try: arcpy.AddError(txt)
        except Exception: print("[ERROR] " + txt)

    # Egylépéses üzenetek
    def msg(self, title: str, text: str = ""):
        self._gp_msg(f"{title}: {text}" if text else f"{title}:")
    def iter(self, msg: str):
        self._gp_msg(f"  {msg}")
    def warn(self, msg: str):
        self._gp_warn(msg)
    def error(self, msg: str):
        self._gp_err(msg)

    # Streamelt blokkok (azonnali, soronkénti kiírás)
    class Stream:
        def __init__(self, title: str):
            self.title = title.rstrip(":")
            Logger._gp_msg(f"{self.title}: ")
            self._done = False
        def step(self, piece: str):
            Logger._gp_msg(f"  - {piece}")
            return self
        def done(self):
            if not self._done:
                Logger._gp_msg(f"{self.title}: Done")
                self._done = True

    def stream(self, title: str) -> "Logger.Stream":
        return Logger.Stream(title)


# -------------------- Memóriamérő (csak ha psutil elérhető) --------------------
class MemoryTracker:
    def __init__(self, logger, enabled: bool):
        self.log = logger
        self.enabled = enabled and HAS_PSUTIL
        if self.enabled:
            self.process = psutil.Process()
            self.system_total_gb = psutil.virtual_memory().total / (1024**3)
            self.system_available_gb = psutil.virtual_memory().available / (1024**3)
            self.start_mb = self.process.memory_info().rss / (1024**2)
            self.peak_mb = self.start_mb

    # “riport” hívások csak akkor írnak ki, ha enabled=True
    def report(self, *_args, **_kwargs):
        if not self.enabled:
            return
        gc.collect()
        mem = self.process.memory_info().rss / (1024**2)
        self.peak_mb = max(self.peak_mb, mem)

    def final_summary(self):
        if not self.enabled:
            return
        self.log.msg("Memory Summary:")
        self.log.iter(f"Peak usage: {self.peak_mb:.0f} MB")
        self.log.iter(f"Total consumed: {self.peak_mb - self.start_mb:.0f} MB")


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
    # Opcionális memória-riportok (iteráció közbeni részletek)
    memory_report: bool = False


# -------------------- Fő osztály --------------------
class CorineGeneralizer:
    def __init__(self, cfg: Config, logger: Logger | None = None):
        self.cfg = cfg
        self.log = logger or Logger()

        # GDB/workspace az output FC alapján
        self.ws = self._extract_gdb_path(self.cfg.out_general)
        if not self.ws or not self.ws.lower().endswith(".gdb"):
            raise ValueError(f"Could not determine .gdb from out_general: {self.cfg.out_general}")

        # Memóriakövető (csak részletes riportokhoz; a kapacitás-becsléshez közvetlen psutil-t használunk)
        self.mem = MemoryTracker(self.log, enabled=self.cfg.memory_report)

        # In-memory ideiglenes
        self.change_copy = "memory/CopyFeatures"
        self.revision_copy = "memory/rev_copy"
        self.diss_c = "memory/diss_c"
        self.diss_r = "memory/diss_r"
        self.union_cr = "memory/union_cr"
        self.mtos_fc = "memory/MToS"
        self.dissolv_tmp = "memory/dissolv_new_code"

        # GDB-ben maradó (szomszéd-táblához)
        self.boundary_poly = os.path.join(self.ws, "diss_l")
        self.boundary_line = os.path.join(self.ws, "line")
        self.neigh_table = os.path.join(self.ws, "all_neighbors")

        self.neighbor_index = {}  # iterációnként újraépítjük

    # ---------- public ----------
    def run(self) -> str:
        t0 = time.time()

        # Induló információk: kapacitás + input poligonszámok (mindig az elején)
        self._print_capacity_and_inputs()

        try:
            s = self.log.stream("Setup")
            self._validate_inputs();         s.step("Input inspections")
            self._setup_env();               s.step("Pro Environment")
            self._cleanup(False);            s.step("Cleanup remnants")
            s.done()
            self.mem.report("after setup")

            p = self.log.stream("Preparations")
            self._prepare(prep_stream=p)
            p.done()
            self.mem.report("after preparations")

            # kezdő poligonszám (info)
            try:
                init_cnt = int(arcpy.management.GetCount(self.cfg.out_general)[0])
                self.log.msg(f"Initial polygon count: {init_cnt}")
            except Exception:
                pass

            self.log.msg("Iterator", "running")
            self._run_iterator()

            self.log.msg("Annotate", "final labels")
            self.annotate()

            fz = self.log.stream("Finalization")
            if not self.cfg.keep_intermediates:
                ld, fd = self._cleanup(False)
                fz.step(f"cleanup layers ({ld})").step(f"cleanup files ({fd})")
            fz.done()

            try:
                fin_cnt = int(arcpy.management.GetCount(self.cfg.out_general)[0])
                self.log.msg(f"Final polygon count: {fin_cnt}")
            except Exception:
                pass

            self.mem.final_summary()
            Logger._gp_msg(f"Result feature class: {self.cfg.out_general} - Done [{time.time()-t0:.2f}s]")
            return self.cfg.out_general

        finally:
            try:
                self._cleanup(False)
                Logger._gp_msg("Final cleanup done.")
            except Exception:
                pass

    # ---------- init helpers ----------
    def _print_capacity_and_inputs(self):
        """Első sorok: memória adatok + kapacitás becslés + input poligonszámok (+ ajánlás)."""
        est_capacity = None
        if HAS_PSUTIL:
            vm = psutil.virtual_memory()
            total_gb = vm.total / (1024**3)
            avail_gb = vm.available / (1024**3)
            self.log.msg("System Memory", f"{total_gb:.1f} GB total, {avail_gb:.1f} GB available")

            # “biztonságos” keret: min(32 GB, 70% a teljesből)
            safe_total_gb = min(32.0, total_gb * 0.70)
            # empirikus skála (MB / 1000 polygon) – óvatos becslés (~40-50 MB/1k)
            mb_per_1k = 45.0
            est_capacity = int((safe_total_gb * 1024) / mb_per_1k * 1000)
            self.log.msg("Estimated capacity (max polys) based on available memory", f"{est_capacity:,}")
        else:
            self.log.msg("System Memory", "not available (psutil missing)")

        # Input poligonszámok
        try:
            rev_cnt = int(arcpy.management.GetCount(self.cfg.input_revision)[0])
        except Exception:
            rev_cnt = 0
        try:
            cha_cnt = int(arcpy.management.GetCount(self.cfg.input_change)[0])
        except Exception:
            cha_cnt = 0
        total = rev_cnt + cha_cnt
        self.log.msg("Input databases", f"{rev_cnt:,} revision + {cha_cnt:,} change = {total:,} polygons")

        # Ajánlás, ha közel a plafonhoz
        if est_capacity and total >= 0.8 * est_capacity:
            self.log.warn("Your inputs approach available memory capacity. "
                          "Please consider freeing memory (close other apps) or adding more RAM.")


    def _validate_inputs(self):
        for pth, nm in [(self.cfg.input_change, "input_change"),
                        (self.cfg.input_revision, "input_revision"),
                        (self.cfg.priority_table, "priority_table (join_pri.dbf)")]:
            if not arcpy.Exists(pth):
                raise FileNotFoundError(f"{nm} does not exist: {pth}")
        # PRI tábla mezőnevek feloldása (case-insensitive)
        self.cfg.priority_code_field = self._resolve_field_name(self.cfg.priority_table, self.cfg.priority_code_field)
        self.cfg.priority_pri_field  = self._resolve_field_name(self.cfg.priority_table, self.cfg.priority_pri_field)
        # iter paraméterek
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
        arcpy.env.parallelProcessingFactor = "100%"
        arcpy.env.qualifiedFieldNames = False
        arcpy.env.addOutputsToMap = False

    def _cleanup(self, verbose: bool = False) -> tuple[int, int]:
        layers = [
            "filled_ga_l","select_sp_l","l_l","inside_area_l","boundary_touch_l",
            "Neighbour Areas Layer","fl_ai","all_polys_lyr","small_not_boundary_lyr","boundary_lyr",
            "__affected__","fl_anno"
        ]
        mem_items = [
            "memory/CopyFeatures","memory/rev_copy","memory/diss_c","memory/diss_r",
            "memory/union_cr","memory/MToS","memory/dissolv_new_code"
        ]
        gdb_files = ["diss_l","line","all_neighbors","dissolv_new_code_sp"]

        ld, fd = 0, 0
        for l in layers:
            try:
                if arcpy.Exists(l): arcpy.Delete_management(l); ld += 1
            except Exception as e:
                if verbose: self.log.warn(f"[cleanup] layer err {l}: {e}")
        for m in mem_items:
            try:
                if arcpy.Exists(m): arcpy.Delete_management(m); fd += 1
            except Exception as e:
                if verbose: self.log.warn(f"[cleanup] memory err {m}: {e}")
        for f in gdb_files:
            try:
                p = os.path.join(self.ws, f)
                if arcpy.Exists(p): arcpy.Delete_management(p); fd += 1
            except Exception as e:
                if verbose: self.log.warn(f"[cleanup] file err {f}: {e}")
        return ld, fd

    # ---------- előkészítés ----------
    def _prepare(self, prep_stream: Logger.Stream | None = None):
        # Change
        arcpy.management.CopyFeatures(self.cfg.input_change, self.change_copy)
        if "NEWCODE" not in [f.name for f in arcpy.ListFields(self.change_copy)]:
            arcpy.management.AddField(self.change_copy,"NEWCODE","LONG")
        arcpy.management.CalculateField(
            self.change_copy,"NEWCODE","121 if !CHCODE! in (1211,1212) else !CHCODE!","PYTHON3")
        if prep_stream: prep_stream.step("Change database")

        # Revision
        arcpy.management.CopyFeatures(self.cfg.input_revision, self.revision_copy)
        if "OLDCODE" not in [f.name for f in arcpy.ListFields(self.revision_copy)]:
            arcpy.management.AddField(self.revision_copy,"OLDCODE","LONG")
        arcpy.management.CalculateField(
            self.revision_copy,"OLDCODE","121 if !REVCODE! in (1211,1212) else !REVCODE!","PYTHON3")
        if prep_stream: prep_stream.step("Revision database")

        # Dissolve (Pairwise, ha lehet; különben klasszikus)
        try:
            arcpy.analysis.PairwiseDissolve(self.change_copy, self.diss_c, "NEWCODE", None, "SINGLE_PART")
        except:
            arcpy.management.Dissolve(self.change_copy, self.diss_c, ["NEWCODE"], "", "SINGLE_PART", "DISSOLVE_LINES")
        try:
            arcpy.analysis.PairwiseDissolve(self.revision_copy, self.diss_r, "OLDCODE", None, "SINGLE_PART")
        except:
            arcpy.management.Dissolve(self.revision_copy, self.diss_r, ["OLDCODE"], "", "SINGLE_PART", "DISSOLVE_LINES")

        # Union
        arcpy.analysis.Union([[self.diss_r,""],[self.diss_c,""]], self.union_cr,"NO_FID","","GAPS")
        if prep_stream: prep_stream.step("Union")

        # MToS
        arcpy.management.MultipartToSinglepart(self.union_cr, self.mtos_fc)
        if prep_stream: prep_stream.step("MToS")

        # NEWCODE robusztus
        arcpy.management.CalculateField(
            self.mtos_fc,"NEWCODE",
            "!OLDCODE! if (!NEWCODE! is None or (isinstance(!NEWCODE!,str) and !NEWCODE!=='') or !NEWCODE! == 0) else !NEWCODE!",
            "PYTHON3"
        )
        with arcpy.da.UpdateCursor(self.mtos_fc,["NEWCODE","OLDCODE"]) as cur:
            for newc, oldc in cur:
                if (newc is None) or (newc == 0) or (isinstance(newc,str) and str(newc).strip()==""):
                    cur.updateRow([oldc,oldc])

        # Generalized output (overwrite)
        if arcpy.Exists(self.cfg.out_general):
            arcpy.management.Delete(self.cfg.out_general)
        arcpy.management.Dissolve(self.mtos_fc, self.cfg.out_general, ["NEWCODE"], "","SINGLE_PART","DISSOLVE_LINES")
        if prep_stream: prep_stream.step("Generalized output")

        # Boundary line (revisionből)
        if "diss" not in [f.name for f in arcpy.ListFields(self.diss_r)]:
            arcpy.management.AddField(self.diss_r,"diss","LONG")
        arcpy.management.CalculateField(self.diss_r,"diss","1","PYTHON3")
        arcpy.management.Dissolve(self.diss_r, self.boundary_poly, ["diss"], "","MULTI_PART","DISSOLVE_LINES")
        arcpy.management.PolygonToLine(self.boundary_poly, self.boundary_line, "IDENTIFY_NEIGHBORS")
        if prep_stream: prep_stream.step("Boundary line")

        # AREA+GID
        self._ensure_gid_area(self.cfg.out_general, force_area=True)

    # ---------- ITERÁTOR ----------
    def _run_iterator(self):
        # prioritás-térkép
        code_f = self._resolve_field_name(self.cfg.priority_table, self.cfg.priority_code_field)
        pri_f  = self._resolve_field_name(self.cfg.priority_table, self.cfg.priority_pri_field)
        pri_map: dict[str,int] = {}
        with arcpy.da.SearchCursor(self.cfg.priority_table,[code_f,pri_f]) as cur:
            for code, pri in cur:
                if pri is None or code is None: continue
                key = self._code_to_str(code)
                try: pri_map[key] = int(pri)
                except: continue

        # egyszeri PolygonNeighbors + index
        if arcpy.Exists(self.neigh_table): arcpy.management.Delete(self.neigh_table)
        arcpy.analysis.PolygonNeighbors(self.cfg.out_general, self.neigh_table, "", "NO_AREA_OVERLAP", "BOTH_SIDES")
        self.neighbor_index = self._build_neighbor_index(self.neigh_table)

        # boundary layer
        boundary_lyr = "boundary_lyr"
        if arcpy.Exists(boundary_lyr): arcpy.management.Delete(boundary_lyr)
        arcpy.management.MakeFeatureLayer(self.boundary_line, boundary_lyr)

        # iterációk
        start = int(self.cfg.from_value)
        stop  = int(self.cfg.to_value)
        step  = int(self.cfg.by_value) if int(self.cfg.by_value) != 0 else 5

        for val in (range(start, stop+1, step) if stop>=start else []):
            self._one_iteration_fast(val, boundary_lyr, pri_map)
            self._global_dissolve_and_refresh()
            if val < stop:
                # rebuild neighbors a következő körre
                if arcpy.Exists(self.neigh_table): arcpy.management.Delete(self.neigh_table)
                arcpy.analysis.PolygonNeighbors(self.cfg.out_general, self.neigh_table, "", "NO_AREA_OVERLAP", "BOTH_SIDES")
                self.neighbor_index = self._build_neighbor_index(self.neigh_table)

        self._ensure_gid_area(self.cfg.out_general, force_area=True)

    def _build_neighbor_index(self, table_path: str) -> dict[int,set[int]]:
        index: dict[int,set[int]] = {}
        fields = [f.name for f in arcpy.ListFields(table_path)]
        src_field = next((f for f in fields if 'src' in f.lower() and ('objectid' in f.lower() or 'fid' in f.lower())), None)
        nbr_field = next((f for f in fields if 'nbr' in f.lower() and ('objectid' in f.lower() or 'fid' in f.lower())), None)
        if not src_field or not nbr_field:
            self.log.warn(f"Could not find neighbor fields in {fields}")
            return index
        with arcpy.da.SearchCursor(table_path, [src_field, nbr_field]) as cur:
            for src, nbr in cur:
                if src == nbr: continue
                s = int(src); n = int(nbr)
                index.setdefault(s, set()).add(n)
                index.setdefault(n, set()).add(s)
        return index

    def _one_iteration_fast(self, val: int, boundary_lyr: str, pri_map: dict[str,int]):
        # small layer (AREA < val), edge kizárás
        small_lyr = "small_not_boundary_lyr"
        if arcpy.Exists(small_lyr): arcpy.management.Delete(small_lyr)
        arcpy.management.MakeFeatureLayer(self.cfg.out_general, small_lyr, where_clause=f"AREA < {val}")
        arcpy.management.SelectLayerByLocation(small_lyr, self.cfg.neighbor_mode, boundary_lyr, selection_type="NEW_SELECTION")
        arcpy.management.SelectLayerByLocation(small_lyr, self.cfg.neighbor_mode, boundary_lyr, selection_type="SWITCH_SELECTION")

        sel_count = int(arcpy.management.GetCount(small_lyr)[0]) if arcpy.Exists(small_lyr) else 0
        if sel_count == 0:
            self.log.iter(f"Iter({val}ha): selected=0 (skip)")
            return

        # teljes out_general adatok egyszerre
        poly_data: dict[int, tuple[str|None, float]] = {}
        with arcpy.da.SearchCursor(self.cfg.out_general, ["OBJECTID","NEWCODE","AREA"]) as c:
            for oid, code, area in c:
                poly_data[int(oid)] = (self._code_to_str(code), float(area or 0.0))

        # small OID-k
        small_oids: set[int] = set()
        with arcpy.da.SearchCursor(small_lyr, ["OID@"]) as c:
            for (oid,) in c:
                small_oids.add(int(oid))

        # frissítések gyűjtése
        updates: dict[int,int] = {}
        for oid in small_oids:
            small_code, _ = poly_data.get(oid, (None, 0.0))
            neighbors = self.neighbor_index.get(oid, set())
            if not neighbors:
                continue
            candidates = []
            for nb_oid in neighbors:
                nb_code, nb_area = poly_data.get(nb_oid, (None, 0.0))
                if not nb_code:
                    continue
                identical = (small_code is not None and nb_code == small_code)
                if identical:
                    real_pri = 0
                else:
                    nb_pri_single = pri_map.get(nb_code, 999999)
                    pair_code = (small_code or "") + nb_code if small_code is not None else nb_code
                    real_pri = pri_map.get(pair_code, nb_pri_single)
                candidates.append((real_pri, -nb_area, nb_code))
            if not candidates:
                continue
            candidates.sort()
            best_code = candidates[0][2]
            try:
                updates[oid] = int(best_code)
            except:
                pass

        # egy update cursor az összeshez
        updated = 0
        if updates:
            with arcpy.da.UpdateCursor(self.cfg.out_general, ["OBJECTID","NEWCODE"]) as ucur:
                for ro, rc in ucur:
                    oid = int(ro)
                    if oid in updates:
                        ucur.updateRow([ro, updates[oid]])
                        updated += 1

        self._ensure_gid_area(self.cfg.out_general, force_area=False)
        self.log.iter(f"Iter({val}ha): selected={sel_count}, updated={updated}")

    # ---------- supporting ----------
    def _global_dissolve_and_refresh(self):
        if arcpy.Exists(self.dissolv_tmp):
            arcpy.management.Delete(self.dissolv_tmp)
        # PairwiseDissolve ha lehet; különben klasszikus
        success = False
        try:
            arcpy.analysis.PairwiseDissolve(self.cfg.out_general, self.dissolv_tmp, "NEWCODE", None, "SINGLE_PART")
            success = True
        except:
            pass
        if not success:
            arcpy.management.Dissolve(self.cfg.out_general, self.dissolv_tmp, ["NEWCODE"], "", "SINGLE_PART", "DISSOLVE_LINES")

        if arcpy.Exists(self.cfg.out_general):
            arcpy.management.Delete(self.cfg.out_general)
        arcpy.management.CopyFeatures(self.dissolv_tmp, self.cfg.out_general)
        self._ensure_gid_area(self.cfg.out_general, force_area=True)

    def annotate(self):
        fc = self.cfg.out_general
        if "Comment" not in [f.name for f in arcpy.ListFields(fc)]:
            arcpy.management.AddField(fc, "Comment", "TEXT", field_length=255)

        fl = "fl_anno"
        if arcpy.Exists(fl): arcpy.management.Delete(fl)
        arcpy.management.MakeFeatureLayer(fc, fl)

        arcpy.management.SelectLayerByAttribute(fl, "NEW_SELECTION", "AREA < 25")
        arcpy.management.CalculateField(fl, "Comment", "'Smaller than MMU'", "PYTHON3")

        arcpy.management.SelectLayerByAttribute(fl, "NEW_SELECTION", "AREA < 25")
        arcpy.management.SelectLayerByLocation(fl, "BOUNDARY_TOUCHES", self.boundary_line, selection_type="SUBSET_SELECTION")
        arcpy.management.CalculateField(fl, "Comment", "'Edge polygon'", "PYTHON3")

        arcpy.management.SelectLayerByAttribute(fl, "CLEAR_SELECTION")
        arcpy.management.Delete(fl)

    @staticmethod
    def _resolve_field_name(table: str, wanted: str) -> str:
        wl=wanted.lower()
        for f in (arcpy.ListFields(table) or []):
            if f.name.lower()==wl: return f.name
        raise ValueError(f"Field '{wanted}' not found in {table}. Avail: {[f.name for f in arcpy.ListFields(table)]}")

    @staticmethod
    def _code_to_str(code_val) -> str | None:
        if code_val is None: return None
        try:
            if isinstance(code_val,(int,float)): return str(int(code_val))
            s = str(code_val).strip()
            return s if s != "" else None
        except:
            return None

    @staticmethod
    def _ensure_gid_area(fc: str, force_area: bool = False):
        names = [f.name for f in arcpy.ListFields(fc)]
        if "GID" not in names:
            arcpy.management.AddField(fc,"GID","DOUBLE")
            arcpy.management.CalculateField(fc,"GID","!OBJECTID!","PYTHON3")
        if force_area or "AREA" not in names:
            if "AREA" not in names:
                arcpy.management.AddField(fc,"AREA","DOUBLE")
            arcpy.management.CalculateField(fc,"AREA","!shape.area@HECTARES!","PYTHON3")

    @staticmethod
    def _extract_gdb_path(path: str) -> str:
        try:
            p = arcpy.Describe(path).path
            while p and not p.lower().endswith(".gdb"):
                p = arcpy.Describe(p).path
            return p or ""
        except Exception:
            low = path.lower()
            idx = low.rfind(".gdb")
            if idx == -1: return ""
            return path[:idx+4].rstrip("\\/")


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
    p0 = arcpy.GetParameterAsText(0)
    p1 = arcpy.GetParameterAsText(1)
    p2 = arcpy.GetParameterAsText(2)
    p3 = arcpy.GetParameterAsText(3)
    p4 = arcpy.GetParameter(4)
    p5 = arcpy.GetParameter(5)
    p6 = arcpy.GetParameter(6)

    cfg = Config(
        input_change=p0,
        input_revision=p1,
        out_general=p2,
        priority_table=p3,
        from_value=int(p4) if p4 not in (None,"") else 3,
        to_value=int(p5)   if p5 not in (None,"") else 23,
        by_value=int(p6)   if p6 not in (None,"") else 5,
        neighbor_mode="BOUNDARY_TOUCHES",
        keep_intermediates=False,
        memory_report=False,           # részletes memória-log NEM kell
    )

    result_fc = CorineGeneralizer(cfg).run()
    arcpy.SetParameterAsText(2, result_fc)


# -------------------- Entrypoint --------------------
if __name__ == "__main__":
    try:
        script_tool()
    except Exception:
        tb = traceback.format_exc()
        try: arcpy.AddError(tb)
        except Exception: print(tb)
        raise
