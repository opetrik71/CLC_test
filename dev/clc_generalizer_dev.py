# -*- coding: utf-8 -*-
from __future__ import annotations
import arcpy, os, sys, time, csv, traceback
from dataclasses import dataclass

# -------------------- Logger --------------------
class Logger:
    class Line:
        def __init__(self, title: str):
            self.title = title.rstrip(":")
            self.parts=[]
            sys.stdout.write(f"{self.title}: "); sys.stdout.flush()
        def add(self, piece: str):
            if self.parts: sys.stdout.write(", ")
            sys.stdout.write(piece); sys.stdout.flush(); self.parts.append(piece)
        def done(self):
            sys.stdout.write(" - Done\n"); sys.stdout.flush()
    def line(self, title: str) -> "Logger.Line":
        return Logger.Line(title)
    def msg(self, title: str, text: str = ""):
        print(f"{title}: {text}" if text else f"{title}:"); sys.stdout.flush()
    def iter(self, msg: str):
        print(f"  {msg}"); sys.stdout.flush()

# -------------------- Config --------------------
@dataclass
class Config:
    gdb_path: str
    # iterációk
    start_small_ha: int = 3
    to_value: int = 23
    by_value: int = 5
    neighbor_mode: str = "BOUNDARY_TOUCHES"  # vagy: SHARE_A_LINE_SEGMENT_WITH / INTERSECT
    # IO
    priority_table: str | None = None
    priority_code_field: str = "CODE"
    priority_pri_field: str = "PRI"
    input_change: str | None = None
    input_revision: str | None = None
    out_general: str | None = None
    # futás / debug
    keep_intermediates: bool = False
    snapshot_iter_outputs: bool = False
    iterator_test: bool = False   # True -> csak start_small_ha kör (CSV + Enter), majd kilép

# -------------------- Fő osztály --------------------
class CorineGeneralizer:
    def __init__(self, cfg: Config, logger: Logger | None = None):
        self.cfg = cfg
        self.log = logger or Logger()
        self.ws = cfg.gdb_path

        # temp path-ok
        self.change_copy   = os.path.join(self.ws, "CopyFeatures")
        self.revision_copy = os.path.join(self.ws, "rev_copy")
        self.diss_c        = os.path.join(self.ws, "diss_c")
        self.diss_r        = os.path.join(self.ws, "diss_r")
        self.union_cr      = os.path.join(self.ws, "union_cr")
        self.mtos_fc       = os.path.join(self.ws, "MToS")
        self.boundary_poly = os.path.join(self.ws, "diss_l")
        self.boundary_line = os.path.join(self.ws, "line")
        self.dissolv_tmp   = os.path.join(self.ws, "dissolv_new_code")
        # CSV-k helye: gdb mappája
        self.out_dir = os.path.dirname(self.ws.rstrip("\\/"))
        self._preiter_snapshot_done = True

    # ---------- public ----------
    def run(self) -> str:
        t0 = time.time()

        setup = self.log.line("Setup")
        self._auto_config(); setup.add("Auto-config")
        self._setup_env();   setup.add("Pro Environment")
        self._cleanup(False);setup.add("Cleanup remnants")
        setup.done()

        prep = self.log.line("Preparations")
        self._prepare(prep_line=prep); prep.done()
        # self._diag("Diagnostics", "after prepare", self.cfg.out_general)

        mode = "test" if self.cfg.iterator_test else "normal"
        self.log.msg("Iterator", f"running (mode={mode})")
        self._run_iterator(test_mode=self.cfg.iterator_test)
        # self._diag("Diagnostics", "after iterator", self.cfg.out_general)

        self.log.msg("Annotate", "final labels")
        self.annotate()

        if not self.cfg.keep_intermediates:
            final = self.log.line("Finalization")
            ld, fd = self._cleanup(False)
            final.add(f"cleanup layers ({ld})"); final.add(f"cleanup files ({fd})"); final.done()

        print(f"Result feature class: {self.cfg.out_general} - Done [{time.time()-t0:.2f}s]")
        return self.cfg.out_general  # type: ignore

    # ---------- lépések ----------
    def _auto_config(self):
        gdb = self.cfg.gdb_path; arcpy.env.workspace = gdb
        for fc in arcpy.ListFeatureClasses() or []:
            l = fc.lower()
            if "change" in l and not self.cfg.input_change:   self.cfg.input_change = f"{gdb}\\{fc}"
            if "revision" in l and not self.cfg.input_revision:self.cfg.input_revision= f"{gdb}\\{fc}"
        ident = os.path.basename(gdb).split(".")[0]
        self.cfg.out_general = f"{gdb}\\gener_{ident}"

        # priority table: gdb mellett először, fallback gdb-ben
        pri = None
        folder = os.path.dirname(gdb); cand = os.path.join(folder,"join_pri.dbf")
        if os.path.exists(cand): pri = cand
        else:
            for t in arcpy.ListTables() or []:
                if "pri" in t.lower(): pri = f"{gdb}\\{t}"; break
        if not pri: raise ValueError("No priority table (join_pri.dbf) found next to the .gdb or inside it.")
        self.cfg.priority_table = pri
        self.cfg.priority_code_field = self._resolve_field_name(pri, "CODE")
        self.cfg.priority_pri_field  = self._resolve_field_name(pri, "PRI")

        if not self.cfg.input_change or not self.cfg.input_revision:
            raise RuntimeError("Auto-config failed: change/revision not found in GDB.")

    def _setup_env(self):
        arcpy.env.workspace = self.ws
        arcpy.env.scratchWorkspace = self.ws
        arcpy.env.overwriteOutput = True
        arcpy.env.parallelProcessingFactor = "75%"
        arcpy.env.qualifiedFieldNames = False
        arcpy.env.addOutputsToMap = False

    def _cleanup(self, verbose: bool = False) -> tuple[int, int]:
        layers = [
            "filled_ga_l","select_sp_l","l_l","inside_area_l","boundary_touch_l",
            "Neighbour Areas Layer","fl_ai","all_polys_lyr","small_not_boundary_lyr","boundary_lyr",
            "__affected__","__diag","__census_all__","__census_lt__"
        ]
        files = [
            "CopyFeatures","rev_copy","diss_c","diss_r","union_cr","MToS","diss_l","line",
            "copySelected","neighbor_areas","neighbor_areas_Sort","out_raw","dissolv_new_code",
            "nighbor_areas","sort_table","first_neighbor","out_raw_Dissolve"
        ]
        ld, fd = 0, 0
        for l in layers:
            try:
                if arcpy.Exists(l): arcpy.Delete_management(l); ld += 1
            except Exception as e:
                if verbose: print(f"[cleanup] layer err {l}: {e}")
        for f in files:
            try:
                p = f"{self.ws}\\{f}"
                if arcpy.Exists(p): arcpy.Delete_management(p); fd += 1
            except Exception as e:
                if verbose: print(f"[cleanup] file err {f}: {e}")
        return ld, fd

    def _prepare(self, prep_line: Logger.Line | None = None):
        # Change
        arcpy.management.CopyFeatures(self.cfg.input_change, self.change_copy)
        if "NEWCODE" not in [f.name for f in arcpy.ListFields(self.change_copy)]:
            arcpy.management.AddField(self.change_copy,"NEWCODE","LONG")
        arcpy.management.CalculateField(
            self.change_copy,"NEWCODE","121 if !CHCODE! in (1211,1212) else !CHCODE!","PYTHON3"
        )
        if prep_line: prep_line.add("Change database")

        # Revision
        arcpy.management.CopyFeatures(self.cfg.input_revision, self.revision_copy)
        if "OLDCODE" not in [f.name for f in arcpy.ListFields(self.revision_copy)]:
            arcpy.management.AddField(self.revision_copy,"OLDCODE","LONG")
        arcpy.management.CalculateField(
            self.revision_copy,"OLDCODE","121 if !REVCODE! in (1211,1212) else !REVCODE!","PYTHON3"
        )
        if prep_line: prep_line.add("Revision database")

        # Union
        arcpy.management.Dissolve(self.change_copy,self.diss_c,["NEWCODE"],"","SINGLE_PART","DISSOLVE_LINES")
        arcpy.management.Dissolve(self.revision_copy,self.diss_r,["OLDCODE"],"","SINGLE_PART","DISSOLVE_LINES")
        arcpy.analysis.Union([[self.diss_r,""],[self.diss_c,""]], self.union_cr,"NO_FID","","GAPS")
        if prep_line: prep_line.add("Union")

        # MToS
        arcpy.management.MultipartToSinglepart(self.union_cr, self.mtos_fc)
        if prep_line: prep_line.add("MToS")

        # NEWCODE robust fill
        arcpy.management.CalculateField(
            self.mtos_fc,
            "NEWCODE",
            "!OLDCODE! if (!NEWCODE! is None or (isinstance(!NEWCODE!,str) and !NEWCODE!=='') or !NEWCODE! == 0) else !NEWCODE!",
            "PYTHON3"
        )
        with arcpy.da.UpdateCursor(self.mtos_fc,["NEWCODE","OLDCODE"]) as cur:
            for newc, oldc in cur:
                if (newc is None) or (newc == 0) or (isinstance(newc,str) and str(newc).strip()==""):
                    cur.updateRow([oldc,oldc])

        # Generalized output
        arcpy.management.Dissolve(self.mtos_fc, self.cfg.out_general, ["NEWCODE"], "","SINGLE_PART","DISSOLVE_LINES")
        if prep_line: prep_line.add("Generalized output")

        # Boundary line
        if "diss" not in [f.name for f in arcpy.ListFields(self.diss_r)]:
            arcpy.management.AddField(self.diss_r,"diss","LONG")
        arcpy.management.CalculateField(self.diss_r,"diss","1","PYTHON3")
        arcpy.management.Dissolve(self.diss_r, self.boundary_poly, ["diss"], "","MULTI_PART","DISSOLVE_LINES")
        arcpy.management.PolygonToLine(self.boundary_poly, self.boundary_line, "IDENTIFY_NEIGHBORS")
        if prep_line: prep_line.add("Boundary line")

        # AREA+GID
        self._ensure_gid_area(self.cfg.out_general)

    # ---------- ITERÁTOR ----------
    def _run_iterator(self, test_mode: bool = False):
        # prioritás-térkép STRING kulccsal
        code_f = self._resolve_field_name(self.cfg.priority_table, self.cfg.priority_code_field)
        pri_f  = self._resolve_field_name(self.cfg.priority_table, self.cfg.priority_pri_field)
        pri_map: dict[str,int] = {}
        with arcpy.da.SearchCursor(self.cfg.priority_table,[code_f,pri_f]) as cur:
            for code, pri in cur:
                if pri is None or code is None: continue
                key = self._code_to_str(code)
                try: pri_map[key] = int(pri)
                except: continue

        # all + boundary layer
        all_lyr, boundary_lyr = "all_polys_lyr", "boundary_lyr"
        if arcpy.Exists(all_lyr): arcpy.management.Delete(all_lyr)
        arcpy.management.MakeFeatureLayer(self.cfg.out_general, all_lyr)
        if arcpy.Exists(boundary_lyr): arcpy.management.Delete(boundary_lyr)
        arcpy.management.MakeFeatureLayer(self.boundary_line, boundary_lyr)

        # snapshot + census (változatlan a 59-eshez)
        if self.cfg.snapshot_iter_outputs:
            self._snapshot_before_iter()
        self._pre_iter_census(self.cfg.out_general, boundary_lyr)

        # futás
        if test_mode:
            val = int(self.cfg.start_small_ha)
            self._one_iteration(val, all_lyr, boundary_lyr, pri_map, test_mode=True)
            # teszt végén is globális Dissolve
            self._global_dissolve_and_refresh()
            self.log.msg("Iterator", "test mode finished (stopping after 1 pass)")
            return

        start = int(self.cfg.start_small_ha)
        stop  = int(self.cfg.to_value)
        step  = int(self.cfg.by_value)
        for val in (range(start, stop+1, step) if stop>=start else []):
            self._one_iteration(val, all_lyr, boundary_lyr, pri_map, test_mode=False)
            # MINDEN iteráció végén globális (normál) Dissolve
            self._global_dissolve_and_refresh()
            # a friss FC-ből újralétrehozzuk az all_lyr-t (biztos, ami biztos)
            if arcpy.Exists(all_lyr): arcpy.management.Delete(all_lyr)
            arcpy.management.MakeFeatureLayer(self.cfg.out_general, all_lyr)

        self._ensure_gid_area(self.cfg.out_general)

    def _one_iteration(self, val: int, all_lyr: str, boundary_lyr: str,
                       pri_map: dict[str,int], test_mode: bool):
        # 1) small layer (AREA < val)
        small_lyr = "small_not_boundary_lyr"
        if arcpy.Exists(small_lyr): arcpy.management.Delete(small_lyr)
        arcpy.management.MakeFeatureLayer(self.cfg.out_general, small_lyr, where_clause=f"AREA < {val}")

        # 2) edge kizárás small rétegen belül
        arcpy.management.SelectLayerByLocation(small_lyr, self.cfg.neighbor_mode, boundary_lyr, selection_type="NEW_SELECTION")
        arcpy.management.SelectLayerByLocation(small_lyr, self.cfg.neighbor_mode, boundary_lyr, selection_type="SWITCH_SELECTION")

        sel_count = int(arcpy.management.GetCount(small_lyr)[0]) if arcpy.Exists(small_lyr) else 0
        if sel_count == 0:
            self.log.iter(f"Iter({val}ha): selected=0 (skip)")
            return

        # CSV-k
        csv_all = os.path.join(self.out_dir, f"iterator_it{val:02d}.csv") if test_mode else None
        csv_sel = os.path.join(self.out_dir, f"selected_only_it{val:02d}.csv")
        if test_mode:
            with open(csv_all, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=';')
                w.writerow(["small_oid","small_code","small_area_ha",
                            "nb_rank","nb_code","nb_area_ha","nb_pri",
                            "pair_code","real_pri","chosen"])
        with open(csv_sel, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=';')
            w.writerow(["small_oid","small_code","small_area_ha",
                        "chosen_nb_code","chosen_nb_area_ha","chosen_pair_code",
                        "chosen_real_pri","choice_reason"])

        chosen_map = {}      # csak teszt dumphoz kell
        small_oids = [oid for (oid,) in arcpy.da.SearchCursor(small_lyr,["OID@"])]
        updated=0; pri_used = 0

        for oid in small_oids:
            tmp = f"inmem_small_{oid}"
            try:
                if arcpy.Exists(tmp): arcpy.management.Delete(tmp)
            except: pass
            arcpy.management.MakeFeatureLayer(self.cfg.out_general, tmp, where_clause=f"OBJECTID = {oid}")

            # small saját adatai
            small_code_val, small_area = None, None
            with arcpy.da.SearchCursor(tmp, ["NEWCODE","AREA"]) as sc:
                for ncode, narea in sc:
                    small_code_val, small_area = ncode, narea
            small_code = self._code_to_str(small_code_val)

            # ——— robosztus kiválasztás előtt töröljük az esetleges előző selection-t ———
            arcpy.management.SelectLayerByAttribute(all_lyr, "CLEAR_SELECTION")
            # szomszédok kiválasztása teljes állományból
            arcpy.management.SelectLayerByLocation(all_lyr, self.cfg.neighbor_mode, tmp, selection_type="NEW_SELECTION")

            neighbors_raw = []
            has_identical = False
            with arcpy.da.SearchCursor(all_lyr,["OBJECTID","NEWCODE","AREA"]) as nc:
                for nb_oid, ncode, narea in nc:
                    if nb_oid == oid:  # saját maga
                        continue
                    nb_code_str = self._code_to_str(ncode)
                    if nb_code_str is None: continue
                    nb_area = float(narea or 0.0)
                    identical = (small_code is not None and nb_code_str == small_code)
                    if identical: has_identical = True

                    nb_pri_single = pri_map.get(nb_code_str, 999999)
                    pair_code = (small_code or "") + nb_code_str if small_code is not None else nb_code_str
                    pair_pri = pri_map.get(pair_code, nb_pri_single)
                    real_pri = 0 if identical else pair_pri

                    neighbors_raw.append({
                        "nb_code": nb_code_str, "nb_area": nb_area,
                        "nb_pri_single": nb_pri_single,
                        "pair_code": pair_code, "real_pri": real_pri,
                        "identical": identical
                    })

            if not neighbors_raw:
                with open(csv_sel, "a", newline="", encoding="utf-8") as f:
                    w = csv.writer(f, delimiter=';')
                    w.writerow([oid, small_code, f"{(small_area or 0.0):.4f}",
                                "", "", "", "", "no_neighbors"])
                # ideiglenes layer takarítás
                try:
                    if arcpy.Exists(tmp): arcpy.management.Delete(tmp)
                except: pass
                continue

            neighbors = sorted(neighbors_raw, key=lambda d: (d["real_pri"], -d["nb_area"]))
            best = neighbors[0]
            choice_reason = "identical" if best["identical"] else "priority"
            if not has_identical: pri_used += 1

            # tesztmód: jelöltek
            if test_mode:
                with open(csv_all, "a", newline="", encoding="utf-8") as f:
                    w = csv.writer(f, delimiter=';')
                    for i, d in enumerate(neighbors, start=1):
                        w.writerow([
                            oid, small_code, f"{(small_area or 0.0):.4f}",
                            i, d["nb_code"], f"{d['nb_area']:.4f}", d["nb_pri_single"],
                            d["pair_code"], d["real_pri"], "YES" if d is best else ""
                        ])
                chosen_map[oid] = (small_code, best["nb_code"], float(small_area or 0.0))

            # csak kiválasztott sor a “selected_only” CSV-be
            with open(csv_sel, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=';')
                w.writerow([
                    oid, small_code, f"{(small_area or 0.0):.4f}",
                    best["nb_code"], f"{best['nb_area']:.4f}", best["pair_code"], best["real_pri"],
                    choice_reason
                ])

            # frissítés
            if best["nb_code"] is not None:
                with arcpy.da.UpdateCursor(self.cfg.out_general,["OBJECTID","NEWCODE"], where_clause=f"OBJECTID = {oid}") as ucur:
                    for ro, rc in ucur:
                        try: ucur.updateRow([ro, int(best["nb_code"])])
                        except:
                            try: ucur.updateRow([ro, int(str(best["nb_code"]))])
                            except: pass
                        updated += 1

            try:
                if arcpy.Exists(tmp): arcpy.management.Delete(tmp)
            except: pass

        self._ensure_gid_area(self.cfg.out_general)
        self.log.iter(f"Iter({val}ha): selected={sel_count}, updated={updated}, pri_used={pri_used}")

        if test_mode:
            try: input(f"[Pause] Press Enter to finish test mode after iter({val}ha)...")
            except EOFError: pass
            return

    # ---------- supporting ----------
    def _snapshot_before_iter(self):
        # ha már volt, vagy ki van kapcsolva: semmit ne csináljon
        if self._preiter_snapshot_done or not self.cfg.snapshot_iter_outputs:
            return
        try:
            base = arcpy.Describe(self.cfg.out_general).baseName
            stamp = time.strftime("%Y%m%d_%H%M%S")
            snap_fc = os.path.join(self.ws, f"{base}_before_iter_{stamp}")  # mindig új név
            arcpy.management.CopyFeatures(self.cfg.out_general, snap_fc)
            self.log.msg("Snapshot", f"Saved baseline: {snap_fc}")
            self._preiter_snapshot_done = True
        except Exception as e:
            self.log.msg("Snapshot", f"Skipped (reason: {e})")

    def _pre_iter_census(self, fc: str, boundary_lyr: str):
        lyr_all = "__census_all__"
        if arcpy.Exists(lyr_all): arcpy.management.Delete(lyr_all)
        arcpy.management.MakeFeatureLayer(fc, lyr_all)
        total = int(arcpy.management.GetCount(lyr_all)[0])

        arcpy.management.SelectLayerByLocation(lyr_all, self.cfg.neighbor_mode, boundary_lyr, selection_type="NEW_SELECTION")
        arcpy.management.SelectLayerByLocation(lyr_all, self.cfg.neighbor_mode, boundary_lyr, selection_type="SWITCH_SELECTION")
        non_edge = int(arcpy.management.GetCount(lyr_all)[0])
        arcpy.management.Delete(lyr_all)

        lyr_lt = "__census_lt__"
        if arcpy.Exists(lyr_lt): arcpy.management.Delete(lyr_lt)
        arcpy.management.MakeFeatureLayer(fc, lyr_lt, where_clause=f"AREA < {self.cfg.start_small_ha}")
        ltX = int(arcpy.management.GetCount(lyr_lt)[0])

        arcpy.management.SelectLayerByLocation(lyr_lt, self.cfg.neighbor_mode, boundary_lyr, selection_type="NEW_SELECTION")
        arcpy.management.SelectLayerByLocation(lyr_lt, self.cfg.neighbor_mode, boundary_lyr, selection_type="SWITCH_SELECTION")
        ltX_non_edge = int(arcpy.management.GetCount(lyr_lt)[0])
        arcpy.management.Delete(lyr_lt)

        print(f"[census] total={total}, non_edge={non_edge}, lt{self.cfg.start_small_ha}ha={ltX}, lt{self.cfg.start_small_ha}ha_non_edge={ltX_non_edge}")

    def annotate(self):
        """Add 'Comment' field only once, at the end.
           <25 ha → 'Smaller than MMU'; ugyanitt határérintő → 'Edge polygon'; ≥25 ha: üres"""
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
        arcpy.management.SelectLayerByLocation(fl, "BOUNDARY_TOUCHES", self.boundary_line, selection_type="SUBSET_SELECTION")
        arcpy.management.CalculateField(fl, "Comment", "'Edge polygon'", "PYTHON3")

        arcpy.management.SelectLayerByAttribute(fl, "CLEAR_SELECTION")
        arcpy.management.Delete(fl)

    def _global_dissolve_and_refresh(self):
        """NEWCODE szerinti TELJES összeolvasztás, majd overwrite az out_general-ra."""
        if arcpy.Exists(self.dissolv_tmp):
            arcpy.management.Delete(self.dissolv_tmp)
        arcpy.management.Dissolve(
            self.cfg.out_general,
            self.dissolv_tmp,
            ["NEWCODE"], "", "SINGLE_PART", "DISSOLVE_LINES"
        )
        if arcpy.Exists(self.cfg.out_general):
            arcpy.management.Delete(self.cfg.out_general)
        arcpy.management.CopyFeatures(self.dissolv_tmp, self.cfg.out_general)
        self._ensure_gid_area(self.cfg.out_general)

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
            if isinstance(code_val, (int, float)): return str(int(code_val))
            return str(code_val).strip()
        except: return None

    @staticmethod
    def _ensure_gid_area(fc: str):
        names=[f.name for f in arcpy.ListFields(fc)]
        if "GID" not in names: arcpy.management.AddField(fc,"GID","DOUBLE")
        arcpy.management.CalculateField(fc,"GID","!OBJECTID!","PYTHON3")
        if "AREA" not in names: arcpy.management.AddField(fc,"AREA","DOUBLE")
        arcpy.management.CalculateField(fc,"AREA","!shape.area@HECTARES!","PYTHON3")

# -------------------- Entrypoint --------------------
def main():
    try:
        cfg = Config(
            gdb_path=r"D:\munka\CLC2024\clc_gener\73.gdb",
            start_small_ha=3, to_value=23, by_value=5,
            neighbor_mode="BOUNDARY_TOUCHES",
            keep_intermediates=False,           # vizsgálathoz True
            snapshot_iter_outputs=False,
            iterator_test=False                # True: csak 1 kör (start_small_ha)
        )
        CorineGeneralizer(cfg).run()
    except Exception:
        print("ERROR during processing:"); traceback.print_exc(); raise

if __name__ == "__main__":
    main()
