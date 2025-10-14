# -*- coding: utf-8 -*-
"""
CORINE Land Cover Generalization Tool
=====================================

This module implements an iterative polygon generalization algorithm for CORINE Land Cover
datasets. It processes large polygon datasets by progressively merging small polygons with
their neighbors based on area thresholds and priority rules.

Main Components:
    - Logger: GP-pane friendly logging with streaming output
    - MemoryTracker: Optional memory monitoring with psutil
    - Config: Configuration dataclass for all parameters
    - CorineGeneralizer: Main processing class

Typical Usage:
    >>> cfg = Config(
    ...     input_change="path/to/gdb/change",
    ...     input_revision="path/to/gdb/revision",
    ...     out_general="path/to/gdb/generalized",
    ...     priority_table="path/to/join_pri.dbf"
    ... )
    >>> generalizer = CorineGeneralizer(cfg)
    >>> result = generalizer.run()

Author: Ottó Petrik
Version: 1.0
Date: 2025-10-13
"""
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
    """
    ArcGIS Geoprocessing pane-friendly logger with streaming output capability.

    This logger provides real-time feedback in the ArcGIS GP pane by using arcpy.AddMessage,
    arcpy.AddWarning, and arcpy.AddError. Falls back to print() if arcpy is unavailable.

    Features:
        - Immediate output (no buffering)
        - Structured messages with consistent formatting
        - Streaming blocks for multi-step operations
        - Automatic fallback for non-ArcGIS environments

    Example:
        >>> log = Logger()
        >>> log.msg("Processing", "started")
        >>>
        >>> stream = log.stream("Data Preparation")
        >>> stream.step("Loading inputs")
        >>> stream.step("Validating geometry")
        >>> stream.done()
        >>>
        >>> log.warn("Memory usage approaching limit")
        >>> log.error("Invalid input detected")
    """
    @staticmethod
    def _gp_msg(txt: str):
        """Send message to GP pane or console."""
        try: arcpy.AddMessage(txt)
        except Exception: print(txt)

    @staticmethod
    def _gp_warn(txt: str):
        """Send warning to GP pane or console."""
        try: arcpy.AddWarning(txt)
        except Exception: print("[WARN] " + txt)

    @staticmethod
    def _gp_err(txt: str):
        """Send error to GP pane or console."""
        try: arcpy.AddError(txt)
        except Exception: print("[ERROR] " + txt)

    # Egylépéses üzenetek
    def msg(self, title: str, text: str = ""):
        """
        Log a standard message.

        Args:
            title: Message title/category
            text: Optional message details

        Example:
            >>> log.msg("Validation", "complete")
            # Output: "Validation: complete"
        """
        self._gp_msg(f"{title}: {text}" if text else f"{title}:")

    def iter(self, msg: str):
        """
        Log an indented iteration message.

        Args:
            msg: Iteration details

        Example:
            >>> log.iter("Iter(3ha): selected=9326, updated=9326")
            # Output: "  Iter(3ha): selected=9326, updated=9326"
        """
        self._gp_msg(f"  {msg}")

    def warn(self, msg: str):
        """
        Log a warning message.

        Args:
            msg: Warning text
        """
        self._gp_warn(msg)

    def error(self, msg: str):
        """
        Log an error message.

        Args:
            msg: Error text
        """
        self._gp_err(msg)

    # Streamelt blokkok (azonnali, soronkénti kiírás)
    class Stream:
        """
        Streaming output block for multi-step operations.

        Provides structured output for operations with multiple sub-steps,
        displaying each step immediately as it completes.

        Attributes:
            title: Block title
            _done: Flag to prevent duplicate "Done" messages

        Example:
            >>> stream = log.stream("Processing")
            >>> stream.step("Load data").step("Transform").step("Save")
            >>> stream.done()
            # Output:
            # Processing:
            #   - Load data
            #   - Transform
            #   - Save
            # Processing: Done
        """
        def __init__(self, title: str):
            """
            Initialize streaming block.

            Args:
                title: Block title (colon added automatically)
            """
            self.title = title.rstrip(":")
            Logger._gp_msg(f"{self.title}: ")
            self._done = False
        def step(self, piece: str):
            """
            Log a step in the streaming block.

            Args:
                piece: Step description

            Returns:
                Self for method chaining
            """
            Logger._gp_msg(f"  - {piece}")
            return self
        def done(self):
            """Mark the streaming block as complete."""
            if not self._done:
                Logger._gp_msg(f"{self.title}: Done")
                self._done = True

    def stream(self, title: str) -> "Logger.Stream":
        """
        Create a streaming output block.

        Args:
            title: Block title

        Returns:
            Stream object for chaining steps
        """
        return Logger.Stream(title)

# -------------------- Memóriamérő (csak ha psutil elérhető) --------------------
class MemoryTracker:
    """
    Optional memory monitoring utility using psutil.

    Tracks memory consumption throughout the processing workflow and provides
    summary statistics. Only active if psutil is installed and enabled=True.

    Attributes:
        log: Logger instance for output
        enabled: Whether tracking is active
        process: psutil Process object (if enabled)
        system_total_gb: Total system RAM in GB
        system_available_gb: Available RAM at start
        start_mb: Process memory at initialization
        peak_mb: Peak memory usage observed

    Example:
        >>> mem = MemoryTracker(logger, enabled=True)
        >>> mem.report("after loading data")
        >>> # ... processing ...
        >>> mem.final_summary()
        # Output:
        # Memory Summary:
        #   Peak usage: 1864 MB
        #   Total consumed: 921 MB
    """
    def __init__(self, logger, enabled: bool):
        """
        Initialize memory tracker.

        Args:
            logger: Logger instance for output
            enabled: Enable tracking (requires psutil)
        """
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
        """
        Report current memory usage and update peak.

        Args:
            *_args: Ignored (for compatibility)
            **_kwargs: Ignored (for compatibility)

        Note:
            Only outputs if enabled=True. Triggers garbage collection
            before measuring to get accurate readings.
        """
        if not self.enabled:
            return
        gc.collect()
        mem = self.process.memory_info().rss / (1024**2)
        self.peak_mb = max(self.peak_mb, mem)

    def final_summary(self):
        """
        Output final memory consumption summary.

        Displays peak usage and total memory consumed during processing.
        Only outputs if enabled=True.
        """
        if not self.enabled:
            return
        self.log.msg("Memory Summary:")
        self.log.iter(f"Peak usage: {self.peak_mb:.0f} MB")
        self.log.iter(f"Total consumed: {self.peak_mb - self.start_mb:.0f} MB")

# -------------------- Config --------------------
@dataclass
class Config:
    """
    Configuration parameters for CORINE generalization.

    This dataclass holds all input/output paths and processing parameters.
    Required parameters must be provided; optional parameters have defaults.

    Required Attributes:
        input_change: Full path to change polygon feature class
        input_revision: Full path to revision polygon feature class
        out_general: Full path to output generalized feature class
        priority_table: Full path to priority lookup table (DBF)

    Optional Attributes:
        priority_code_field: Field name for code in priority table (default: "CODE")
        priority_pri_field: Field name for priority value (default: "PRI")
        from_value: Starting area threshold in hectares (default: 3)
        to_value: Ending area threshold in hectares (default: 23)
        by_value: Step size for thresholds (default: 5)
        neighbor_mode: Spatial relationship for neighbor detection (default: "BOUNDARY_TOUCHES")
        keep_intermediates: Retain intermediate datasets (default: False)
        memory_report: Enable detailed memory logging (default: False)

    Example:
        >>> cfg = Config(
        ...     input_change="C:/data/change.shp",
        ...     input_revision="C:/data/revision.shp",
        ...     out_general="C:/output.gdb/generalized",
        ...     priority_table="C:/lookup/join_pri.dbf",
        ...     from_value=5,
        ...     to_value=25,
        ...     by_value=10
        ... )

    Notes:
        - All paths should be full absolute paths
        - Output geodatabase must exist (feature class will be created)
        - Priority table format: two columns mapping codes/pairs to priorities
        neighbor_mode: Spatial relationship for boundary exclusion, e.g.
            "BOUNDARY_TOUCHES" (default), "SHARE_A_LINE_SEGMENT_WITH", "INTERSECT"
    """

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
    """
    CORINE Land Cover generalization using iterative neighbor-based merging.

    This class implements a multi-iteration polygon generalization algorithm that:
    1. Identifies small polygons below area thresholds
    2. Merges them with neighbors based on priority rules
    3. Progressively increases thresholds to reduce polygon count
    4. Preserves boundary polygons (edge features)

    Algorithm Overview:
        For each threshold (e.g., 3ha, 8ha, 13ha, 18ha, 23ha):
          - Select polygons where AREA < threshold AND not touching boundary
          - For each small polygon:
            * Find all neighbors using pre-built spatial index
            * Score neighbors by priority:
              - Identical code: priority = 0 (highest)
              - Pair code in table: use pair priority
              - Single code in table: use single code priority
              - Not in table: default priority = 999999 (lowest)
            * Break ties by largest neighbor area
            * Assign NEWCODE from best-scoring neighbor
          - Dissolve all polygons by NEWCODE
          - Rebuild neighbor relationships for next iteration

    Memory Management:
        - Estimates capacity: (available_GB * 0.7) / 45MB per 1k polygons
        - Uses in-memory workspace for temporary datasets
        - Tracks peak memory usage if psutil available
        - Warns if input size approaches estimated capacity

    Performance Characteristics:
        - Typical runtime: 10-15 minutes for 50k polygons
        - Memory scaling: ~40-50 MB per 1000 polygons
        - Polygon reduction: typically 20-30% fewer polygons
        - Bottlenecks: Dissolve operations, neighbor index building

    Attributes:
        cfg: Configuration object with all parameters
        log: Logger instance for GP-pane output
        mem: Memory tracker (optional, based on cfg.memory_report)
        ws: Workspace path (geodatabase) extracted from out_general
        neighbor_index: Dict mapping ObjectID -> set of neighbor ObjectIDs

        # Temporary in-memory datasets
        change_copy: "memory/CopyFeatures"
        revision_copy: "memory/rev_copy"
        diss_c: "memory/diss_c"
        diss_r: "memory/diss_r"
        union_cr: "memory/union_cr"
        mtos_fc: "memory/MToS"
        dissolv_tmp: "memory/dissolv_new_code"

        # Persistent GDB datasets
        boundary_poly: "diss_l"
        boundary_line: "line"
        neigh_table: "all_neighbors"

    Example:
        >>> cfg = Config(
        ...     input_change="C:/data/change.shp",
        ...     input_revision="C:/data/revision.shp",
        ...     out_general="C:/output.gdb/result",
        ...     priority_table="C:/lookup/join_pri.dbf"
        ... )
        >>> generalizer = CorineGeneralizer(cfg)
        >>> output_fc = generalizer.run()
        >>> print(f"Result: {output_fc}")

    Raises:
        ValueError: If out_general doesn't point to a .gdb workspace
        FileNotFoundError: If any required input doesn't exist
        RuntimeError: If processing fails at any stage

    Notes:
    - All temporary data uses "memory" workspace for performance
    - Neighbor relationships rebuilt after each iteration
    - Boundary polygons (touching revision extent) never merged
    - Output includes AREA, GID, NEWCODE, and Comment fields
      (GID mirrors the current OBJECTID after each dissolve; it is not a persistent original ID)

    """
    def __init__(self, cfg: Config, logger: Logger | None = None):
        """
        Initialize the generalizer with configuration.

        Args:
            cfg: Configuration object with all parameters
            logger: Optional logger instance (creates new if None)

        Raises:
            ValueError: If workspace cannot be determined from out_general path
        """
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
        """
        Execute the complete generalization workflow.

        This is the main entry point that orchestrates all processing steps:
        1. Print capacity estimate and input statistics
        2. Validate inputs and setup environment
        3. Prepare data (copy, dissolve, union, multipart-to-singlepart)
        4. Run iterative generalization
        5. Annotate results with comments
        6. Cleanup temporary data
        7. Report final statistics

        Returns:
            Path to output feature class

        Raises:
            FileNotFoundError: If inputs don't exist
            RuntimeError: If any processing step fails

        Side Effects:
            - Creates/overwrites output feature class
            - Creates temporary datasets in memory and GDB
            - Modifies ArcGIS environment settings
            - Outputs progress messages to GP pane

        Example:
            >>> generalizer = CorineGeneralizer(cfg)
            >>> result = generalizer.run()
            # Output:
            # System Memory: 63.9 GB total, 47.5 GB available
            # Estimated capacity: 1,424,622 polygons
            # Input databases: 42,202 revision + 11,297 change = 53,499 polygons
            # Setup:
            #   - Input inspections
            #   - Pro Environment
            #   - Cleanup remnants
            # Setup: Done
            # ...
            # Result feature class: C:/output.gdb/result - Done [605.48s]
        """
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
        """
                Display memory capacity estimate and input polygon counts.

                Outputs:
                1. System memory (total and available)
                2. Estimated maximum polygon capacity based on available RAM
                3. Input polygon counts (revision + change = total)
                4. Warning if inputs approach capacity threshold (>80%)

                Capacity Calculation:
                    safe_capacity = min(32 GB, 70% of total RAM)
                    max_polygons = (safe_capacity * 1024 MB) / (45 MB per 1k polys) * 1000

                The 45 MB/1k empirical constant is based on observed memory consumption
                during dissolve operations (worst case scenario).

                Example Output:
                    System Memory: 63.9 GB total, 47.5 GB available
                    Estimated capacity (max polys): 746,705
                    Input databases: 42,202 revision + 11,297 change = 53,499 polygons

                Notes:
                    - Only calculates capacity if psutil is available
                    - Uses conservative 70% threshold to avoid system instability
                    - The capacity estimate is empirical and intentionally conservative
                """
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
        """
        Validate all input paths and parameters.

        Checks:
        1. Existence of input feature classes and priority table
        2. Field name resolution (case-insensitive matching)
        3. Iteration parameter validity (non-zero, reasonable ranges)

        Raises:
            FileNotFoundError: If any required input doesn't exist
            ValueError: If field names cannot be resolved

        Side Effects:
            - Resolves field names to actual case in priority table
            - Sets default values for empty iteration parameters
            - Logs warnings for missing/invalid parameters
        """
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
        """
        Configure ArcGIS environment settings for optimal performance.

        Settings:
        - workspace: Set to output geodatabase
        - scratchWorkspace: Same as workspace
        - overwriteOutput: True (allows overwriting existing data)
        - parallelProcessingFactor: "100%" (use all available cores)
        - qualifiedFieldNames: False (simpler field references)
        - addOutputsToMap: False (don't clutter map with intermediates)

        Notes:
            These settings persist for the Python session and affect all
            subsequent ArcPy operations.
        """
        arcpy.env.workspace = self.ws
        arcpy.env.scratchWorkspace = self.ws
        arcpy.env.overwriteOutput = True
        arcpy.env.parallelProcessingFactor = "100%"
        arcpy.env.qualifiedFieldNames = False
        arcpy.env.addOutputsToMap = False

    def _cleanup(self, verbose: bool = False) -> tuple[int, int]:
        """
        Remove temporary layers and datasets.

        Deletes three categories of temporary data:
        1. Feature layers (in-memory layer objects)
        2. Memory workspace features
        3. Geodatabase files (except final output)

        Args:
            verbose: If True, log warnings for failed deletions

        Returns:
            Tuple of (layers_deleted, files_deleted)

        Example:
            >>> ld, fd = self._cleanup(verbose=True)
            >>> print(f"Cleaned {ld} layers and {fd} files")
        """
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

    # ---------- Preparation ----------
    def _prepare(self, prep_stream: Logger.Stream | None = None):
        """
        Prepare input data for generalization processing.

        This method performs the following steps:
        1. Copy and normalize change dataset (CHCODE → NEWCODE, merge 1211/1212 → 121)
        2. Copy and normalize revision dataset (REVCODE → OLDCODE, merge 1211/1212 → 121)
        3. Dissolve both datasets by normalized codes
        4. Union change and revision layers
        5. Convert multipart to singlepart geometries
        6. Ensure NEWCODE field is populated (fallback to OLDCODE if needed)
        7. Create initial generalized output by dissolving union
        8. Generate boundary line from revision extent
        9. Add AREA and GID fields to output

        Args:
            prep_stream: Optional streaming logger for step-by-step output

        Side Effects:
            - Creates multiple in-memory temporary datasets
            - Creates boundary_poly and boundary_line in GDB
            - Creates/overwrites out_general feature class
            - Adds/updates NEWCODE, OLDCODE, AREA, GID fields

        Notes:
            - Uses PairwiseDissolve if available (ArcGIS Pro), falls back to classic Dissolve
            - NEWCODE logic: Use OLDCODE if NEWCODE is None, empty string, or 0
            - Boundary line identifies revision dataset extent (edge features)
            - All geometries converted to singlepart for accurate area calculations

        Example Data Flow:
            input_change (CHCODE) → change_copy (NEWCODE) → diss_c
                                                              ↓
            input_revision (REVCODE) → revision_copy (OLDCODE) → diss_r
                                                              ↓
                                                          union_cr
                                                              ↓
                                                           mtos_fc
                                                              ↓
                                                        out_general
        """
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
        """
        Execute the main iterative generalization loop.

        This is the core algorithm that progressively merges small polygons
        with their neighbors across multiple threshold iterations.

        Process:
        1. Build priority map from priority_table (code → priority value)
        2. Build initial neighbor spatial index (one-time PolygonNeighbors)
        3. Create boundary layer for edge detection
        4. For each threshold (from_value to to_value by by_value):
           a. Run one iteration (_one_iteration_fast)
           b. Dissolve all polygons by NEWCODE
           c. Rebuild neighbor index for next iteration
        5. Final AREA/GID field update

        Priority Map Structure:
            - Single codes: "121" → priority (e.g., 5)
            - Pair codes: "121211" → priority (e.g., 3)
            - Missing codes default to 999999 (lowest priority)

        Neighbor Index Structure:
            Dict[ObjectID, Set[ObjectID]]
            Example: {1: {2, 5, 7}, 2: {1, 3}, ...}

        Performance Notes:
            - Neighbor index is O(n) to build but O(1) for lookups
            - Rebuilding after each iteration handles topology changes
            - Memory usage peaks during dissolve operations
            - Typical iteration takes 30-90 seconds for 50k polygons

        Example Output:
            Iterator: running
            Generating neighbor relationships: (one-time computation)
              Iter(3ha): selected=9326, updated=9326
              Iter(8ha): selected=3930, updated=3930
              Iter(13ha): selected=2816, updated=2816
              Iter(18ha): selected=1737, updated=1737
              Iter(23ha): selected=1352, updated=1352
        """
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
        """
        Build in-memory neighbor relationship index from PolygonNeighbors table.

        Converts the neighbor table into a bidirectional dictionary for O(1) lookups
        during iteration processing. This significantly improves performance vs.
        repeated spatial queries.

        Args:
            table_path: Path to PolygonNeighbors output table

        Returns:
            Dictionary mapping ObjectID → set of neighbor ObjectIDs
            Relationships are bidirectional (if A neighbors B, then B neighbors A)

        Table Structure (expected):
            - src_*OBJECTID or src_*FID: Source polygon ID
            - nbr_*OBJECTID or nbr_*FID: Neighbor polygon ID
            - LENGTH: Shared boundary length (not used)
            - NODE_COUNT: Number of shared nodes (not used)

        Example:
            Input table:
                src_OBJECTID | nbr_OBJECTID
                -------------|-------------
                1            | 2
                1            | 3
                2            | 1
                2            | 4

            Output index:
                {
                    1: {2, 3},
                    2: {1, 4},
                    3: {1},
                    4: {2}
                }

        Performance:
            - Time complexity: O(n) where n is number of relationships
            - Space complexity: O(n) for storing bidirectional relationships
            - Typical size: 4-8 neighbors per polygon × polygon count

        Notes:
            - Self-neighbors (src == nbr) are excluded
            - Field names detected automatically (case-insensitive)
            - Empty table returns empty dict (logged as warning)
        """
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
        """
        Execute one generalization iteration at given area threshold.

        This is the core merging algorithm that:
        1. Selects small polygons (AREA < val) not touching boundary
        2. For each small polygon, finds best neighbor based on priority
        3. Updates NEWCODE to merge with best neighbor
        4. Recalculates AREA field

        Selection Criteria:
            - AREA < val (in hectares)
            - Does NOT touch boundary_line (BOUNDARY_TOUCHES with SWITCH_SELECTION)

        Neighbor Scoring (lower = better):
            1. Identical NEWCODE: priority = 0 (always best)
            2. Pair code in pri_map: use pair priority (e.g., "121211" → 3)
            3. Single neighbor code in pri_map: use single priority (e.g., "211" → 5)
            4. Code not in pri_map: default priority = 999999 (worst)

        Tie-Breaking:
            If multiple neighbors have same priority, choose largest area (negative sort)

        Args:
            val: Area threshold in hectares
            boundary_lyr: Feature layer of boundary lines
            pri_map: Dictionary of code/pair strings → priority integers

        Returns:
            None (updates self.cfg.out_general in-place)

        Side Effects:
            - Updates NEWCODE field for selected polygons
            - Does NOT recalculate AREA here; AREA is recalculated in the global dissolve step
            - Logs iteration statistics

        Algorithm Detail:
            1. Create layer of polygons < threshold
            2. Exclude boundary-touching polygons (switch selection)
            3. Load all polygon data (OID, NEWCODE, AREA) into memory dict
            4. Extract small polygon OIDs from selection
            5. For each small polygon:
               a. Get neighbor OIDs from pre-built index
               b. Score each neighbor by priority rules
               c. Sort by (priority, -area) and take best
               d. Record NEWCODE update
            6. Apply all updates in single UpdateCursor pass
            7. Recalculate AREA field

        Performance:
            - Time: O(n × k) where n = small polys, k = avg neighbors (typically 4-6)
            - Memory: O(n) for polygon data dict + O(m) for updates dict
            - Batch update much faster than row-by-row

        Example:
            Small polygon OID=100, NEWCODE="121", AREA=2.5 ha
            Neighbors:
              - OID=101, NEWCODE="121", AREA=50 ha → score=(0, -50)  [identical]
              - OID=102, NEWCODE="211", AREA=30 ha → score=(5, -30)  [pri_map["211"]=5]
              - OID=103, NEWCODE="999", AREA=40 ha → score=(999999, -40)  [not in map]

            Best neighbor: OID=101 (score 0)
            Update: polygon 100 NEWCODE = "121"
        """
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
        """
        Dissolve all polygons by NEWCODE and refresh output feature class.

        This is called after each iteration to:
        1. Merge adjacent polygons with identical NEWCODE
        2. Convert any multipart geometries to singlepart
        3. Update AREA and GID fields

        Process:
            1. Dissolve out_general → dissolv_tmp (by NEWCODE, SINGLE_PART)
            2. Delete old out_general
            3. Copy dissolv_tmp → out_general
            4. Recalculate AREA (force_area=True)

        Performance:
            - Dissolve is the slowest operation (60-70% of iteration time)
            - Uses PairwiseDissolve if available (faster for large datasets)
            - Falls back to classic Dissolve if Pairwise fails

        Side Effects:
            - Overwrites out_general feature class
            - Resets ObjectIDs (neighbor index must be rebuilt after this)
            - Updates AREA field to reflect new merged geometries

        Notes:
            - SINGLE_PART ensures single-part output directly; no extra MultipartToSinglepart needed here
            - AREA is recalculated in this step to reflect merged geometries
            - After replacing the dataset, GID is recomputed as GID = OBJECTID (mirrors current OIDs)
        """
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
        """
        Add descriptive comments to output polygons.

        Annotates polygons in the Comment field based on characteristics:
        - "Smaller than MMU": Polygons < 25 hectares
        - "Edge polygon": Polygons < 25 ha AND touching boundary line

        This helps identify polygons that don't meet minimum mapping unit
        requirements and distinguishes edge effects from true small features.

        Side Effects:
            - Adds "Comment" field if not present
            - Updates Comment values for qualifying polygons

        Example:
            After annotation, query polygons with:
            >>> arcpy.Select_analysis(
            ...     out_fc,
            ...     "small_features",
            ...     "Comment = 'Smaller than MMU'"
            ... )
        """
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
        """
        Resolve field name case-insensitively.

        ArcGIS field names are case-insensitive but case-preserving. This
        function finds the actual field name matching the wanted name.

        Args:
            table: Path to table/feature class
            wanted: Desired field name (any case)

        Returns:
            Actual field name as it exists in the table

        Raises:
            ValueError: If field not found

        Example:
            >>> _resolve_field_name("data.gdb/fc", "newcode")
            "NEWCODE"  # Found as uppercase in table
        """
        wl=wanted.lower()
        for f in (arcpy.ListFields(table) or []):
            if f.name.lower()==wl: return f.name
        raise ValueError(f"Field '{wanted}' not found in {table}. Avail: {[f.name for f in arcpy.ListFields(table)]}")

    @staticmethod
    def _code_to_str(code_val) -> str | None:
        """
        Convert code value to standardized string format.

        Handles various input types (int, float, str, None) and converts
        to consistent string representation for dictionary lookups.

        Args:
            code_val: Code value (any type)

        Returns:
            String representation of code, or None if invalid

        Examples:
            >>> _code_to_str(121)
            "121"
            >>> _code_to_str(121.0)
            "121"
            >>> _code_to_str("121")
            "121"
            >>> _code_to_str(None)
            None
            >>> _code_to_str("")
            None
        """
        if code_val is None: return None
        try:
            if isinstance(code_val,(int,float)): return str(int(code_val))
            s = str(code_val).strip()
            return s if s != "" else None
        except:
            return None

    @staticmethod
    def _ensure_gid_area(fc: str, force_area: bool = False):
        """
        Ensure GID and AREA fields exist and are populated.

        GID (Global ID): Preserves original ObjectID for tracking
        AREA: Polygon area in hectares (recalculated from geometry)

        Args:
            fc: Feature class path
            force_area: If True, always recalculate AREA (even if field exists)

        Side Effects:
            - Adds GID field if missing (set to current ObjectID)
            - Adds AREA field if missing
            - Recalculates AREA if force_area=True

        Performance:
            - CalculateField uses shape.area@HECTARES (fast geometry accessor)
            - Only recalculates when necessary to save time

        Notes:
            - GID is set once and never updated (preserves original ID)
            - AREA must be recalculated after dissolve operations
            - Hectares chosen for CORINE compatibility (typical MMU = 25 ha)
        """
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
        """
        Extract file geodatabase path from a feature class path.

        Handles both catalog paths and file system paths. Walks up the
        path hierarchy until finding a .gdb folder.

        Args:
            path: Full path to feature class or dataset

        Returns:
            Path to containing .gdb, or empty string if not found

        Examples:
            >>> _extract_gdb_path("C:/data/output.gdb/dataset/fc")
            "C:/data/output.gdb"

            >>> _extract_gdb_path("C:/data/output.gdb/fc")
            "C:/data/output.gdb"

            >>> _extract_gdb_path("C:/data/shapefile.shp")
            ""

        Notes:
            - First tries arcpy.Describe (handles catalog paths)
            - Falls back to string parsing if Describe fails
            - Returns empty string for non-GDB paths (shapefiles, etc.)
        """
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
    ArcGIS Script Tool parameter wrapper.

    This function is called when the tool is executed from ArcGIS Pro/ArcMap.
    It extracts parameters from the geoprocessing framework and passes them
    to the CorineGeneralizer class.

    Script Tool Parameters (in order):
        0: input_change (Feature Class)
           - Change polygon dataset with CHCODE field

        1: input_revision (Feature Class)
           - Revision polygon dataset with REVCODE field

        2: out_general (Feature Class, derived/output)
           - Output generalized feature class path
           - Will be created/overwritten

        3: priority_table (Table/DBF)
           - Priority lookup table with CODE and PRI fields
           - Format: CODE (text/long), PRI (long)

        4: from_value (Long, optional)
           - Starting area threshold in hectares
           - Default: 3

        5: to_value (Long, optional)
           - Ending area threshold in hectares
           - Default: 23

        6: by_value (Long, optional)
           - Step size for thresholds
           - Default: 5

    Example Tool Execution:
        Input Change: C:/data/change.shp
        Input Revision: C:/data/revision.shp
        Output: C:/output.gdb/generalized
        Priority Table: C:/lookup/join_pri.dbf
        From: 3
        To: 23
        By: 5

        Result: Processes thresholds 3, 8, 13, 18, 23 hectares

    Notes:
        - Parameters extracted using arcpy.GetParameter/GetParameterAsText
        - Output parameter (2) must be set with SetParameterAsText for ArcGIS
        - Empty parameters use default values from Config dataclass
        - All exceptions logged to GP messages pane
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
    """
    Module entry point for script tool execution.

    When run as a script tool, this block:
    1. Calls script_tool() to process GP parameters
    2. Catches and logs any exceptions to GP messages pane
    3. Re-raises exception to mark tool as failed

    Exception Handling:
        - All exceptions caught and formatted with full traceback
        - Logged to GP pane using arcpy.AddError
        - Falls back to print() if arcpy unavailable
        - Exception re-raised to ensure tool shows as failed

    Example Traceback Output:
        ERROR 000001: Traceback (most recent call last):
          File "corine_iterator.py", line 850, in <module>
            script_tool()
          File "corine_iterator.py", line 825, in script_tool
            result_fc = CorineGeneralizer(cfg).run()
          File "corine_iterator.py", line 215, in run
            self._validate_inputs()
          File "corine_iterator.py", line 285, in _validate_inputs
            raise FileNotFoundError(f"{nm} does not exist: {pth}")
        FileNotFoundError: input_change does not exist: C:/missing.shp
    """
    try:
        script_tool()
    except Exception:
        tb = traceback.format_exc()
        try: arcpy.AddError(tb)
        except Exception: print(tb)
        raise
