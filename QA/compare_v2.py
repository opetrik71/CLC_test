import arcpy, os

def code_stats(fc):
    out = {}
    with arcpy.da.SearchCursor(fc, ["NEWCODE","SHAPE@AREA"]) as cur:
        for code, a in cur:
            key = int(code) if code is not None else None
            ha = float(a) / 10000.0  # m2 -> ha, ha projected meters; ha kell: shape.area@HECTARES-re válts
            if key not in out: out[key] = [0, 0.0]
            out[key][0] += 1
            out[key][1] += ha
    return out

def compare(ref_fc, test_fc, wksp):
    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = wksp

    # 1) counts
    ref_cnt = int(arcpy.management.GetCount(ref_fc)[0])
    test_cnt = int(arcpy.management.GetCount(test_fc)[0])

    # 2) code stats
    ref_s = code_stats(ref_fc)
    test_s = code_stats(test_fc)
    all_codes = sorted(set(ref_s) | set(test_s))
    rows = []
    for c in all_codes:
        r = ref_s.get(c, [0,0.0]); t = test_s.get(c, [0,0.0])
        rows.append((c, r[0], t[0], round(r[1],4), round(t[1],4), round(t[1]-r[1],4)))

    # 3) geometric diffs
    symdif = os.path.join(wksp, "qa_symdiff")
    if arcpy.Exists(symdif): arcpy.Delete_management(symdif)
    arcpy.analysis.SymDiff(ref_fc, test_fc, symdif, "ALL", "")
    sym_ha = 0.0
    with arcpy.da.SearchCursor(symdif, ["SHAPE@AREA"]) as cur:
        for a in cur: sym_ha += float(a[0]) / 10000.0

    # 4) union + kódkülönbség
    uni = os.path.join(wksp, "qa_union")
    if arcpy.Exists(uni): arcpy.Delete_management(uni)
    arcpy.analysis.Union([[ref_fc,""], [test_fc,""]], uni, "NO_FID", "", "GAPS")
    # próbáljuk meg rugalmasan megtalálni a mezőket
    flds = [f.name for f in arcpy.ListFields(uni)]
    new_ref = next((f for f in flds if f.lower().startswith("newcode") and f.endswith(("", "_1")) and "_1" not in f), None)
    new_test= next((f for f in flds if f.lower().startswith("newcode") and f.endswith("_1")), None)
    if not new_ref or not new_test:
        # egyszerűbb fallback: az első két NEWCODE-szerű mező
        newcodes = [f for f in flds if f.lower().startswith("newcode")]
        if len(newcodes) >= 2:
            new_ref, new_test = newcodes[0], newcodes[1]
        else:
            new_ref = new_test = None

    code_diff_ha = 0.0
    if new_ref and new_test:
        with arcpy.da.SearchCursor(uni, [new_ref, new_test, "SHAPE@AREA"]) as cur:
            for c1, c2, a in cur:
                if c1 is not None and c2 is not None and int(c1) != int(c2):
                    code_diff_ha += float(a)/10000.0

    print("== QA summary ==")
    print(f"ref polys: {ref_cnt} | test polys: {test_cnt}")
    print(f"SymDiff area (ha): {round(sym_ha,4)}")
    print(f"Code-diff area within union (ha): {round(code_diff_ha,4)}")
    print("code, ref_count, test_count, ref_area_ha, test_area_ha, delta_ha")
    for r in rows:
        print(*r, sep=";")

# sample call
ref_fc  = r"D:\munka\CLC2024\clc_gener\63.gdb\gener_63_ref"
test_fc = r"D:\munka\CLC2024\clc_gener\63.gdb\gener_63_new5"
wksp    = r"D:\munka\CLC2024\clc_gener\63.gdb"
compare(ref_fc, test_fc, wksp)
