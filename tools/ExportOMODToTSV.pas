{
  ExportOMODToTSV.pas
  ────────────────────
  xEdit script — exports OMOD records to TWO TSV files:

  1. OMOD_Export.tsv — main record info (one row per OMOD):
       OMOD_FormID · OMOD_EDID · FULL · DESC
       FormType · MaxRank · IncludeCount · PropertyCount
       AttachPoint_FormID · AttachPoint_EDID · AttachPoint_Name
       AttachParentSlots · Includes_Flat
       MNAM_TargetKeywords · FNAM_FilterKeywords
       LNAM_LooseMod · DNAM_DependantMod
       NAM1_Priority · FLTR
       DATA_Flat
       ReferencedByCount · Ref_1 … Ref_30

  2. OMOD_Properties_Export.tsv — one row per property entry:
       OMOD_FormID · OMOD_EDID · PropIndex
       ValueType · FunctionType · PropertyName
       Value1 · Value2 · CurveTable

     This captures ALL Float values that the old export missed.
     Join back to the main file on OMOD_FormID.

  Usage
  ─────
    1.  Open SeventySix.esm in FO76Edit.
    2.  Expand OMOD group → select all OMOD records (or the group
        node itself to export every record).
    3.  Right-click → Apply Script → choose ExportOMODToTSV.
    4.  Output: <xEdit folder>\OMOD_Export.tsv
               <xEdit folder>\OMOD_Properties_Export.tsv
        Rename with the month tag before committing to dfbnb-data.
}
unit ExportOMODToTSV;

const
  TAB       = #9;
  MAX_REFS  = 30;

var
  slMain: TStringList;    // OMOD_Export.tsv
  slProps: TStringList;   // OMOD_Properties_Export.tsv
  recCount: Integer;
  propRowCount: Integer;

// ─── helpers ────────────────────────────────────────────────────

function Q(const s: string): string;
begin
  Result := StringReplace(s, '"', '""', [rfReplaceAll]);
end;

function FID(e: IInterface): string;
begin
  if not Assigned(e) then
    Result := ''
  else
    Result := IntToHex(FormID(e) and $00FFFFFF, 8);
end;

function GEV(e: IInterface; const path: string): string;
var
  el: IInterface;
begin
  Result := '';
  if not Assigned(e) then Exit;
  el := ElementByPath(e, path);
  if Assigned(el) then
    Result := GetEditValue(el);
end;

function GEVDirect(e: IInterface): string;
begin
  if Assigned(e) then
    Result := GetEditValue(e)
  else
    Result := '';
end;

function RefTag(e: IInterface): string;
// Build "EDID[FormID]" from a linked record.
begin
  if not Assigned(e) then begin
    Result := '';
    Exit;
  end;
  Result := EditorID(e) + '[' + FID(e) + ']';
end;

// ─── Includes helpers ──────────────────────────────────────────

function BuildIncludesFlat(rec: IInterface; var inclCount: Integer): string;
var
  includes, inc, modRef, linkedMod: IInterface;
  i, cnt: Integer;
  s: string;
begin
  Result := '';
  inclCount := 0;
  includes := ElementByName(rec, 'Includes');
  if not Assigned(includes) then Exit;

  cnt := ElementCount(includes);
  inclCount := cnt;
  for i := 0 to cnt - 1 do begin
    inc := ElementByIndex(includes, i);
    if not Assigned(inc) then Continue;

    modRef := ElementByName(inc, 'Mod');
    if Assigned(modRef) then begin
      linkedMod := LinksTo(modRef);
      if Assigned(linkedMod) then
        s := EditorID(linkedMod) + ' "' + GEV(linkedMod, 'FULL') + '" [OMOD:' + FID(linkedMod) + ']'
      else
        s := GEVDirect(modRef);
    end else
      s := '';

    if Result <> '' then
      Result := Result + ' | ';
    Result := Result + s;
  end;
end;

// ─── Property export ───────────────────────────────────────────
// Iterate "Properties (sorted)" and write one row per property
// to the properties TSV.

function ExportProperties(rec: IInterface; const omodFID, omodEDID: string): Integer;
var
  props, prop, elV1, elV2, elCT: IInterface;
  linkedRef: IInterface;
  i, cnt: Integer;
  sVT, sFT, sProp, sV1, sV2, sCT, row: string;
begin
  Result := 0;
  props := ElementByName(rec, 'Properties (sorted)');
  if not Assigned(props) then Exit;

  cnt := ElementCount(props);
  Result := cnt;

  for i := 0 to cnt - 1 do begin
    prop := ElementByIndex(props, i);
    if not Assigned(prop) then Continue;

    // Value Type (enum: Int, Float, Bool, FormID,Int, FormID,Float, etc.)
    sVT := GEV(prop, 'Value Type');
    // Function Type (enum: SET, MUL+ADD, ADD, REM, etc.)
    sFT := GEV(prop, 'Function Type');
    // Property (enum name: DamageBonusMult, ArmorPenetration, etc.)
    sProp := GEV(prop, 'Property');

    // Value 1 — may be a float, int, or FormID reference
    elV1 := ElementByName(prop, 'Value 1');
    if Assigned(elV1) then begin
      linkedRef := LinksTo(elV1);
      if Assigned(linkedRef) then
        sV1 := EditorID(linkedRef) + ' "' + GEV(linkedRef, 'FULL') + '" [' + Signature(linkedRef) + ':' + FID(linkedRef) + ']'
      else
        sV1 := GEVDirect(elV1);
    end else
      sV1 := '';

    // Value 2 — the critical Float/Int value the old export missed
    elV2 := ElementByName(prop, 'Value 2');
    if Assigned(elV2) then begin
      linkedRef := LinksTo(elV2);
      if Assigned(linkedRef) then
        sV2 := EditorID(linkedRef) + ' [' + Signature(linkedRef) + ':' + FID(linkedRef) + ']'
      else
        sV2 := GEVDirect(elV2);
    end else
      sV2 := '';

    // Curve Table (FormID reference to a CURV record)
    elCT := ElementByName(prop, 'Curve Table');
    if Assigned(elCT) then begin
      linkedRef := LinksTo(elCT);
      if Assigned(linkedRef) then
        sCT := EditorID(linkedRef) + ' [CURV:' + FID(linkedRef) + ']'
      else
        sCT := GEVDirect(elCT);
    end else
      sCT := '';

    row := omodFID     + TAB
         + omodEDID    + TAB
         + IntToStr(i) + TAB
         + sVT         + TAB
         + sFT         + TAB
         + sProp       + TAB
         + Q(sV1)      + TAB
         + Q(sV2)      + TAB
         + Q(sCT);

    slProps.Add(row);
    Inc(propRowCount);
  end;
end;

// ─── Referenced-By helpers ─────────────────────────────────────

procedure CollectRefs(rec: IInterface; var refs: array of string;
  maxRefs: Integer; var refCount: Integer);
var
  i, cnt: Integer;
  refRec: IInterface;
begin
  for i := 0 to maxRefs - 1 do
    refs[i] := '';
  cnt := ReferencedByCount(rec);
  refCount := cnt;
  for i := 0 to cnt - 1 do begin
    if i >= maxRefs then Break;
    refRec := ReferencedByIndex(rec, i);
    refs[i] := FID(refRec) + ':' + EditorID(refRec) + ':' + Signature(refRec);
  end;
end;

// ─── Keyword list helper ───────────────────────────────────────

function BuildKeywordList(container: IInterface): string;
var
  i, cnt: Integer;
  entry, linked: IInterface;
begin
  Result := '';
  if not Assigned(container) then Exit;
  cnt := ElementCount(container);
  for i := 0 to cnt - 1 do begin
    entry := ElementByIndex(container, i);
    if not Assigned(entry) then Continue;
    linked := LinksTo(entry);
    if Assigned(linked) then begin
      if Result <> '' then Result := Result + ' | ';
      Result := Result + EditorID(linked) + ' "' + GEV(linked, 'FULL') + '" [KYWD:' + FID(linked) + ']';
    end;
  end;
end;

// ═══════════════════════════════════════════════════════════════
//  Initialize — build header rows
// ═══════════════════════════════════════════════════════════════

function Initialize: Integer;
var
  hdrMain, hdrProps: string;
  i: Integer;
begin
  slMain  := TStringList.Create;
  slProps := TStringList.Create;
  recCount := 0;
  propRowCount := 0;

  // ── Main file header ──
  hdrMain := 'OMOD_FormID'        + TAB
           + 'OMOD_EDID'          + TAB
           + 'FULL'               + TAB
           + 'DESC'               + TAB
           + 'FormType'           + TAB
           + 'MaxRank'            + TAB
           + 'IncludeCount'       + TAB
           + 'PropertyCount'      + TAB
           + 'AttachPoint_FormID' + TAB
           + 'AttachPoint_EDID'   + TAB
           + 'AttachPoint_Name'   + TAB
           + 'AttachParentSlots'  + TAB
           + 'Includes_Flat'      + TAB
           + 'MNAM_TargetKWDs'    + TAB
           + 'FNAM_FilterKWDs'    + TAB
           + 'LNAM_LooseMod'      + TAB
           + 'DNAM_DependantMod'  + TAB
           + 'NAM1_Priority'      + TAB
           + 'FLTR'               + TAB
           + 'DATA_Flat'          + TAB
           + 'ReferencedByCount';

  for i := 1 to MAX_REFS do
    hdrMain := hdrMain + TAB + 'Ref_' + IntToStr(i);

  slMain.Add(hdrMain);

  // ── Properties file header ──
  hdrProps := 'OMOD_FormID'  + TAB
            + 'OMOD_EDID'    + TAB
            + 'PropIndex'    + TAB
            + 'ValueType'    + TAB
            + 'FunctionType' + TAB
            + 'PropertyName' + TAB
            + 'Value1'       + TAB
            + 'Value2'       + TAB
            + 'CurveTable';

  slProps.Add(hdrProps);
  Result := 0;
end;

// ═══════════════════════════════════════════════════════════════
//  Process — called once per selected record
// ═══════════════════════════════════════════════════════════════

function Process(e: IInterface): Integer;
var
  rec, elAP, linkedAP, elDATA: IInterface;
  elLNAM, elDNAM, linkedLNAM, linkedDNAM: IInterface;
  elMNAM, elFNAM: IInterface;
  i, inclCount, propCount, refCount: Integer;
  row: string;
  omodFID, omodEDID, omodFULL, omodDESC: string;
  formType, maxRank: string;
  apFID, apEDID, apName, apSlots: string;
  includesFlat, dataFlat: string;
  mnamKWDs, fnamKWDs, lnamStr, dnamStr, nam1Str, fltrStr: string;
  refs: array [0..29] of string;
begin
  Result := 0;
  if Signature(e) <> 'OMOD' then Exit;
  rec := e;
  Inc(recCount);

  // ── Header fields ───────────────────────────────────────────
  omodFID  := FID(rec);
  omodEDID := EditorID(rec);
  omodFULL := GEV(rec, 'FULL');
  omodDESC := GEV(rec, 'DESC');
  formType := GEV(rec, 'DATA\Form Type');
  maxRank  := GEV(rec, 'DATA\Max Rank');

  // Attach Point
  elAP := ElementByPath(rec, 'DATA\Attach Point');
  if Assigned(elAP) then begin
    linkedAP := LinksTo(elAP);
    if Assigned(linkedAP) then begin
      apFID  := FID(linkedAP);
      apEDID := EditorID(linkedAP);
      apName := GEV(linkedAP, 'FULL');
    end else begin
      apFID := GEVDirect(elAP); apEDID := ''; apName := '';
    end;
  end else begin
    apFID := ''; apEDID := ''; apName := '';
  end;

  apSlots := GEV(rec, 'DATA\Attach Parent Slots');

  // Includes
  inclCount := 0;
  includesFlat := BuildIncludesFlat(rec, inclCount);

  // MNAM Target Keywords
  elMNAM := ElementBySignature(rec, 'MNAM');
  mnamKWDs := '';
  if Assigned(elMNAM) then
    mnamKWDs := BuildKeywordList(elMNAM);

  // FNAM Filter Keywords
  elFNAM := ElementBySignature(rec, 'FNAM');
  fnamKWDs := '';
  if Assigned(elFNAM) then
    fnamKWDs := BuildKeywordList(elFNAM);

  // LNAM Loose Mod
  elLNAM := ElementBySignature(rec, 'LNAM');
  lnamStr := '';
  if Assigned(elLNAM) then begin
    linkedLNAM := LinksTo(elLNAM);
    if Assigned(linkedLNAM) then
      lnamStr := EditorID(linkedLNAM) + '[' + FID(linkedLNAM) + ']'
    else
      lnamStr := GEVDirect(elLNAM);
  end;

  // DNAM Dependant Mod
  elDNAM := ElementBySignature(rec, 'DNAM');
  dnamStr := '';
  if Assigned(elDNAM) then begin
    linkedDNAM := LinksTo(elDNAM);
    if Assigned(linkedDNAM) then
      dnamStr := EditorID(linkedDNAM) + '[' + FID(linkedDNAM) + ']'
    else
      dnamStr := GEVDirect(elDNAM);
  end;

  // NAM1 Priority
  nam1Str := GEV(rec, 'NAM1');

  // FLTR Filter string
  fltrStr := GEV(rec, 'FLTR');

  // DATA flat
  elDATA := ElementByName(rec, 'DATA');
  if Assigned(elDATA) then
    dataFlat := GEVDirect(elDATA)
  else
    dataFlat := '';

  // ── Properties → separate file ─────────────────────────────
  propCount := ExportProperties(rec, omodFID, omodEDID);

  // ── Referenced-By ───────────────────────────────────────────
  refCount := 0;
  CollectRefs(rec, refs, MAX_REFS, refCount);

  // ── Build main row ──────────────────────────────────────────
  row := omodFID                + TAB
       + omodEDID               + TAB
       + Q(omodFULL)            + TAB
       + Q(omodDESC)            + TAB
       + formType               + TAB
       + maxRank                + TAB
       + IntToStr(inclCount)    + TAB
       + IntToStr(propCount)    + TAB
       + apFID                  + TAB
       + apEDID                 + TAB
       + Q(apName)              + TAB
       + Q(apSlots)             + TAB
       + Q(includesFlat)        + TAB
       + Q(mnamKWDs)            + TAB
       + Q(fnamKWDs)            + TAB
       + Q(lnamStr)             + TAB
       + Q(dnamStr)             + TAB
       + nam1Str                + TAB
       + Q(fltrStr)             + TAB
       + Q(dataFlat)            + TAB
       + IntToStr(refCount);

  for i := 0 to MAX_REFS - 1 do
    row := row + TAB + refs[i];

  slMain.Add(row);
end;

// ═══════════════════════════════════════════════════════════════
//  Finalize — write both files
// ═══════════════════════════════════════════════════════════════

function Finalize: Integer;
var
  pathMain, pathProps: string;
begin
  pathMain  := ProgramPath + 'OMOD_Export.tsv';
  pathProps := ProgramPath + 'OMOD_Properties_Export.tsv';

  AddMessage('Saving ' + IntToStr(recCount) + ' OMOD records -> ' + pathMain);
  slMain.SaveToFile(pathMain);
  slMain.Free;

  AddMessage('Saving ' + IntToStr(propRowCount) + ' property rows -> ' + pathProps);
  slProps.SaveToFile(pathProps);
  slProps.Free;

  AddMessage('Done.  Rename both files with the month tag before committing.');
  Result := 0;
end;

end.
