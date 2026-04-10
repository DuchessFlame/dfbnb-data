(*
  ExportCOBJToTSV.pas
  ====================
  Exports selected COBJ (Constructible Object) records to TSV.

  Fix history:
    - 2026-04-11: Rewritten to use ElementBySignature instead of
      ElementByPath('XXXX - Label') — the label text drifts between
      xEdit versions, which silently returned nil and produced empty
      GNAM/FNAM/FVPA columns in the April 2026 export. Signatures are
      immutable. Also added BNAM (real workbench keyword in FO76) and
      explicit sub-field parsing for FVPA Component/Count pairs.
    - 2026-04-11 (later): Switched the outer header comment from
      {...} to (* ... *) because Pascal {...} comments don't nest —
      the inner {workbench keyword - FO76} annotation was silently
      closing the outer block, causing xEdit to throw
      "'unit' expected but 'GNAM_FormID' found." on load.

  Columns (header):
    COBJ_FormID, COBJ_EDID,
    CNAM_FormID, CNAM_EDID, CNAM_FULL,
    BNAM_FormID, BNAM_EDID, BNAM_FULL      -- workbench keyword (FO76)
    GNAM_FormID, GNAM_EDID, GNAM_FULL      -- recipe book ref (legacy/optional)
    FNAM_Keywords, FVPA, ReferencedBy_Flat, ReferencedByCount,
    Ref_1..Ref_N

  Legacy builders (build_cobj_recipes_json.py) accept either
  COBJ_EDID or EDID for the EDID column — we emit COBJ_EDID so older
  monthly TSVs stay comparable.

  Variable-width Ref columns (0..N). FNAM and FVPA are flat-joined
  with pipes. FVPA format: "ComponentEDID:Count|ComponentEDID:Count".
*)

unit UserScript;

var
  slData, slOut: TStringList;
  SaveDialog: TSaveDialog;
  maxRefs: integer;

const
  FIELD_SEP = #31;  // internal separator for base fields
  REFS_SEP  = #30;  // internal separator for refs list

// --------------------------
// Helpers
// --------------------------

function HexFormID8(e: IInterface): string;
begin
  if Assigned(e) then
    Result := IntToHex(GetLoadOrderFormID(e), 8)
  else
    Result := '';
end;

function SafeEditorID(e: IInterface): string;
begin
  if Assigned(e) then
    Result := EditorID(e)
  else
    Result := '';
end;

function CleanCell(const s: string): string;
var
  t: string;
begin
  t := s;
  t := StringReplace(t, #13#10, ' ', [rfReplaceAll]);
  t := StringReplace(t, #13,    ' ', [rfReplaceAll]);
  t := StringReplace(t, #10,    ' ', [rfReplaceAll]);
  t := StringReplace(t, #9,     ' ', [rfReplaceAll]);
  Result := Trim(t);
end;

function LinkCell(target: IInterface): string;
begin
  if Assigned(target) then
    Result := HexFormID8(target) + ':' + SafeEditorID(target) + ':' + Signature(target)
  else
    Result := '';
end;

{ Resolve a linked record from a subrecord signature (e.g. 'CNAM'). }
function LinkBySig(e: IInterface; const sig: string): IInterface;
var
  el: IInterface;
begin
  Result := nil;
  if not Assigned(e) then Exit;
  el := ElementBySignature(e, sig);
  if Assigned(el) then
    Result := LinksTo(el);
end;

{ Return a short link record "HEX:EDID" for a target, or three empty
  FIELD_SEP-separated columns if target is nil. Writes the three
  output parameters so the caller can place them directly in the record. }
procedure ExpandLinkCols(target: IInterface; var outFormID, outEDID, outFull: string);
begin
  if Assigned(target) then begin
    outFormID := HexFormID8(target);
    outEDID   := SafeEditorID(target);
    outFull   := CleanCell(GetElementEditValues(target, 'FULL - Name'));
    if outFull = '' then
      outFull := CleanCell(GetElementEditValues(target, 'FULL'));
  end else begin
    outFormID := '';
    outEDID   := '';
    outFull   := '';
  end;
end;

{ Try to pull a sub-field from an FVPA entry by name, falling back to
  positional index. xEdit has used 'Component'/'Count' as sub-field
  names across versions but some builds expose them via signature
  'CVPA' or plain index — we try a few before giving up. }
function FvpaSubByName(entry: IInterface; const name: string; idx: integer): IInterface;
begin
  Result := nil;
  if not Assigned(entry) then Exit;
  Result := ElementByName(entry, name);
  if not Assigned(Result) then
    Result := ElementByPath(entry, name);
  if not Assigned(Result) then
    Result := ElementByIndex(entry, idx);
end;

function BuildHeader(aMaxRefs: integer): string;
var
  i: integer;
begin
  Result :=
    'COBJ_FormID'#9'COBJ_EDID'#9 +
    'CNAM_FormID'#9'CNAM_EDID'#9'CNAM_FULL'#9 +
    'BNAM_FormID'#9'BNAM_EDID'#9'BNAM_FULL'#9 +
    'GNAM_FormID'#9'GNAM_EDID'#9'GNAM_FULL'#9 +
    'FNAM_Keywords'#9'FVPA'#9'ReferencedBy_Flat'#9'ReferencedByCount';
  for i := 1 to aMaxRefs do
    Result := Result + #9 + 'Ref_' + IntToStr(i);
end;

// --------------------------
// xEdit lifecycle
// --------------------------

function Initialize: integer;
var
  dt: TDateTime;
  fn: string;
begin
  Result := 0;

  slData := TStringList.Create;
  slOut  := TStringList.Create;
  maxRefs := 0;

  SaveDialog := TSaveDialog.Create(nil);
  SaveDialog.Options := SaveDialog.Options + [ofOverwritePrompt];

  dt := Now;
  fn := FormatDateTime('mmm_yyyy', dt);
  SaveDialog.FileName := 'COBJ_Export_' + fn + '.tsv';
  SaveDialog.Filter := 'TSV files (*.tsv)|*.tsv|All files (*.*)|*.*';
  SaveDialog.Title := 'Save COBJ Export TSV';
end;

function Process(e: IInterface): integer;
var
  edid: string;
  cnamLink, bnamLink, gnamLink: IInterface;
  cnamFormID, cnamEDID, cnamFull: string;
  bnamFormID, bnamEDID, bnamFull: string;
  gnamFormID, gnamEDID, gnamFull: string;
  fnamEl, fvpaEl, fvEntry, compEl, cntEl, kw: IInterface;
  i, refC: integer;
  kwFlat, fvpaFlat, refsFlat, refsJoined: string;
  compName, cntStr, fvpaPair: string;
  r: IInterface;
  rec: string;
begin
  Result := 0;

  if not Assigned(e) then Exit;
  if Signature(e) <> 'COBJ' then Exit;

  edid := CleanCell(EditorID(e));

  // CNAM (Created Object) — signature-based
  cnamLink := LinkBySig(e, 'CNAM');
  ExpandLinkCols(cnamLink, cnamFormID, cnamEDID, cnamFull);

  // BNAM (Workbench Keyword) — FO76's real workbench ref
  bnamLink := LinkBySig(e, 'BNAM');
  ExpandLinkCols(bnamLink, bnamFormID, bnamEDID, bnamFull);

  // GNAM — emitted for legacy comparability; may be nil in FO76
  gnamLink := LinkBySig(e, 'GNAM');
  ExpandLinkCols(gnamLink, gnamFormID, gnamEDID, gnamFull);

  // FNAM (Keywords) — array of keyword refs, flat pipe-joined
  kwFlat := '';
  fnamEl := ElementBySignature(e, 'FNAM');
  if Assigned(fnamEl) then begin
    for i := 0 to ElementCount(fnamEl) - 1 do begin
      kw := LinksTo(ElementByIndex(fnamEl, i));
      if Assigned(kw) then begin
        if kwFlat <> '' then
          kwFlat := kwFlat + '|';
        kwFlat := kwFlat + CleanCell(SafeEditorID(kw));
      end;
    end;
  end;

  // FVPA (Component requirements) — array of {Component, Count} structs.
  // Parse sub-fields explicitly so we're not at the mercy of GetEditValue.
  fvpaFlat := '';
  fvpaEl := ElementBySignature(e, 'FVPA');
  if Assigned(fvpaEl) then begin
    for i := 0 to ElementCount(fvpaEl) - 1 do begin
      fvEntry := ElementByIndex(fvpaEl, i);
      if not Assigned(fvEntry) then Continue;

      compEl := FvpaSubByName(fvEntry, 'Component', 0);
      cntEl  := FvpaSubByName(fvEntry, 'Count', 1);

      compName := '';
      cntStr := '1';

      if Assigned(compEl) then begin
        compName := CleanCell(SafeEditorID(LinksTo(compEl)));
        if compName = '' then
          compName := CleanCell(GetEditValue(compEl));
      end;

      if Assigned(cntEl) then begin
        cntStr := CleanCell(GetEditValue(cntEl));
        if cntStr = '' then
          cntStr := '1';
      end;

      if compName = '' then Continue;

      fvpaPair := compName + ':' + cntStr;
      if fvpaFlat <> '' then
        fvpaFlat := fvpaFlat + '|';
      fvpaFlat := fvpaFlat + fvpaPair;
    end;
  end;

  // ReferencedBy
  refC := ReferencedByCount(e);
  if refC > maxRefs then
    maxRefs := refC;

  refsFlat := '';
  refsJoined := '';
  for i := 0 to refC - 1 do begin
    r := ReferencedByIndex(e, i);
    if i > 0 then begin
      refsFlat := refsFlat + '|';
      refsJoined := refsJoined + REFS_SEP;
    end;
    refsFlat := refsFlat + CleanCell(LinkCell(r));
    refsJoined := refsJoined + CleanCell(LinkCell(r));
  end;

  rec :=
    HexFormID8(e) + FIELD_SEP +
    edid + FIELD_SEP +
    cnamFormID + FIELD_SEP + cnamEDID + FIELD_SEP + cnamFull + FIELD_SEP +
    bnamFormID + FIELD_SEP + bnamEDID + FIELD_SEP + bnamFull + FIELD_SEP +
    gnamFormID + FIELD_SEP + gnamEDID + FIELD_SEP + gnamFull + FIELD_SEP +
    kwFlat + FIELD_SEP +
    fvpaFlat + FIELD_SEP +
    refsFlat + FIELD_SEP +
    IntToStr(refC) + FIELD_SEP +
    refsJoined;

  slData.Add(rec);
end;

procedure WriteOutput;
var
  i, j, refC: integer;
  rec, refsJoined: string;
  parts, refs: TStringList;
  line: string;
begin
  slOut.Clear;
  slOut.Add(BuildHeader(maxRefs));

  parts := TStringList.Create;
  refs  := TStringList.Create;
  try
    parts.Delimiter := FIELD_SEP;
    parts.StrictDelimiter := True;

    refs.Delimiter := REFS_SEP;
    refs.StrictDelimiter := True;

    for i := 0 to slData.Count - 1 do begin
      rec := slData[i];
      parts.DelimitedText := rec;

      // 15 base fields:
      //  0 COBJ_FormID  1 COBJ_EDID
      //  2 CNAM_FormID  3 CNAM_EDID  4 CNAM_FULL
      //  5 BNAM_FormID  6 BNAM_EDID  7 BNAM_FULL
      //  8 GNAM_FormID  9 GNAM_EDID 10 GNAM_FULL
      // 11 FNAM_Keywords 12 FVPA 13 ReferencedBy_Flat 14 ReferencedByCount
      if parts.Count < 15 then
        Continue;

      line :=
        parts[0] + #9 + parts[1] + #9 +
        parts[2] + #9 + parts[3] + #9 + parts[4] + #9 +
        parts[5] + #9 + parts[6] + #9 + parts[7] + #9 +
        parts[8] + #9 + parts[9] + #9 + parts[10] + #9 +
        parts[11] + #9 + parts[12] + #9 + parts[13] + #9 + parts[14];

      refC := StrToIntDef(parts[14], 0);

      refs.Clear;
      refsJoined := '';
      if parts.Count >= 16 then
        refsJoined := parts[15];

      if refsJoined <> '' then
        refs.DelimitedText := refsJoined;

      // Pad out to maxRefs columns (blank where missing)
      for j := 0 to maxRefs - 1 do begin
        if j < refs.Count then
          line := line + #9 + refs[j]
        else
          line := line + #9;
      end;

      slOut.Add(line);
    end;

  finally
    refs.Free;
    parts.Free;
  end;
end;

function Finalize: integer;
begin
  Result := 0;

  if slData.Count = 0 then begin
    AddMessage('[COBJ Export] No COBJ records processed. Select COBJ records and run again.');
    slData.Free;
    slOut.Free;
    SaveDialog.Free;
    Exit;
  end;

  if not SaveDialog.Execute then begin
    AddMessage('[COBJ Export] Cancelled.');
    slData.Free;
    slOut.Free;
    SaveDialog.Free;
    Exit;
  end;

  WriteOutput;
  slOut.SaveToFile(SaveDialog.FileName);

  AddMessage('[COBJ Export] Wrote: ' + SaveDialog.FileName);
  AddMessage(Format('[COBJ Export] Records: %d | Max ReferencedBy: %d', [slData.Count, maxRefs]));

  slData.Free;
  slOut.Free;
  SaveDialog.Free;
end;

end.
