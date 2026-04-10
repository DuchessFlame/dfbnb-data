{
  ExportALCHToTSV.pas
  ====================
  Exports selected ALCH (Ingestible) records to TSV.

  Fix history:
    - 2026-04-11: Rewritten to resolve the ENIT subrecord with
      ElementBySignature instead of the label string
      'ENIT - Effect item settings'. The label text drifts between
      xEdit versions; when it stops matching, every ENIT_* column
      silently empties. Signatures (four-character record codes) are
      immutable, so lookups survive xEdit updates. DATA weight falls
      back to name-lookup within the DATA struct.

  Columns (header):
    FormID, EDID, FULL, DESC, Weight, Value, DNAM, Keywords_Flat,
    KeywordCount, Keyword_1..Keyword_N,
    ENIT_Value, ENIT_Flags, ENIT_Addiction_FormID, ENIT_Addiction_EDID,
    ENIT_Addiction_FULL, ENIT_AddictionChance, ENIT_ConsumeSound_FormID,
    ENIT_ConsumeSound_EDID, ENIT_ConsumeSound_FULL, ENIT_HealthCurve_FormID,
    ENIT_HealthCurve_EDID, ENIT_HealthCurve_FULL

  Keywords are variable-width (0..N columns). Two-phase process:
    - Phase 1 collects keywords with an internal separator
    - Phase 2 (WriteOutput) pads each row to maxKW columns.
}

unit UserScript;

var
  slData, slOut: TStringList;
  SaveDialog: TSaveDialog;
  maxKW: integer;

const
  FIELD_SEP = #31;  // internal separator for base fields
  KWDA_SEP  = #30;  // internal separator for keywords list

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

{ Look up a sub-element by name, falling back to path lookup. Returns
  nil if neither resolves. }
function SubByName(parent: IInterface; const name: string): IInterface;
begin
  Result := nil;
  if not Assigned(parent) then Exit;
  Result := ElementByName(parent, name);
  if not Assigned(Result) then
    Result := ElementByPath(parent, name);
end;

{ Read sub-element edit value by name, blank on miss. }
function SubEditValue(parent: IInterface; const name: string): string;
var
  el: IInterface;
begin
  Result := '';
  el := SubByName(parent, name);
  if Assigned(el) then
    Result := Trim(GetEditValue(el));
end;

{ Resolve a linked record under a struct element by sub-field name. }
function SubLink(parent: IInterface; const name: string): IInterface;
var
  el: IInterface;
begin
  Result := nil;
  el := SubByName(parent, name);
  if Assigned(el) then
    Result := LinksTo(el);
end;

function GetFirstNonEmptyEditValue(e: IInterface; const pathA, pathB: string): string;
begin
  Result := Trim(GetElementEditValues(e, pathA));
  if Result = '' then
    Result := Trim(GetElementEditValues(e, pathB));
end;

function BuildHeader(aMaxKW: integer): string;
var
  i: integer;
begin
  Result := 'FormID'#9'EDID'#9'FULL'#9'DESC'#9'Weight'#9'Value'#9'DNAM'#9'Keywords_Flat'#9'KeywordCount';
  for i := 1 to aMaxKW do
    Result := Result + #9 + 'Keyword_' + IntToStr(i);
  Result := Result + #9'ENIT_Value'#9'ENIT_Flags'#9'ENIT_Addiction_FormID'#9'ENIT_Addiction_EDID'#9'ENIT_Addiction_FULL'#9'ENIT_AddictionChance'#9'ENIT_ConsumeSound_FormID'#9'ENIT_ConsumeSound_EDID'#9'ENIT_ConsumeSound_FULL'#9'ENIT_HealthCurve_FormID'#9'ENIT_HealthCurve_EDID'#9'ENIT_HealthCurve_FULL';
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
  maxKW := 0;

  SaveDialog := TSaveDialog.Create(nil);
  SaveDialog.Options := SaveDialog.Options + [ofOverwritePrompt];

  dt := Now;
  fn := FormatDateTime('mmm_yyyy', dt);
  SaveDialog.FileName := 'ALCH_Export_' + fn + '.tsv';
  SaveDialog.Filter := 'TSV files (*.tsv)|*.tsv|All files (*.*)|*.*';
  SaveDialog.Title := 'Save ALCH Export TSV';
end;

function Process(e: IInterface): integer;
var
  edid, fullN, desc, weight, value, dnam: string;
  enitVal, enitFlags, enitAddChance: string;
  dataEl, enitEl: IInterface;
  kwdaEl: IInterface;
  i, kwCount: integer;
  kw: IInterface;
  kwdsJoined, kwdsFlat: string;
  addictionLink, consumeSoundLink, healthCurveLink: IInterface;
  addictionStr, consumeSoundStr, healthCurveStr: string;
  rec: string;
begin
  Result := 0;

  if not Assigned(e) then Exit;
  if Signature(e) <> 'ALCH' then Exit;

  edid := CleanCell(EditorID(e));
  fullN := CleanCell(GetFirstNonEmptyEditValue(e, 'FULL - Name', 'FULL'));
  desc := CleanCell(GetFirstNonEmptyEditValue(e, 'DESC - Description', 'DESC'));
  dnam := CleanCell(GetFirstNonEmptyEditValue(e, 'DNAM - Addiction', 'DNAM'));

  // DATA subrecord — signature-based
  dataEl := ElementBySignature(e, 'DATA');
  weight := '';
  value := '';
  if Assigned(dataEl) then begin
    weight := CleanCell(SubEditValue(dataEl, 'Weight'));
    // Some ALCH DATA structs also expose Value; if not present we
    // fall back to ENIT.Value below.
    value := CleanCell(SubEditValue(dataEl, 'Value'));
  end;

  // Keywords (KWDA) — already signature-based in the original, kept.
  kwdaEl := ElementBySignature(e, 'KWDA');
  kwCount := 0;
  kwdsJoined := '';
  kwdsFlat := '';

  if Assigned(kwdaEl) then begin
    kwCount := ElementCount(kwdaEl);
    if kwCount > maxKW then
      maxKW := kwCount;

    for i := 0 to kwCount - 1 do begin
      kw := LinksTo(ElementByIndex(kwdaEl, i));
      if Assigned(kw) then begin
        if kwdsJoined <> '' then
          kwdsJoined := kwdsJoined + KWDA_SEP;
        kwdsJoined := kwdsJoined + CleanCell(LinkCell(kw));

        if kwdsFlat <> '' then
          kwdsFlat := kwdsFlat + '|';
        kwdsFlat := kwdsFlat + CleanCell(SafeEditorID(kw));
      end;
    end;
  end;

  // ENIT subrecord — signature-based lookup
  enitVal := '';
  enitFlags := '';
  enitAddChance := '';
  addictionLink := nil;
  consumeSoundLink := nil;
  healthCurveLink := nil;

  enitEl := ElementBySignature(e, 'ENIT');
  if Assigned(enitEl) then begin
    enitVal := CleanCell(SubEditValue(enitEl, 'Value'));
    enitFlags := CleanCell(SubEditValue(enitEl, 'Flags'));
    enitAddChance := CleanCell(SubEditValue(enitEl, 'Addiction Chance'));

    addictionLink   := SubLink(enitEl, 'Addiction');
    consumeSoundLink := SubLink(enitEl, 'Consume Sound');
    healthCurveLink  := SubLink(enitEl, 'Health Curve');
  end;

  // If DATA didn't expose a Value but ENIT does, promote it so the
  // older Value column isn't blank across the whole export.
  if (value = '') and (enitVal <> '') then
    value := enitVal;

  if Assigned(addictionLink) then
    addictionStr := HexFormID8(addictionLink) + FIELD_SEP + SafeEditorID(addictionLink) + FIELD_SEP + CleanCell(GetElementEditValues(addictionLink, 'FULL - Name'))
  else
    addictionStr := FIELD_SEP + FIELD_SEP;

  if Assigned(consumeSoundLink) then
    consumeSoundStr := HexFormID8(consumeSoundLink) + FIELD_SEP + SafeEditorID(consumeSoundLink) + FIELD_SEP + CleanCell(GetElementEditValues(consumeSoundLink, 'FULL - Name'))
  else
    consumeSoundStr := FIELD_SEP + FIELD_SEP;

  if Assigned(healthCurveLink) then
    healthCurveStr := HexFormID8(healthCurveLink) + FIELD_SEP + SafeEditorID(healthCurveLink) + FIELD_SEP + CleanCell(GetElementEditValues(healthCurveLink, 'FULL - Name'))
  else
    healthCurveStr := FIELD_SEP + FIELD_SEP;

  // Internal record (pad later)
  rec :=
    HexFormID8(e) + FIELD_SEP +
    edid + FIELD_SEP +
    fullN + FIELD_SEP +
    desc + FIELD_SEP +
    weight + FIELD_SEP +
    value + FIELD_SEP +
    dnam + FIELD_SEP +
    kwdsFlat + FIELD_SEP +
    IntToStr(kwCount) + FIELD_SEP +
    kwdsJoined + FIELD_SEP +
    enitVal + FIELD_SEP +
    enitFlags + FIELD_SEP +
    addictionStr + FIELD_SEP +
    enitAddChance + FIELD_SEP +
    consumeSoundStr + FIELD_SEP +
    healthCurveStr;

  slData.Add(rec);
end;

procedure WriteOutput;
var
  i, j, kwCount: integer;
  rec: string;
  parts, kwds: TStringList;
  line: string;
begin
  slOut.Clear;
  slOut.Add(BuildHeader(maxKW));

  parts := TStringList.Create;
  kwds  := TStringList.Create;
  try
    parts.Delimiter := FIELD_SEP;
    parts.StrictDelimiter := True;

    kwds.Delimiter := KWDA_SEP;
    kwds.StrictDelimiter := True;

    for i := 0 to slData.Count - 1 do begin
      rec := slData[i];
      parts.DelimitedText := rec;

      // Basic fields: FormID, EDID, FULL, DESC, Weight, Value, DNAM, Keywords_Flat, KeywordCount (9)
      if parts.Count < 9 then
        Continue;

      line := parts[0] + #9 + parts[1] + #9 + parts[2] + #9 + parts[3] + #9 + parts[4] + #9 + parts[5] + #9 + parts[6] + #9 + parts[7] + #9 + parts[8];

      kwCount := StrToIntDef(parts[8], 0);

      // Keyword columns
      kwds.Clear;
      if parts.Count >= 10 then
        kwds.DelimitedText := parts[9];

      for j := 0 to maxKW - 1 do begin
        if j < kwds.Count then
          line := line + #9 + kwds[j]
        else
          line := line + #9;
      end;

      // ENIT fields (parts[10] onward)
      if parts.Count >= 16 then begin
        line := line + #9 + parts[10];  // ENIT_Value
        line := line + #9 + parts[11];  // ENIT_Flags

        // Addiction (FormID:EDID:FULL split from FIELD_SEP)
        line := line + #9 + parts[12];  // ENIT_Addiction_FormID
        line := line + #9 + parts[13];  // ENIT_Addiction_EDID
        line := line + #9 + parts[14];  // ENIT_Addiction_FULL
        line := line + #9 + parts[15];  // ENIT_AddictionChance

        // ConsumeSound
        if parts.Count >= 18 then begin
          line := line + #9 + parts[16];  // ENIT_ConsumeSound_FormID
          line := line + #9 + parts[17];  // ENIT_ConsumeSound_EDID
          line := line + #9 + parts[18];  // ENIT_ConsumeSound_FULL

          // HealthCurve
          if parts.Count >= 21 then begin
            line := line + #9 + parts[19];  // ENIT_HealthCurve_FormID
            line := line + #9 + parts[20];  // ENIT_HealthCurve_EDID
            line := line + #9 + parts[21];  // ENIT_HealthCurve_FULL
          end;
        end;
      end;

      slOut.Add(line);
    end;

  finally
    kwds.Free;
    parts.Free;
  end;
end;

function Finalize: integer;
begin
  Result := 0;

  if slData.Count = 0 then begin
    AddMessage('[ALCH Export] No ALCH records processed. Select ALCH records and run again.');
    slData.Free;
    slOut.Free;
    SaveDialog.Free;
    Exit;
  end;

  if not SaveDialog.Execute then begin
    AddMessage('[ALCH Export] Cancelled.');
    slData.Free;
    slOut.Free;
    SaveDialog.Free;
    Exit;
  end;

  WriteOutput;
  slOut.SaveToFile(SaveDialog.FileName);

  AddMessage('[ALCH Export] Wrote: ' + SaveDialog.FileName);
  AddMessage(Format('[ALCH Export] Records: %d | Max Keywords: %d', [slData.Count, maxKW]));

  slData.Free;
  slOut.Free;
  SaveDialog.Free;
end;

end.
