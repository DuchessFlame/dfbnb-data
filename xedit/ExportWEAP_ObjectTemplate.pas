{
  ExportWEAP_ObjectTemplate.pas
  xEdit script — exports WEAP Object Template (mod slots / legendary) to TSV.

  Output: WEAP_Export_{DateStamp}_ObjectTemplate.tsv
  Each row = one Include (mod slot) on a weapon's Object Template Combination.
  Columns: FormID, EDID, FULL, CombinationIndex, IncludeIndex,
           Mod (OMOD ref), AttachPointIndex, Optional, DontUseAll

  This captures the full mod breakdown for named/unique weapons:
    Include #0 → Appearance (weapon paint)
    Include #1 → Lawbringer paint (ATX paint OMOD)
    Include #2 → Legendary 1★ (e.g. Adrenal)
    Include #3 → Legendary 2★ (e.g. Rapid)
    Include #4 → Legendary 3★ (e.g. Swift)
    Include #5 → Legendary 4★ (e.g. Thrill-Seeker's)
    Include #6 → Receiver
    Include #7 → Grip
    Include #8 → Sights
    Include #9 → Barrel
    ...etc.

  Usage: Select [00] SeventySix.esm → Apply Script → ExportWEAP_ObjectTemplate

  Part 2 of 3 WEAP exports. See also:
    ExportWEAP_Base.pas  — core record data
    ExportWEAP_DNAM.pas  — combat stats
}

unit ExportWEAP_ObjectTemplate;

var
  sl: TStringList;

function Initialize: integer;
begin
  sl := TStringList.Create;
  sl.Add(
    'WEAP_FormID' + #9 +
    'WEAP_EDID' + #9 +
    'WEAP_FULL' + #9 +
    'OBTE_Count' + #9 +
    'CombinationIndex' + #9 +
    'OBTF_EditorOnly' + #9 +
    'Combination_FULL' + #9 +
    'OBTS_IncludeCount' + #9 +
    'OBTS_PropertyCount' + #9 +
    'OBTS_LevelMin' + #9 +
    'OBTS_LevelMax' + #9 +
    'OBTS_Default' + #9 +
    'IncludeIndex' + #9 +
    'Include_Mod' + #9 +
    'Include_AttachPointIndex' + #9 +
    'Include_Optional' + #9 +
    'Include_DontUseAll'
  );
  Result := 0;
end;

function Process(e: IInterface): integer;
var
  sig: string;
  formid, edid, full: string;
  objTemplate, combinations: IInterface;
  obteCount: integer;
  ci, ii: integer;
  combo, obts, includes, inc: IInterface;
  obtf, comboFull: string;
  incCount, propCount: integer;
  lvlMin, lvlMax, isDefault: string;
  modRef, attachIdx, optional, dontUseAll: string;
begin
  Result := 0;
  sig := Signature(e);
  if sig <> 'WEAP' then Exit;

  formid := IntToHex(FormID(e) and $00FFFFFF, 8);
  edid   := GetElementEditValues(e, 'EDID');
  full   := GetElementEditValues(e, 'FULL');

  objTemplate := ElementByPath(e, 'Object Template');
  if not Assigned(objTemplate) then Exit;

  obteCount := 0;
  if ElementExists(objTemplate, 'OBTE') then
    obteCount := StrToIntDef(GetElementEditValues(objTemplate, 'OBTE'), 0);

  combinations := ElementByPath(objTemplate, 'Combinations');
  if not Assigned(combinations) then Exit;

  for ci := 0 to ElementCount(combinations) - 1 do begin
    combo := ElementByIndex(combinations, ci);
    if not Assigned(combo) then Continue;

    obtf := GetElementEditValues(combo, 'OBTF');
    comboFull := GetElementEditValues(combo, 'FULL');

    obts := ElementByPath(combo, 'OBTS');
    if not Assigned(obts) then Continue;

    incCount  := StrToIntDef(GetElementEditValues(obts, 'Include Count'), 0);
    propCount := StrToIntDef(GetElementEditValues(obts, 'Property Count'), 0);
    lvlMin    := GetElementEditValues(obts, 'Level Min');
    lvlMax    := GetElementEditValues(obts, 'Level Max');
    isDefault := GetElementEditValues(obts, 'Default');

    includes := ElementByPath(obts, 'Includes');
    if not Assigned(includes) then Continue;

    for ii := 0 to ElementCount(includes) - 1 do begin
      inc := ElementByIndex(includes, ii);
      if not Assigned(inc) then Continue;

      modRef     := GetElementEditValues(inc, 'Mod');
      attachIdx  := GetElementEditValues(inc, 'Attach Point Index');
      optional   := GetElementEditValues(inc, 'Optional');
      dontUseAll := GetElementEditValues(inc, 'Don''t Use All');

      sl.Add(
        formid + #9 +
        edid + #9 +
        full + #9 +
        IntToStr(obteCount) + #9 +
        IntToStr(ci) + #9 +
        obtf + #9 +
        comboFull + #9 +
        IntToStr(incCount) + #9 +
        IntToStr(propCount) + #9 +
        lvlMin + #9 +
        lvlMax + #9 +
        isDefault + #9 +
        IntToStr(ii) + #9 +
        modRef + #9 +
        attachIdx + #9 +
        optional + #9 +
        dontUseAll
      );
    end;
  end;
end;

function Finalize: integer;
var
  fname: string;
begin
  fname := ProgramPath + 'WEAP_Export_ObjectTemplate.tsv';
  AddMessage('Saving ' + IntToStr(sl.Count - 1) + ' WEAP Object Template rows to: ' + fname);
  sl.SaveToFile(fname);
  sl.Free;
  Result := 0;
end;

end.
