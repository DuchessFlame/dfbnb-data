{
  ExportWEAP_Base.pas
  xEdit script — exports core WEAP record data to TSV.

  Output: WEAP_Export_{DateStamp}_Base.tsv
  Columns: FormID, EDID, FULL (Name), Model, Flags (XALG), Keywords,
           EILV Level, IBSD Break Sound, Equipment Type, PTRN Preview Transform

  Usage: Select [00] SeventySix.esm → Apply Script → ExportWEAP_Base

  Part 1 of 3 WEAP exports. See also:
    ExportWEAP_ObjectTemplate.pas  — mod slot / legendary breakdown
    ExportWEAP_DNAM.pas            — combat stats (damage, speed, range, etc.)
}

unit ExportWEAP_Base;

var
  sl: TStringList;

function Initialize: integer;
begin
  sl := TStringList.Create;
  // Header row
  sl.Add(
    'WEAP_FormID' + #9 +
    'WEAP_EDID' + #9 +
    'WEAP_FULL' + #9 +
    'WEAP_Model' + #9 +
    'XALG_Flags' + #9 +
    'EILV_Level' + #9 +
    'ETYP_EquipType' + #9 +
    'PTRN_PreviewTransform' + #9 +
    'IBSD_BreakSound' + #9 +
    'KeywordCount' + #9 +
    'Keywords' + #9 +
    'APPR_SlotCount' + #9 +
    'APPR_Slots'
  );
  Result := 0;
end;

function Process(e: IInterface): integer;
var
  sig: string;
  formid, edid, full, model, flags, eilv, etyp, ptrn, ibsd: string;
  kwda, appr: IInterface;
  kwCount, apprCount, i: integer;
  kwStr, apprStr: string;
  kw, slot: IInterface;
begin
  Result := 0;
  sig := Signature(e);
  if sig <> 'WEAP' then Exit;

  formid := IntToHex(FormID(e) and $00FFFFFF, 8);
  edid   := GetElementEditValues(e, 'EDID');
  full   := GetElementEditValues(e, 'FULL');
  model  := GetElementEditValues(e, 'Model');
  flags  := GetElementEditValues(e, 'XALG');
  ptrn   := GetElementEditValues(e, 'PTRN');
  ibsd   := GetElementEditValues(e, 'IBSD');
  etyp   := GetElementEditValues(e, 'ETYP');

  // Eligible Levels — first level entry
  eilv := '';
  if ElementExists(e, 'EILV') then begin
    eilv := GetElementEditValues(e, 'EILV\Level #0');
  end;

  // Keywords
  kwStr := '';
  kwCount := 0;
  kwda := ElementBySignature(e, 'KWDA');
  if Assigned(kwda) then begin
    kwCount := ElementCount(kwda);
    for i := 0 to kwCount - 1 do begin
      kw := ElementByIndex(kwda, i);
      if i > 0 then kwStr := kwStr + '|';
      kwStr := kwStr + GetEditValue(kw);
    end;
  end;

  // APPR — Attach Parent Slots (sorted)
  apprStr := '';
  apprCount := 0;
  appr := ElementByPath(e, 'APPR');
  if Assigned(appr) then begin
    apprCount := ElementCount(appr);
    for i := 0 to apprCount - 1 do begin
      slot := ElementByIndex(appr, i);
      if i > 0 then apprStr := apprStr + '|';
      apprStr := apprStr + GetEditValue(slot);
    end;
  end;

  sl.Add(
    formid + #9 +
    edid + #9 +
    full + #9 +
    model + #9 +
    flags + #9 +
    eilv + #9 +
    etyp + #9 +
    ptrn + #9 +
    ibsd + #9 +
    IntToStr(kwCount) + #9 +
    kwStr + #9 +
    IntToStr(apprCount) + #9 +
    apprStr
  );
end;

function Finalize: integer;
var
  fname: string;
begin
  fname := ProgramPath + 'WEAP_Export_Base.tsv';
  AddMessage('Saving ' + IntToStr(sl.Count - 1) + ' WEAP records to: ' + fname);
  sl.SaveToFile(fname);
  sl.Free;
  Result := 0;
end;

end.
