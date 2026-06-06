{
  NPC_ Full Export Script for xEdit
  Exports ALL NPC_ records from the selected file to TSV.

  Produces 3 files in the script's Data folder:
    1. NPC_Export_Main.tsv        — one row per NPC (flat fields + DNAM)
    2. NPC_Export_PRPS.tsv        — one row per PRPS property per NPC
    3. NPC_Export_Refs.tsv         — one row per referenced-by record per NPC

  Usage: Select SeventySix.esm in the left panel, right-click > Apply Script.
}
unit NPC_Export_xEdit;

var
  slMain, slPRPS, slRefs: TStringList;

function Initialize: integer;
begin
  slMain := TStringList.Create;
  slPRPS := TStringList.Create;
  slRefs := TStringList.Create;

  // Main header
  slMain.Add(
    'FormID' + #9 +
    'EDID' + #9 +
    'FULL' + #9 +
    'SHRT' + #9 +
    'RNAM_FormID' + #9 +
    'RNAM_Name' + #9 +
    'ATKR_FormID' + #9 +
    'ATKR_Name' + #9 +
    'INAM_FormID' + #9 +
    'INAM_EDID' + #9 +
    'TPLT_FormID' + #9 +
    'TPLT_Name' + #9 +
    'LTPT_FormID' + #9 +
    'LTPT_Name' + #9 +
    'LTPC_FormID' + #9 +
    'LTPC_Name' + #9 +
    'CNAM_FormID' + #9 +
    'CNAM_Name' + #9 +
    'VTCK_FormID' + #9 +
    'VTCK_Name' + #9 +
    'WNAM_FormID' + #9 +
    'WNAM_EDID' + #9 +
    'DOFT_FormID' + #9 +
    'DOFT_EDID' + #9 +
    'ACBS_Flags' + #9 +
    'ACBS_XPOffset' + #9 +
    'ACBS_Level' + #9 +
    'ACBS_CalcMinLvl' + #9 +
    'ACBS_CalcMaxLvl' + #9 +
    'ACBS_DispositionBase' + #9 +
    'ACBS_TemplateFlags' + #9 +
    'AJNG_LvlMinGlob' + #9 +
    'AJXG_LvlMaxGlob' + #9 +
    'DNAM_CalcHealth' + #9 +
    'DNAM_CalcActionPts' + #9 +
    'DNAM_FarAwayDist' + #9 +
    'DNAM_GearedUpWeapons' + #9 +
    'ZNAM_FormID' + #9 +
    'ZNAM_EDID' + #9 +
    'ECOR_FormID' + #9 +
    'ECOR_EDID'
  );

  // PRPS header
  slPRPS.Add(
    'NPC_FormID' + #9 +
    'NPC_EDID' + #9 +
    'ActorValue_FormID' + #9 +
    'ActorValue_Name' + #9 +
    'Value' + #9 +
    'CurveTable_FormID' + #9 +
    'CurveTable_EDID'
  );

  // Refs header
  slRefs.Add(
    'NPC_FormID' + #9 +
    'NPC_EDID' + #9 +
    'RefBy_FormID' + #9 +
    'RefBy_EDID' + #9 +
    'RefBy_Signature'
  );

  Result := 0;
end;

// ── Helpers ──────────────────────────────────────────────────────────────

function HexFormID(e: IInterface): string;
begin
  if not Assigned(e) then
    Result := ''
  else
    Result := IntToHex(FormID(e), 8);
end;

function SafeEDID(e: IInterface): string;
begin
  if not Assigned(e) then
    Result := ''
  else
    Result := EditorID(e);
end;

function SafeFULL(e: IInterface): string;
begin
  if not Assigned(e) then
    Result := ''
  else
    Result := GetElementEditValues(e, 'FULL');
end;

function LinkedFormID(rec: IInterface; sig: string): string;
var
  el, linked: IInterface;
begin
  Result := '';
  el := ElementBySignature(rec, sig);
  if not Assigned(el) then Exit;
  linked := LinksTo(el);
  if Assigned(linked) then
    Result := IntToHex(FormID(linked), 8);
end;

function LinkedEDID(rec: IInterface; sig: string): string;
var
  el, linked: IInterface;
begin
  Result := '';
  el := ElementBySignature(rec, sig);
  if not Assigned(el) then Exit;
  linked := LinksTo(el);
  if Assigned(linked) then
    Result := EditorID(linked);
end;

function LinkedName(rec: IInterface; sig: string): string;
var
  el, linked: IInterface;
  s: string;
begin
  Result := '';
  el := ElementBySignature(rec, sig);
  if not Assigned(el) then Exit;
  linked := LinksTo(el);
  if not Assigned(linked) then Exit;
  s := GetElementEditValues(linked, 'FULL');
  if s = '' then
    s := EditorID(linked);
  Result := s;
end;

function GetSubVal(parent: IInterface; path: string): string;
begin
  Result := '';
  if Assigned(parent) then
    Result := GetElementEditValues(parent, path);
end;

// ── Process each NPC_ record ─────────────────────────────────────────────

function Process(e: IInterface): integer;
var
  sig, fid, edid, full, shrt: string;
  acbs, dnam, prps, prop, scaling: IInterface;
  avEl, cvEl, linked: IInterface;
  i, refCount: integer;
  refRec: IInterface;
  sRow: string;
begin
  Result := 0;
  sig := Signature(e);
  if sig <> 'NPC_' then Exit;

  fid := IntToHex(FormID(e), 8);
  edid := EditorID(e);
  full := GetElementEditValues(e, 'FULL');
  shrt := GetElementEditValues(e, 'SHRT');

  // ── ACBS ──
  acbs := ElementBySignature(e, 'ACBS');

  // ── Actor Scaling Info (AJNG / AJXG) ──
  scaling := ElementByPath(e, 'Actor Scaling Info');

  // ── DNAM ──
  dnam := ElementBySignature(e, 'DNAM');

  // ── Main row ──
  sRow :=
    fid + #9 +
    edid + #9 +
    full + #9 +
    shrt + #9 +
    LinkedFormID(e, 'RNAM') + #9 +
    LinkedName(e, 'RNAM') + #9 +
    LinkedFormID(e, 'ATKR') + #9 +
    LinkedName(e, 'ATKR') + #9 +
    LinkedFormID(e, 'INAM') + #9 +
    LinkedEDID(e, 'INAM') + #9 +
    LinkedFormID(e, 'TPLT') + #9 +
    LinkedName(e, 'TPLT') + #9 +
    LinkedFormID(e, 'LTPT') + #9 +
    LinkedName(e, 'LTPT') + #9 +
    LinkedFormID(e, 'LTPC') + #9 +
    LinkedName(e, 'LTPC') + #9 +
    LinkedFormID(e, 'CNAM') + #9 +
    LinkedName(e, 'CNAM') + #9 +
    LinkedFormID(e, 'VTCK') + #9 +
    LinkedName(e, 'VTCK') + #9 +
    LinkedFormID(e, 'WNAM') + #9 +
    LinkedEDID(e, 'WNAM') + #9 +
    LinkedFormID(e, 'DOFT') + #9 +
    LinkedEDID(e, 'DOFT') + #9 +
    GetSubVal(acbs, 'Flags (sorted)') + #9 +
    GetSubVal(acbs, 'XP Value Offset') + #9 +
    GetSubVal(acbs, 'Level') + #9 +
    GetSubVal(acbs, 'Calc min level') + #9 +
    GetSubVal(acbs, 'Calc max level') + #9 +
    GetSubVal(acbs, 'Disposition Base') + #9 +
    GetSubVal(acbs, 'Template Flags (sorted)') + #9 +
    LinkedEDID(scaling, 'AJNG - Level Min Global') + #9 +
    LinkedEDID(scaling, 'AJXG - Level Max Global') + #9 +
    GetSubVal(dnam, 'Calculated Health') + #9 +
    GetSubVal(dnam, 'Calculated Action Points') + #9 +
    GetSubVal(dnam, 'Far Away Model Distance') + #9 +
    GetSubVal(dnam, 'Geared Up Weapons') + #9 +
    LinkedFormID(e, 'ZNAM') + #9 +
    LinkedEDID(e, 'ZNAM') + #9 +
    LinkedFormID(e, 'ECOR') + #9 +
    LinkedEDID(e, 'ECOR');

  slMain.Add(sRow);

  // ── PRPS ──
  prps := ElementByPath(e, 'PRPS');
  if Assigned(prps) then begin
    for i := 0 to ElementCount(prps) - 1 do begin
      prop := ElementByIndex(prps, i);
      if not Assigned(prop) then Continue;

      // Actor Value
      avEl := ElementByPath(prop, 'Actor Value');
      linked := nil;
      if Assigned(avEl) then
        linked := LinksTo(avEl);

      // Curve Table
      cvEl := ElementByPath(prop, 'Curve Table');

      slPRPS.Add(
        fid + #9 +
        edid + #9 +
        HexFormID(linked) + #9 +
        SafeEDID(linked) + #9 +
        GetSubVal(prop, 'Value') + #9 +
        GetElementEditValues(prop, 'Curve Table') + #9 +
        LinkedEDID(prop, 'Curve Table')
      );
    end;
  end;

  // ── Refs ──
  refCount := ReferencedByCount(e);
  for i := 0 to refCount - 1 do begin
    refRec := ReferencedByIndex(e, i);
    if not Assigned(refRec) then Continue;
    slRefs.Add(
      fid + #9 +
      edid + #9 +
      IntToHex(FormID(refRec), 8) + #9 +
      EditorID(refRec) + #9 +
      Signature(refRec)
    );
  end;
end;

// ── Finalize — write files ───────────────────────────────────────────────

function Finalize: integer;
var
  path: string;
begin
  path := ProgramPath + 'Data\';

  slMain.SaveToFile(path + 'NPC_Export_Main.tsv');
  AddMessage('Wrote ' + IntToStr(slMain.Count - 1) + ' NPCs to NPC_Export_Main.tsv');

  slPRPS.SaveToFile(path + 'NPC_Export_PRPS.tsv');
  AddMessage('Wrote ' + IntToStr(slPRPS.Count - 1) + ' properties to NPC_Export_PRPS.tsv');

  slRefs.SaveToFile(path + 'NPC_Export_Refs.tsv');
  AddMessage('Wrote ' + IntToStr(slRefs.Count - 1) + ' refs to NPC_Export_Refs.tsv');

  slMain.Free;
  slPRPS.Free;
  slRefs.Free;

  Result := 0;
end;

end.
