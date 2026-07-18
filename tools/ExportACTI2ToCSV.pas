{
  !!!Wordpress - ExportACTI2ToCSV.pas

  SECOND ACTI pass — placement locations for the Treasure Map dig-site page (and any other
  activator-based spawn set).

  Run this on the SAME ACTI selection you run "!!!Wordpress - ExportACTIToCSV.pas" on.
  Pass 1 pulls the normal activator info; pass 2 (this one) pulls the DETAILED placement
  references for the target activator SETS below — one row per placed REFR with worldspace,
  cell, X/Y/Z and the game location name — so src/crossref_mappalachia_markers.py can resolve
  a nearest map marker + region for each dig site.

  Currently exports the 35 Treasure Map mounds (TreasureMapMoundActivator1..35). The TEMP /
  DEBUG variants and the unrelated Ash/Floodlands loot mounds are skipped.

  Output (channel prompt at start, saved straight into the repo):
    LIVE -> dfbnb-data\tsv\      ACTI2_Export_<mmmm_yyyy>.tsv
    PTS  -> dfbnb-data\tsv\pts\  ACTI2_Export_PTS_<yyyy-mm-dd_hhnn>.tsv

  Columns:
    set  item_formid  edid  name  section  worldspace  cell  x  y  z  ref_formid  holder  loc_name  loc_source

  Matches conventions of ExportMISC2ToCSV.pas / ExportACTIToCSV.pas.
}

unit UserScript;

var
  gChannel  : string;
  gBasePath : string;
  gAbort    : boolean;
  slData    : TStringList;
  slVisited : TStringList;
  SaveDialog: TSaveDialog;

// --------------------------
// Helpers
// --------------------------
function HexFormID8(e: IInterface): string;
begin
  if Assigned(e) then Result := IntToHex(GetLoadOrderFormID(e), 8) else Result := '';
end;

function SafeEditorID(e: IInterface): string;
begin
  if Assigned(e) then Result := EditorID(e) else Result := '';
end;

function CleanCell(const s: string): string;
var t: string;
begin
  t := s;
  t := StringReplace(t, #13#10, ' ', [rfReplaceAll]);
  t := StringReplace(t, #13,    ' ', [rfReplaceAll]);
  t := StringReplace(t, #10,    ' ', [rfReplaceAll]);
  t := StringReplace(t, #9,     ' ', [rfReplaceAll]);
  Result := Trim(t);
end;

function InList(hay, needle: string): boolean;
begin
  Result := Pos(needle, hay) > 0;
end;

// --------------------------
// Membership — which set an activator belongs to (or '' for none)
// --------------------------
function SetForRecord(fidHex, edidLow, nameLow: string): string;
begin
  Result := '';

  // Treasure Map dig sites — TreasureMapMoundActivator1..35 (skip TEMP/DEBUG).
  if InList(edidLow, 'treasuremapmoundactivator')
     and (not InList(edidLow, 'temp'))
     and (not InList(edidLow, 'debug')) then begin
    Result := 'treasure-maps'; Exit;
  end;

  // Other activator-based sets (e.g. Ash/Floodlands loot mounds): fill when wanted.
end;

// --------------------------
// Placement resolution  (identical machinery to ExportMISC2ToCSV.pas)
// --------------------------
function IsPlacementOf(refRec, baseRec: IInterface): boolean;
var nameEl, linked: IInterface;
begin
  Result := False;
  if Signature(refRec) <> 'REFR' then Exit;
  nameEl := ElementBySignature(refRec, 'NAME');
  if not Assigned(nameEl) then Exit;
  linked := LinksTo(nameEl);
  if not Assigned(linked) then Exit;
  Result := (FormID(linked) = FormID(baseRec));
end;

function IsHolderBase(sig: string): boolean;
begin
  Result := (sig = 'CONT') or (sig = 'ACTI') or (sig = 'FURN') or
            (sig = 'MSTT') or (sig = 'SCOL') or (sig = 'STAT') or
            (sig = 'NPC_') or (sig = 'LVLI');
end;

// Walk up the (anchored) container chain to a record of the given signature.
function ParentOfSig(e: IInterface; const sig: string): IInterface;
var c, nxt: IInterface; guard: integer; s: string;
begin
  Result := nil; c := e; guard := 0;
  while Assigned(c) and (guard < 256) do begin
    s := ''; try s := Signature(c); except s := ''; end;
    if s = sig then begin Result := c; Exit; end;
    nxt := nil; try nxt := GetContainer(c); except nxt := nil; end;
    c := nxt; Inc(guard);
  end;
end;

procedure OwningPlacement(r: IInterface;
  var worldEdid, cellEdid, px, py, pz, locName, locSource: string);
var cellRec, wrldRec: IInterface; s: string;
begin
  worldEdid := ''; cellEdid := ''; px := ''; py := ''; pz := '';
  locName := ''; locSource := '';

  // Re-anchor the detached ref (ReferencedByIndex) so the container chain is walkable.
  try r := WinningOverride(r); except end;

  px := CleanCell(GetElementEditValues(r, 'DATA\Position\X'));
  py := CleanCell(GetElementEditValues(r, 'DATA\Position\Y'));
  pz := CleanCell(GetElementEditValues(r, 'DATA\Position\Z'));

  cellRec := ParentOfSig(r, 'CELL');
  if not Assigned(cellRec) then
    try cellRec := ContainingMainRecord(GetContainer(r)); except cellRec := nil; end;

  wrldRec := nil;
  if Assigned(cellRec) then
    wrldRec := ParentOfSig(cellRec, 'WRLD');

  if Assigned(cellRec) then begin
    cellEdid := CleanCell(SafeEditorID(cellRec));
    s := Trim(GetElementEditValues(cellRec, 'FULL - Name'));
    if s = '' then s := Trim(GetElementEditValues(cellRec, 'FULL'));
    if s <> '' then begin locName := CleanCell(s); locSource := 'CellFULL'; end;
  end;

  if Assigned(wrldRec) then begin
    worldEdid := CleanCell(SafeEditorID(wrldRec));
    if locName = '' then begin
      s := Trim(GetElementEditValues(wrldRec, 'FULL - Name'));
      if s = '' then s := Trim(GetElementEditValues(wrldRec, 'FULL'));
      if s = '' then s := SafeEditorID(wrldRec);
      if s <> '' then begin locName := CleanCell(s); locSource := 'WRLDName'; end;
    end;
  end;

  if (locName = '') and (cellEdid <> '') then begin
    locName := cellEdid; locSource := 'CellEDID';
  end;
end;

procedure EmitPlacement(refRec: IInterface; setSlug, fidHex, edid, full, section, holder: string);
var worldEdid, cellEdid, px, py, pz, locName, locSource: string;
begin
  OwningPlacement(refRec, worldEdid, cellEdid, px, py, pz, locName, locSource);
  slData.Add(
    setSlug + #9 + fidHex + #9 + edid + #9 + full + #9 + section + #9 +
    worldEdid + #9 + cellEdid + #9 + px + #9 + py + #9 + pz + #9 +
    HexFormID8(refRec) + #9 + holder + #9 + locName + #9 + locSource
  );
end;

procedure CollectPlacements(target: IInterface;
  setSlug, fidHex, edid, full, section, holder: string; depth: integer);
var i, refCount: integer; refRec: IInterface; tgtFid, sig, subHolder: string;
begin
  if not Assigned(target) then Exit;
  if depth > 2 then Exit;
  tgtFid := HexFormID8(target);
  if slVisited.IndexOf(tgtFid) >= 0 then Exit;
  slVisited.Add(tgtFid);

  refCount := ReferencedByCount(target);
  for i := 0 to refCount - 1 do begin
    refRec := ReferencedByIndex(target, i);
    if not Assigned(refRec) then Continue;
    sig := Signature(refRec);
    if (sig = 'REFR') and IsPlacementOf(refRec, target) then
      EmitPlacement(refRec, setSlug, fidHex, edid, full, section, holder)
    else if (depth < 2) and IsHolderBase(sig) then begin
      if holder = '' then subHolder := SafeEditorID(refRec) else subHolder := holder;
      CollectPlacements(refRec, setSlug, fidHex, edid, full, section, subHolder, depth + 1);
    end;
  end;
end;

// --------------------------
// Channel prompt (LIVE/PTS)
// --------------------------
function _ChooseChannelPath(const liveName, ptsName: string): boolean;
var res: integer; d: TSaveDialog;
begin
  Result := False;
  res := MessageDlg('Export channel?' + #13#10 + #13#10 +
    'Yes = LIVE  (saves into tsv\)' + #13#10 +
    'No  = PTS   (saves into tsv\pts\)' + #13#10 +
    'Cancel = abort', mtConfirmation, [mbYes, mbNo, mbCancel], 0);
  if res = mrCancel then Exit;
  if res = mrYes then gChannel := 'LIVE' else gChannel := 'PTS';
  d := TSaveDialog.Create(nil);
  try
    d.Filter := 'TSV files (*.tsv)|*.tsv|All files (*.*)|*.*';
    d.Options := d.Options + [ofOverwritePrompt];
    if gChannel = 'PTS' then begin
      d.InitialDir := 'C:\Users\Duche\OneDrive\GitHub\dfbnb-data\tsv\pts\';
      d.FileName := ptsName;
      d.Title := 'PTS export - save into tsv\pts';
    end else begin
      d.InitialDir := 'C:\Users\Duche\OneDrive\GitHub\dfbnb-data\tsv\';
      d.FileName := liveName;
      d.Title := 'LIVE export - save into tsv';
    end;
    if not d.Execute then Exit;
    gBasePath := d.FileName;
  finally
    d.Free;
  end;
  Result := True;
end;

// --------------------------
// xEdit lifecycle
// --------------------------
function Initialize: integer;
begin
  Result := 0;
  gAbort := False;
  if not _ChooseChannelPath(
        'ACTI2_Export_' + FormatDateTime('mmmm_yyyy', Now) + '.tsv',
        'ACTI2_Export_PTS_' + FormatDateTime('yyyy-mm-dd_hhnn', Now) + '.tsv') then begin
    gAbort := True;
    AddMessage('[ACTI2 Export] Cancelled at startup - nothing will be written.');
  end;

  slData := TStringList.Create;
  slVisited := TStringList.Create;
  slData.Add('set' + #9 + 'item_formid' + #9 + 'edid' + #9 + 'name' + #9 +
             'section' + #9 + 'worldspace' + #9 + 'cell' + #9 +
             'x' + #9 + 'y' + #9 + 'z' + #9 + 'ref_formid' + #9 + 'holder' + #9 +
             'loc_name' + #9 + 'loc_source');

  SaveDialog := TSaveDialog.Create(nil);
  SaveDialog.Options := SaveDialog.Options + [ofOverwritePrompt];
  SaveDialog.FileName := gBasePath;
  SaveDialog.Filter := 'TSV files (*.tsv)|*.tsv|All files (*.*)|*.*';
  SaveDialog.Title := 'Save ACTI2 Export TSV';
end;

function Process(e: IInterface): integer;
var setSlug, fidHex, edid, full, section: string;
begin
  Result := 0;
  if gAbort then Exit;
  if not Assigned(e) then Exit;
  if Signature(e) <> 'ACTI' then Exit;   // second ACTI pass — ACTI records only

  fidHex := HexFormID8(e);
  edid   := SafeEditorID(e);
  full   := CleanCell(GetElementEditValues(e, 'FULL'));

  setSlug := SetForRecord(fidHex, LowerCase(edid), LowerCase(full));
  if setSlug = '' then Exit;

  section := Signature(e);
  slVisited.Clear;
  CollectPlacements(e, setSlug, fidHex, edid, full, section, '', 0);
end;

function Finalize: integer;
begin
  Result := 0;
  if gAbort then begin
    if Assigned(slData) then slData.Free;
    if Assigned(slVisited) then slVisited.Free;
    if Assigned(SaveDialog) then SaveDialog.Free;
    Exit;
  end;

  if slData.Count <= 1 then begin
    AddMessage('[ACTI2 Export] No target activator placements found. '
             + 'Select the ACTI group and run again (treasure mounds live in ACTI).');
    slData.Free; slVisited.Free; SaveDialog.Free;
    Exit;
  end;

  slData.SaveToFile(SaveDialog.FileName);
  AddMessage('[ACTI2 Export] Wrote: ' + SaveDialog.FileName);
  AddMessage(Format('[ACTI2 Export] Placement rows: %d', [slData.Count - 1]));

  slData.Free;
  slVisited.Free;
  SaveDialog.Free;
end;

end.
