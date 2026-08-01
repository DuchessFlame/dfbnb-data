{
  !!!Wordpress - ExportNukaColaLocationsToTSV.pas

  NUKA COLA spawn export — every placed reference that can ever yield each Nuka-Cola
  flavour, for the "{Type} Locations" pages on buffsnbrew.com.

  WHY THIS EXISTS
  ---------------
  The old pipeline read the Mappalachia Position table for the flavour's own leveled item
  only, so it missed anything dispensed through a nested leveled list, a container, a vending
  machine or a dispenser. This script does the proper thing: for each flavour it starts from
  the flavour's seed record(s) (the LPI_Drink_NukaCola_<flavour> leveled item, and the
  Nukashine dispenser LLs) and walks UP the ReferencedBy tree — LVLI -> LVLI -> CONT/ACTI/
  FURN/NPC_/… — collecting EVERY placed REFR whose base can ultimately give that flavour.
  This is the "widest" walk: it includes generic loot lists and vendor pools, not just fixed
  spots. Cross-referencing to region/marker happens later in src (Mappalachia), not here.

  HOW TO RUN
  ----------
  In FO76Edit: load SeventySix.esm (+ any masters), then Apply Script on the whole file
  (right-click the file -> Apply Script) or select the ALCH + LVLI + CONT + ACTI groups.
  The script only acts on records whose FormID is in the seed map below, so a whole-file
  Apply Script is fine (just slower). Choose LIVE or PTS at the prompt.

  OUTPUT (channel prompt at start, saved straight into the repo like the other scripts)
    LIVE -> dfbnb-data\tsv\      NukaCola_CollectableLocations_<mmmm_yyyy>.tsv
    PTS  -> dfbnb-data\tsv\pts\  NukaCola_CollectableLocations_PTS_<yyyy-mm-dd_hhnn>.tsv

  COLUMNS (superset of the collectables export so crossref logic still applies)
    set  item_formid  edid  name  section  worldspace  cell  x  y  z
    ref_formid  holder  holder_type  loc_name  loc_source
      set          flavour slug (nuka-cola-cherry, …)
      item_formid  the SEED record this REFR was reached from (provenance)
      edid/name    seed EditorID / FULL name
      section      seed signature (LVLI / ALCH)
      holder       EditorID of the base object the REFR actually places (the CONT/ACTI/…)
      holder_type  signature of that placed base (CONT / ACTI / FURN / NPC_ / STAT / LVLI …)
      loc_name     interior CELL FULL name, else worldspace name (region resolution)
      worldspace/cell/x/y/z/ref_formid  placement geometry + unique REFR id

  SEED MAP — the ONLY thing to edit when a flavour's FormIDs change. Decimal, load-order
  FormIDs (top byte 00 = SeventySix.esm), matching src/nuka_cola_spawns_config.py. To widen
  a flavour further, add its drink ALCH FormID as a seed here too.

  Modelled on ExportMISC2ToCSV.pas (same channel prompt, OwningPlacement, ReferencedBy walk),
  with a deeper recursion and a per-flavour seed map instead of the MISC set filter.
}

unit UserScript;

const
  MAX_DEPTH = 12;   // deep enough for nested leveled-list -> container chains

var
  gChannel  : string;
  gBasePath : string;
  gAbort    : boolean;
  slData    : TStringList;   // finished tab-delimited rows
  slVisited : TStringList;   // FormIDs walked this seed (cycle guard)
  slEmitted : TStringList;   // set|ref already emitted (dup guard across seeds)
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

// --------------------------
// Membership — flavour slug for a seed FormID (load-order, decimal), or '' for none.
// Mirrors src/nuka_cola_spawns_config.py. Add a flavour's drink ALCH FormID here to widen it.
// --------------------------
function SetForFormID(fid: cardinal): string;
begin
  Result := '';
  case fid of
    3834214, 295773:            Result := 'nuka-cola';            // base + machine-feed LLs
    4629914:                    Result := 'nuka-cola-cherry';
    5875111:                    Result := 'nuka-cola-cranberry';
    2163089:                    Result := 'nuka-cola-dark';
    2162567:                    Result := 'nuka-cola-grape';
    2162568:                    Result := 'nuka-cola-orange';
    5417048:                    Result := 'nuka-cola-quantum';
    2163091:                    Result := 'nuka-cola-wild';
    4642683, 4648160, 4112839:  Result := 'nukashine';           // Nukashine dispenser LLs
    // Twist / Vaccinated / Sunset Sarsaparilla — no placed source; hand-authored pages.
  end;
end;

// A placed base we hop THROUGH going up the tree (leveled lists + things that hold inventory).
function IsHolderBase(sig: string): boolean;
begin
  Result := (sig = 'LVLI') or (sig = 'CONT') or (sig = 'ACTI') or (sig = 'FURN') or
            (sig = 'MSTT') or (sig = 'SCOL') or (sig = 'STAT') or (sig = 'NPC_') or
            (sig = 'FLOR') or (sig = 'TERM');
end;

// True when refRec is a REFR that actually places baseRec (NAME links back to it).
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
  // ReferencedByIndex returns a detached node; re-anchor before walking up.
  try r := WinningOverride(r); except end;

  px := CleanCell(GetElementEditValues(r, 'DATA\Position\X'));
  py := CleanCell(GetElementEditValues(r, 'DATA\Position\Y'));
  pz := CleanCell(GetElementEditValues(r, 'DATA\Position\Z'));

  cellRec := ParentOfSig(r, 'CELL');
  if not Assigned(cellRec) then
    try cellRec := ContainingMainRecord(GetContainer(r)); except cellRec := nil; end;

  wrldRec := nil;
  if Assigned(cellRec) then wrldRec := ParentOfSig(cellRec, 'WRLD');

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

// Emit one row for a placed REFR (placedBase = the base the REFR instantiates).
procedure EmitPlacement(refRec, placedBase: IInterface;
  setSlug, seedFid, seedEdid, seedName, seedSection: string);
var worldEdid, cellEdid, px, py, pz, locName, locSource, refFid, dupKey: string;
begin
  refFid := HexFormID8(refRec);
  dupKey := setSlug + '|' + refFid;
  if slEmitted.IndexOf(dupKey) >= 0 then Exit;
  slEmitted.Add(dupKey);

  OwningPlacement(refRec, worldEdid, cellEdid, px, py, pz, locName, locSource);
  slData.Add(
    setSlug + #9 + seedFid + #9 + seedEdid + #9 + seedName + #9 + seedSection + #9 +
    worldEdid + #9 + cellEdid + #9 + px + #9 + py + #9 + pz + #9 +
    refFid + #9 + CleanCell(SafeEditorID(placedBase)) + #9 + Signature(placedBase) + #9 +
    locName + #9 + locSource
  );
end;

// Walk ReferencedBy of target: emit direct REFR placements of target, and recurse UP through
// holder bases (LVLI/CONT/ACTI/…) until MAX_DEPTH. seed* stays fixed for provenance.
procedure CollectPlacements(target: IInterface;
  setSlug, seedFid, seedEdid, seedName, seedSection: string; depth: integer);
var i, refCount: integer; refRec: IInterface; tgtFid, sig: string;
begin
  if not Assigned(target) then Exit;
  if depth > MAX_DEPTH then Exit;
  tgtFid := HexFormID8(target);
  if slVisited.IndexOf(tgtFid) >= 0 then Exit;
  slVisited.Add(tgtFid);

  refCount := ReferencedByCount(target);
  for i := 0 to refCount - 1 do begin
    refRec := ReferencedByIndex(target, i);
    if not Assigned(refRec) then Continue;
    sig := Signature(refRec);

    if (sig = 'REFR') and IsPlacementOf(refRec, target) then
      EmitPlacement(refRec, target, setSlug, seedFid, seedEdid, seedName, seedSection)
    else if (depth < MAX_DEPTH) and IsHolderBase(sig) then
      CollectPlacements(refRec, setSlug, seedFid, seedEdid, seedName, seedSection, depth + 1);
  end;
end;

// --------------------------
// Channel prompt (LIVE/PTS) — mirrors ExportMISC2ToCSV.pas
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
        'NukaCola_CollectableLocations_' + FormatDateTime('mmmm_yyyy', Now) + '.tsv',
        'NukaCola_CollectableLocations_PTS_' + FormatDateTime('yyyy-mm-dd_hhnn', Now) + '.tsv') then begin
    gAbort := True;
    AddMessage('[NukaCola Export] Cancelled at startup - nothing will be written.');
  end;

  slData := TStringList.Create;
  slVisited := TStringList.Create;
  slEmitted := TStringList.Create;
  slData.Add('set' + #9 + 'item_formid' + #9 + 'edid' + #9 + 'name' + #9 + 'section' + #9 +
             'worldspace' + #9 + 'cell' + #9 + 'x' + #9 + 'y' + #9 + 'z' + #9 +
             'ref_formid' + #9 + 'holder' + #9 + 'holder_type' + #9 + 'loc_name' + #9 + 'loc_source');

  SaveDialog := TSaveDialog.Create(nil);
  SaveDialog.Options := SaveDialog.Options + [ofOverwritePrompt];
  SaveDialog.FileName := gBasePath;
  SaveDialog.Filter := 'TSV files (*.tsv)|*.tsv|All files (*.*)|*.*';
  SaveDialog.Title := 'Save NukaCola Export TSV';
end;

function Process(e: IInterface): integer;
var setSlug, seedFid, seedEdid, seedName, seedSection: string;
begin
  Result := 0;
  if gAbort then Exit;
  if not Assigned(e) then Exit;

  setSlug := SetForFormID(GetLoadOrderFormID(e));
  if setSlug = '' then Exit;   // not a Nuka seed record

  seedFid     := HexFormID8(e);
  seedEdid    := SafeEditorID(e);
  seedName    := CleanCell(GetElementEditValues(e, 'FULL'));
  seedSection := Signature(e);

  slVisited.Clear;   // fresh cycle guard per seed; slEmitted persists (cross-seed dup guard)
  CollectPlacements(e, setSlug, seedFid, seedEdid, seedName, seedSection, 0);
end;

function Finalize: integer;
begin
  Result := 0;
  if gAbort then begin
    if Assigned(slData) then slData.Free;
    if Assigned(slVisited) then slVisited.Free;
    if Assigned(slEmitted) then slEmitted.Free;
    if Assigned(SaveDialog) then SaveDialog.Free;
    Exit;
  end;

  if slData.Count <= 1 then begin
    AddMessage('[NukaCola Export] No Nuka seed placements found. '
             + 'Apply the script to SeventySix.esm (or the ALCH/LVLI/CONT groups) and re-run.');
    slData.Free; slVisited.Free; slEmitted.Free; SaveDialog.Free;
    Exit;
  end;

  slData.SaveToFile(SaveDialog.FileName);
  AddMessage('[NukaCola Export] Wrote: ' + SaveDialog.FileName);
  AddMessage(Format('[NukaCola Export] Placement rows: %d', [slData.Count - 1]));

  slData.Free; slVisited.Free; slEmitted.Free; SaveDialog.Free;
end;

end.
