{
  ExportPERKToTSV.pas
  ────────────────────
  xEdit script — exports PERK records to a single TSV file with one
  row per Effect entry.  Restores the entry-point data columns that
  were present in the March 2026 format and dropped in the April
  refactor.

  Columns produced (122 total):
    PERK_FormID · PERK_EDID · FULL · DESC · ICON · FNAM_SpriteName
    DATA_Playable · DATA_Hidden · DATA_Unknown
    Refs_Flat · Ref_1 … Ref_30
    EffectCount · EffectIndex
    PRKE_Type · PRKE_Rank · PRKE_Unknown
    EP_EntryPoint · EP_Function · EP_PerkCondTabCount · EP_Unknown
    PerkConditions_Flat · Cond_1 … Cond_30
    EPFT_Type · EPFB_PerkEntryID · EPFD_Float
    EPF2_UnknownSpellValue · EPF4_ActorValue2
    CurveTable_FormID · CurveTable_EDID · CurveTable_FULL
    Spell_FormID · Spell_EDID · Spell_FULL
    EffectLinks_Flat · EffectLink_1 … EffectLink_30

  Usage
  ─────
    1.  Open SeventySix.esm in FO76Edit.
    2.  Expand PERK group → select all PERK records (or the group
        node itself to export every record).
    3.  Right-click → Apply Script → choose ExportPERKToTSV.
    4.  Output: <xEdit folder>\PERK_Export.tsv
        Rename with the month tag before committing to dfbnb-data.
}
unit ExportPERKToTSV;

const
  TAB       = #9;
  MAX_REFS  = 30;
  MAX_CONDS = 30;
  MAX_LINKS = 30;

var
  slOutput: TStringList;
  recCount: Integer;

// ─── helpers ────────────────────────────────────────────────────

function Q(const s: string): string;
// Wrap a value in double-quotes, escaping any embedded quotes.
begin
  Result := '"' + StringReplace(s, '"', '""', [rfReplaceAll]) + '"';
end;

function FID(e: IInterface): string;
// 8-hex FormID without load-order prefix.
begin
  if not Assigned(e) then
    Result := ''
  else
    Result := IntToHex(FormID(e) and $00FFFFFF, 8);
end;

function GEV(e: IInterface; const path: string): string;
// Safe GetEditValue via path.
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
// GetEditValue on the element itself.
begin
  if Assigned(e) then
    Result := GetEditValue(e)
  else
    Result := '';
end;

// ─── Referenced-By helpers ──────────────────────────────────────

procedure CollectRefs(rec: IInterface; var flat: string;
  var refs: array of string; maxRefs: Integer);
var
  i, cnt: Integer;
  refRec: IInterface;
  s: string;
begin
  flat := '';
  for i := 0 to maxRefs - 1 do
    refs[i] := '';

  cnt := ReferencedByCount(rec);
  for i := 0 to cnt - 1 do begin
    refRec := ReferencedByIndex(rec, i);
    s := FID(refRec) + ':' + EditorID(refRec) + ':' + Signature(refRec);
    if i < maxRefs then
      refs[i] := s;
    if flat <> '' then flat := flat + ' | ';
    flat := flat + s;
  end;
end;

// ─── Condition helpers ──────────────────────────────────────────

procedure CollectConditions(condContainer: IInterface;
  var flat: string; var conds: array of string; maxConds: Integer);
var
  i, cnt: Integer;
  cond, ctda: IInterface;
  s: string;
begin
  flat := '';
  for i := 0 to maxConds - 1 do
    conds[i] := '';

  if not Assigned(condContainer) then Exit;
  cnt := ElementCount(condContainer);

  for i := 0 to cnt - 1 do begin
    cond := ElementByIndex(condContainer, i);
    // Each Perk Condition contains a CTDA subrecord
    ctda := ElementBySignature(cond, 'CTDA');
    if not Assigned(ctda) then
      ctda := cond;  // fallback — treat the element itself as the condition

    s := GEVDirect(ctda);
    if s = '' then
      s := GEVDirect(cond);

    if i < maxConds then
      conds[i] := s;
    if flat <> '' then flat := flat + ' | ';
    flat := flat + s;
  end;
end;

// ─── Effect-link helpers ────────────────────────────────────────
// Walk conditions + EPFD and collect every FormID reference found
// (signature:EDID[formid] style used in March format).

function RefTag(linked: IInterface): string;
// Build a "SIG:EDID[FormID]" tag from a linked record.
begin
  if not Assigned(linked) then begin
    Result := '';
    Exit;
  end;
  Result := Signature(linked) + ':' + EditorID(linked)
          + '[' + IntToHex(FormID(linked) and $00FFFFFF, 8) + ']';
end;

procedure CollectEffectLinks(effect: IInterface;
  var flat: string; var links: array of string; maxLinks: Integer);
var
  condContainer, cond, ctda, el, linked: IInterface;
  i, cnt, linkIdx: Integer;
  tag: string;
  seen: TStringList;

  procedure AddLink(const t: string);
  begin
    if t = '' then Exit;
    // Fill individual EffectLink columns (may repeat)
    if linkIdx < maxLinks then
      links[linkIdx] := t;
    Inc(linkIdx);
    if seen.IndexOf(t) >= 0 then Exit;
    seen.Add(t);
    if flat <> '' then flat := flat + ' | ';
    flat := flat + t;
  end;

begin
  flat := '';
  linkIdx := 0;
  for i := 0 to maxLinks - 1 do
    links[i] := '';

  seen := TStringList.Create;
  try
    // Walk conditions for linked references
    condContainer := ElementByName(effect, 'Perk Conditions');
    if Assigned(condContainer) then begin
      cnt := ElementCount(condContainer);
      for i := 0 to cnt - 1 do begin
        cond := ElementByIndex(condContainer, i);
        ctda := ElementBySignature(cond, 'CTDA');
        if not Assigned(ctda) then
          ctda := cond;
        // Parameter 1 and Parameter 2 may reference forms
        el := ElementByName(ctda, 'Parameter #1');
        if Assigned(el) then begin
          linked := LinksTo(el);
          tag := RefTag(linked);
          AddLink(tag);
        end;
        el := ElementByName(ctda, 'Parameter #2');
        if Assigned(el) then begin
          linked := LinksTo(el);
          tag := RefTag(linked);
          AddLink(tag);
        end;
      end;
    end;

    // EPFD may reference a form (Spell, LeveledItem, Activator, etc.)
    el := ElementBySignature(effect, 'EPFD');
    if Assigned(el) then begin
      linked := LinksTo(el);
      tag := RefTag(linked);
      AddLink(tag);
    end;
  finally
    seen.Free;
  end;
end;

// ─── Resolve EPFT descriptive name ─────────────────────────────

function EPFTName(effect: IInterface): string;
// Return the human-readable EPFT type name that matches the
// March 2026 format values: Float, Spell Item,
// Actor Value and Value, Activate Choice, Leveled List, Item, etc.
var
  el: IInterface;
begin
  el := ElementBySignature(effect, 'EPFT');
  if Assigned(el) then
    Result := GEVDirect(el)
  else
    Result := '';
end;

// ─── Resolve EPFD value ────────────────────────────────────────

function EPFDFloat(effect: IInterface): string;
// For Float-type entry points, return the float string.
// For other types, return the raw edit value.
var
  el: IInterface;
begin
  el := ElementBySignature(effect, 'EPFD');
  if Assigned(el) then
    Result := GEVDirect(el)
  else
    Result := '';
end;

// ─── Resolve linked records (CurveTable / Spell) ──────────────

procedure ResolveCurveTable(effect: IInterface;
  var ctFID, ctEDID, ctFULL: string);
// EPFD may link to a CURV record when EPFT is Float+CurveTable.
// This walks the EPFD reference chain.
var
  el, linked: IInterface;
begin
  ctFID := ''; ctEDID := ''; ctFULL := '';
  el := ElementBySignature(effect, 'EPFD');
  if not Assigned(el) then Exit;
  linked := LinksTo(el);
  if not Assigned(linked) then Exit;
  if Signature(linked) <> 'CURV' then Exit;
  ctFID  := FID(linked);
  ctEDID := EditorID(linked);
  ctFULL := GEV(linked, 'FULL');
end;

procedure ResolveSpell(effect: IInterface;
  var spFID, spEDID, spFULL: string);
// For Spell Item / Ability type effects, EPFD links to a SPEL.
var
  el, linked: IInterface;
begin
  spFID := ''; spEDID := ''; spFULL := '';
  // Try EPFD first
  el := ElementBySignature(effect, 'EPFD');
  if Assigned(el) then begin
    linked := LinksTo(el);
    if Assigned(linked) and (Signature(linked) = 'SPEL') then begin
      spFID  := FID(linked);
      spEDID := EditorID(linked);
      spFULL := GEV(linked, 'FULL');
      Exit;
    end;
  end;
  // For Ability-type effects, the DATA subrecord itself is a SPEL ref
  el := ElementByPath(effect, 'DATA');
  if Assigned(el) then begin
    linked := LinksTo(el);
    if Assigned(linked) and (Signature(linked) = 'SPEL') then begin
      spFID  := FID(linked);
      spEDID := EditorID(linked);
      spFULL := GEV(linked, 'FULL');
    end;
  end;
end;

// ═══════════════════════════════════════════════════════════════
//  Initialize — build header row
// ═══════════════════════════════════════════════════════════════

function Initialize: Integer;
var
  hdr: string;
  i: Integer;
begin
  slOutput := TStringList.Create;
  recCount := 0;

  hdr := 'PERK_FormID'     + TAB
       + 'PERK_EDID'       + TAB
       + 'FULL'            + TAB
       + 'DESC'            + TAB
       + 'ICON'            + TAB
       + 'FNAM_SpriteName' + TAB
       + 'DATA_Playable'   + TAB
       + 'DATA_Hidden'     + TAB
       + 'DATA_Unknown'    + TAB
       + 'Refs_Flat';

  for i := 1 to MAX_REFS do
    hdr := hdr + TAB + 'Ref_' + IntToStr(i);

  hdr := hdr
       + TAB + 'EffectCount'
       + TAB + 'EffectIndex'
       + TAB + 'PRKE_Type'
       + TAB + 'PRKE_Rank'
       + TAB + 'PRKE_Unknown'
       + TAB + 'EP_EntryPoint'
       + TAB + 'EP_Function'
       + TAB + 'EP_PerkCondTabCount'
       + TAB + 'EP_Unknown'
       + TAB + 'PerkConditions_Flat';

  for i := 1 to MAX_CONDS do
    hdr := hdr + TAB + 'Cond_' + IntToStr(i);

  hdr := hdr
       + TAB + 'EPFT_Type'
       + TAB + 'EPFB_PerkEntryID'
       + TAB + 'EPFD_Float'
       + TAB + 'EPF2_UnknownSpellValue'
       + TAB + 'EPF4_ActorValue2'
       + TAB + 'CurveTable_FormID'
       + TAB + 'CurveTable_EDID'
       + TAB + 'CurveTable_FULL'
       + TAB + 'Spell_FormID'
       + TAB + 'Spell_EDID'
       + TAB + 'Spell_FULL'
       + TAB + 'EffectLinks_Flat';

  for i := 1 to MAX_LINKS do
    hdr := hdr + TAB + 'EffectLink_' + IntToStr(i);

  slOutput.Add(hdr);
  Result := 0;
end;

// ═══════════════════════════════════════════════════════════════
//  Process — called once per selected record
// ═══════════════════════════════════════════════════════════════

function Process(e: IInterface): Integer;
var
  effects, effect, prke, epData, condContainer: IInterface;
  elEPFB, elEPF2: IInterface;
  i, j, effCount: Integer;
  // PERK header
  perkFID, perkEDID, perkFULL, perkDESC, perkICON, perkFNAM: string;
  dataPlayable, dataHidden, dataUnk: string;
  // Refs
  refsFlat: string;
  refs: array [0..29] of string;
  // Effect fields
  prkeType, prkeRank, prkeUnk: string;
  epEntryPoint, epFunction, epTabCount, epUnknown: string;
  condFlat: string;
  conds: array [0..29] of string;
  epftType, epfbID, epfdVal, epf2Val, epf4Val: string;
  ctFID, ctEDID, ctFULL: string;
  spFID, spEDID, spFULL: string;
  linksFlat: string;
  effLinks: array [0..29] of string;
  row: string;
begin
  Result := 0;
  if Signature(e) <> 'PERK' then Exit;
  Inc(recCount);

  // ── PERK-level fields ────────────────────────────────────────
  perkFID  := FID(e);
  perkEDID := EditorID(e);
  perkFULL := GEV(e, 'FULL');
  perkDESC := GEV(e, 'DESC');
  perkICON := GEV(e, 'ICON');
  perkFNAM := GEV(e, 'FNAM');

  dataPlayable := GEV(e, 'DATA\Trait');
  dataHidden   := GEV(e, 'DATA\Hidden');
  dataUnk      := GEV(e, 'DATA\Unknown');

  // ── Referenced-By ────────────────────────────────────────────
  CollectRefs(e, refsFlat, refs, MAX_REFS);

  // ── Effects ──────────────────────────────────────────────────
  effects := ElementByName(e, 'Effects');
  if Assigned(effects) then
    effCount := ElementCount(effects)
  else
    effCount := 0;

  // If no effects, emit one row with empty effect columns
  if effCount = 0 then begin
    row := perkFID                + TAB
         + Q(perkEDID)           + TAB
         + Q(perkFULL)           + TAB
         + Q(perkDESC)           + TAB
         + Q(perkICON)           + TAB
         + Q(perkFNAM)           + TAB
         + Q(dataPlayable)       + TAB
         + Q(dataHidden)         + TAB
         + Q(dataUnk)            + TAB
         + Q(refsFlat);
    for i := 0 to MAX_REFS - 1 do
      row := row + TAB + Q(refs[i]);
    row := row + TAB + '0';  // EffectCount
    // Remaining 81 columns empty (EffectIndex…EffectLink_30)
    for i := 1 to (9 + MAX_CONDS + 12 + MAX_LINKS) do
      row := row + TAB;
    slOutput.Add(row);
    Exit;
  end;

  // One row per effect
  for j := 0 to effCount - 1 do begin
    effect := ElementByIndex(effects, j);

    // ── PRKE header ────────────────────────────────────────────
    prke := ElementBySignature(effect, 'PRKE');
    prkeType := ''; prkeRank := ''; prkeUnk := '';
    if Assigned(prke) then begin
      prkeType := GEV(prke, 'Type');
      prkeRank := GEV(prke, 'Rank');
      prkeUnk  := GEV(prke, 'Priority');
      if prkeUnk = '' then
        prkeUnk := GEV(prke, 'Unknown');
    end;

    // ── Entry Point DATA ───────────────────────────────────────
    epEntryPoint := ''; epFunction := ''; epTabCount := ''; epUnknown := '';
    epData := ElementByPath(effect, 'DATA');
    if Assigned(epData) then begin
      epEntryPoint := GEV(epData, 'Entry Point');
      epFunction   := GEV(epData, 'Function');
      epTabCount   := GEV(epData, 'Perk Condition Tab Count');
      epUnknown    := GEV(epData, 'Unknown');
      // Some structures use 'Entry Point\Entry Point' nesting
      if epEntryPoint = '' then
        epEntryPoint := GEV(epData, 'Entry Point\Entry Point');
      if epFunction = '' then
        epFunction := GEV(epData, 'Entry Point\Function');
    end;

    // ── Perk Conditions ────────────────────────────────────────
    condContainer := ElementByName(effect, 'Perk Conditions');
    CollectConditions(condContainer, condFlat, conds, MAX_CONDS);

    // ── EPFT / EPFB / EPFD / EPF2 ─────────────────────────────
    epftType := EPFTName(effect);
    epfdVal  := EPFDFloat(effect);

    elEPFB := ElementBySignature(effect, 'EPF3');
    if not Assigned(elEPFB) then
      elEPFB := ElementBySignature(effect, 'EPFB');
    if Assigned(elEPFB) then
      epfbID := GEVDirect(elEPFB)
    else
      epfbID := '';

    elEPF2 := ElementBySignature(effect, 'EPF2');
    if Assigned(elEPF2) then
      epf2Val := GEVDirect(elEPF2)
    else
      epf2Val := '';

    // EPF4 / Actor Value 2 — some effects store a second AV
    epf4Val := GEV(effect, 'EPFD\Actor Value 2');
    if epf4Val = '' then
      epf4Val := GEV(effect, 'EPF4');

    // ── CurveTable / Spell references ──────────────────────────
    ResolveCurveTable(effect, ctFID, ctEDID, ctFULL);
    ResolveSpell(effect, spFID, spEDID, spFULL);

    // ── Effect Links ───────────────────────────────────────────
    CollectEffectLinks(effect, linksFlat, effLinks, MAX_LINKS);

    // ── Build row ──────────────────────────────────────────────
    row := perkFID                    + TAB
         + Q(perkEDID)               + TAB
         + Q(perkFULL)               + TAB
         + Q(perkDESC)               + TAB
         + Q(perkICON)               + TAB
         + Q(perkFNAM)               + TAB
         + Q(dataPlayable)           + TAB
         + Q(dataHidden)             + TAB
         + Q(dataUnk)                + TAB
         + Q(refsFlat);

    for i := 0 to MAX_REFS - 1 do
      row := row + TAB + Q(refs[i]);

    row := row
         + TAB + IntToStr(effCount)
         + TAB + IntToStr(j)
         + TAB + Q(prkeType)
         + TAB + Q(prkeRank)
         + TAB + Q(prkeUnk)
         + TAB + Q(epEntryPoint)
         + TAB + Q(epFunction)
         + TAB + Q(epTabCount)
         + TAB + Q(epUnknown)
         + TAB + Q(condFlat);

    for i := 0 to MAX_CONDS - 1 do
      row := row + TAB + Q(conds[i]);

    row := row
         + TAB + Q(epftType)
         + TAB + Q(epfbID)
         + TAB + Q(epfdVal)
         + TAB + Q(epf2Val)
         + TAB + Q(epf4Val)
         + TAB + ctFID
         + TAB + Q(ctEDID)
         + TAB + Q(ctFULL)
         + TAB + spFID
         + TAB + Q(spEDID)
         + TAB + Q(spFULL)
         + TAB + Q(linksFlat);

    for i := 0 to MAX_LINKS - 1 do
      row := row + TAB + Q(effLinks[i]);

    slOutput.Add(row);
  end;
end;

// ═══════════════════════════════════════════════════════════════
//  Finalize — save file
// ═══════════════════════════════════════════════════════════════

function Finalize: Integer;
var
  outPath: string;
  rowCount: Integer;
begin
  outPath  := ProgramPath + 'PERK_Export.tsv';
  rowCount := slOutput.Count - 1;  // subtract header row
  AddMessage('Saving ' + IntToStr(recCount) + ' PERK records ('
           + IntToStr(rowCount) + ' data rows) -> ' + outPath);
  slOutput.SaveToFile(outPath);
  slOutput.Free;
  AddMessage('Done.');
  Result := 0;
end;

end.
