{
  ExportWEAP_DNAM.pas
  xEdit script — exports WEAP DNAM combat/stats data to TSV.

  Output: WEAP_Export_{DateStamp}_DNAM.tsv
  Columns: FormID, EDID, FULL, Ammo, Speed, ReloadSpeed, NPCReloadDelay,
           Reach, ReachEngagementMult, MinRange, MaxRange, AttackDelaySeconds,
           DamageOutOfRangeMult, OnHit, Flags, Capacity, AmmoPerShot,
           WeaponType, SecondaryDamage, Weight, Value, BaseDamage,
           SoundLevel, AccuracyBonus, AnimationAttackSeconds, Rank,
           ActionPointCost, FullPowerSeconds, MinPowerPerShot, Stagger,
           Health, CritDamageMult, CritChargeBonus, CritEffect,
           Projectiles, RumblePattern, SneakAttackMultiplier

  Usage: Select [00] SeventySix.esm → Apply Script → ExportWEAP_DNAM

  Part 3 of 3 WEAP exports. See also:
    ExportWEAP_Base.pas            — core record data
    ExportWEAP_ObjectTemplate.pas  — mod slots / legendary breakdown
}

unit ExportWEAP_DNAM;

var
  sl: TStringList;

function Initialize: integer;
begin
  sl := TStringList.Create;
  sl.Add(
    'WEAP_FormID' + #9 +
    'WEAP_EDID' + #9 +
    'WEAP_FULL' + #9 +
    'DNAM_Ammo' + #9 +
    'DNAM_Speed' + #9 +
    'DNAM_ReloadSpeed' + #9 +
    'DNAM_NPCReloadDelay' + #9 +
    'DNAM_Reach' + #9 +
    'DNAM_ReachEngagementMult' + #9 +
    'DNAM_MinRange' + #9 +
    'DNAM_MaxRange' + #9 +
    'DNAM_AttackDelaySeconds' + #9 +
    'DNAM_DamageOutOfRangeMult' + #9 +
    'DNAM_OnHit' + #9 +
    'DNAM_Flags' + #9 +
    'DNAM_Capacity' + #9 +
    'DNAM_AmmoPerShot' + #9 +
    'DNAM_WeaponType' + #9 +
    'DNAM_SecondaryDamage' + #9 +
    'DNAM_Weight' + #9 +
    'DNAM_Value' + #9 +
    'DNAM_BaseDamage' + #9 +
    'DNAM_SoundLevel' + #9 +
    'DNAM_AccuracyBonus' + #9 +
    'DNAM_AnimAttackSeconds' + #9 +
    'DNAM_Rank' + #9 +
    'DNAM_ActionPointCost' + #9 +
    'DNAM_FullPowerSeconds' + #9 +
    'DNAM_MinPowerPerShot' + #9 +
    'DNAM_Stagger' + #9 +
    'DNAM_Health' + #9 +
    'CRDT_CritDamageMult' + #9 +
    'CRDT_CritChargeBonus' + #9 +
    'CRDT_CritEffect' + #9 +
    'DNAM_Projectiles' + #9 +
    'DNAM_RumblePattern' + #9 +
    'WSAM_SneakAttackMult' + #9 +
    'CVT0_DamageCurve' + #9 +
    'CVT1_MinDurabilityCurve' + #9 +
    'CVT2_CondLossScale' + #9 +
    'CVT3_BashCondLossScale' + #9 +
    'CVT4_MaxDurabilityCurve'
  );
  Result := 0;
end;

function GetDNAM(e: IInterface; field: string): string;
begin
  Result := GetElementEditValues(e, 'DNAM\' + field);
end;

function Process(e: IInterface): integer;
var
  sig: string;
  formid, edid, full: string;
  crdt: IInterface;
  critMult, critBonus, critEffect: string;
  wsam: string;
  cvt0, cvt1, cvt2, cvt3, cvt4: string;
begin
  Result := 0;
  sig := Signature(e);
  if sig <> 'WEAP' then Exit;

  formid := IntToHex(FormID(e) and $00FFFFFF, 8);
  edid   := GetElementEditValues(e, 'EDID');
  full   := GetElementEditValues(e, 'FULL');

  // Critical data
  critMult   := GetElementEditValues(e, 'CRDT\Crit Damage Mult');
  critBonus  := GetElementEditValues(e, 'CRDT\Crit Charge Bonus');
  critEffect := GetElementEditValues(e, 'CRDT\Crit Effect');

  // Sneak attack multiplier
  wsam := GetElementEditValues(e, 'WSAM');

  // Curve tables
  cvt0 := GetElementEditValues(e, 'CVT0');
  cvt1 := GetElementEditValues(e, 'CVT1');
  cvt2 := GetElementEditValues(e, 'CVT2');
  cvt3 := GetElementEditValues(e, 'CVT3');
  cvt4 := GetElementEditValues(e, 'CVT4');

  sl.Add(
    formid + #9 +
    edid + #9 +
    full + #9 +
    GetDNAM(e, 'Ammo') + #9 +
    GetDNAM(e, 'Speed') + #9 +
    GetDNAM(e, 'Reload Speed') + #9 +
    GetDNAM(e, 'NPC Reload Delay') + #9 +
    GetDNAM(e, 'Reach') + #9 +
    GetDNAM(e, 'Reach Engagement Mult') + #9 +
    GetDNAM(e, 'Min Range') + #9 +
    GetDNAM(e, 'Max Range') + #9 +
    GetDNAM(e, 'Attack Delay Seconds') + #9 +
    GetDNAM(e, 'Damage - OutOfRangeMult') + #9 +
    GetDNAM(e, 'On Hit') + #9 +
    GetElementEditValues(e, 'DNAM\Flags (sorted)') + #9 +
    GetDNAM(e, 'Capacity') + #9 +
    GetDNAM(e, 'Ammo used per shot') + #9 +
    GetDNAM(e, 'Weapon Type') + #9 +
    GetDNAM(e, 'Secondary Damage') + #9 +
    GetDNAM(e, 'Weight') + #9 +
    GetDNAM(e, 'Value') + #9 +
    GetDNAM(e, 'Base Damage') + #9 +
    GetElementEditValues(e, 'DNAM\Sound Data\Sound Level') + #9 +
    GetDNAM(e, 'Accuracy Bonus') + #9 +
    GetDNAM(e, 'Animation Attack Seconds') + #9 +
    GetDNAM(e, 'Rank') + #9 +
    GetDNAM(e, 'Action Point Cost') + #9 +
    GetDNAM(e, 'Full Power Seconds') + #9 +
    GetDNAM(e, 'Min Power Per Shot') + #9 +
    GetDNAM(e, 'Stagger') + #9 +
    GetDNAM(e, 'Health') + #9 +
    critMult + #9 +
    critBonus + #9 +
    critEffect + #9 +
    GetDNAM(e, '# Projectiles') + #9 +
    GetDNAM(e, 'Rumble Pattern') + #9 +
    wsam + #9 +
    cvt0 + #9 +
    cvt1 + #9 +
    cvt2 + #9 +
    cvt3 + #9 +
    cvt4
  );
end;

function Finalize: integer;
var
  fname: string;
begin
  fname := ProgramPath + 'WEAP_Export_DNAM.tsv';
  AddMessage('Saving ' + IntToStr(sl.Count - 1) + ' WEAP DNAM records to: ' + fname);
  sl.SaveToFile(fname);
  sl.Free;
  Result := 0;
end;

end.
