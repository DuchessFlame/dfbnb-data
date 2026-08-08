# Purveyor Murmrgh — Legendary Mod Pool (authoritative)

_Built from hard datamine: `NPC2_Vendors_August_2026.tsv`, `LVLI_Export_July_2026_*`, and the new `LGDI_Export_PTS_2026-08-08_*` (main + `_Mods`) resolved against `OMOD_Export` names. This replaces the earlier shard-inferred draft — the vendor's LGDIs reference the `modcol → mod_Legendary_*` OMOD pools, not the parallel `LegendaryShards` MISC system._

## 1. The data chain (fully resolved)

| Step | Record | FormID | EDID |
|---|---|---|---|
| Vendor NPC | Purveyor Murmrgh | `003F878E` | `LGV01_MoleMiner` |
| Merchant container | Vendor chest | `003FEC8C` | `LGV01_MoleMiner_VendorChest` |
| Sale leveled list | Her inventory | `005380D1` | `LGV01_LL_Vendor_LegendaryItems` |
| Roll generators | 12 × LGDI | — | `LegendaryItems_{Armor\|Weapons_Melee\|Weapons_Ranged\|PowerArmor}_Rank{1,2,3}` |
| Mod pool per rank/tier | modcol OMOD | — | `modcol_Legendary_Crafting_{Cat}{Tier}` |
| Actual effects | mod OMODs | — | `mod_Legendary_{Cat}{Tier}_{Effect}` |

Her inventory list also carries `LegendaryModule` (qty 100) and `c_Steel_Vault94_scrap` (qty 500) — currency/scrap, not mods — plus a conditional `LegendaryItems_Special_AllItems` gated on the `Update01_LGV_Mystery1_Enabled` global.

## 2. Scrip cost (from LGDI DATA field)

Ranks 1–3 are flat across every category; the price only splits at 4★:

| Category | 1★ | 2★ | 3★ | 4★ |
|---|--:|--:|--:|--:|
| Armor | 25 | 35 | 60 | 100 |
| Weapon (melee & ranged) | 25 | 35 | 60 | 150 |
| Power Armor | 25 | 35 | 60 | 200 |

**Murmrgh sells 1★–3★ only** — her list has no Rank 4 generator. The 4★ costs above come from the base `LegendaryItems_*_Rank4` records (used by crafting / other sources), included here for the scrip-cost page.

## 3. How a roll works (rank accumulation)

A Rank-N generator draws **one effect from each tier pool 1..N** — that's why a 3★ item has a 1★ + 2★ + 3★ effect. `LegModCount` on the LGDI = N confirms it:

- **Rank 1** → tier-1 pool (1 effect)
- **Rank 2** → tier-1 + tier-2 pools (2 effects)
- **Rank 3** → tier-1 + tier-2 + tier-3 pools (3 effects)

Melee and ranged generators share the same `modcol_Weapon{1,2,3}` pools; the melee/ranged split is enforced by each mod's own attach conditions (`Guns_` = ranged-only, `Melee_` = melee-only), with different applicable-item lists (`Weapons_Melee_All` vs `Weapons_Ranged_All`). **Power Armor is its own distinct, smaller pool** — not the armor pool.

Pool sizes: Armor 20 / 15 / 19 · Weapon 25 / 13 / 18 · Power Armor 17 / 15 / 15.

## 4. The pools — every effect she can roll

### Weapon (melee & ranged share these)

**Tier 1 (25):** Anti-armor, Medic's, Junkie's, Instigating, Bloodied, Berserker's, Juggernaut's, Aristocrat's, Nocturnal, Mutant's, Furious, Suppressor's, Executioner's, Gourmand's, Vampire's, Hunter's, Exterminator's, Ghoul Slayer's, Assassin's, Troubleshooter's, Zealot's, Mutant Slayer's, Stalker's *(ranged)*, Quad *(ranged)*, Two Shot *(ranged)*

**Tier 2 (13):** Inertial, Vital, Crippling, Rapid, Basher's *(ranged)*, Hitman's *(ranged)*, Explosive *(ranged)*, Last Shot *(ranged)*, V.A.T.S. Enhanced *(ranged)*, Riposting *(melee)*, Heavy Hitter's *(melee)*, Steady *(melee)*, Rapid *(melee)*
_(Rapid exists as both a ranged `Guns_RoF` and a melee `Melee_SwingSpeed` mod — same display name.)_

**Tier 3 (18):** Lucky Hit, Durability, V.A.T.S. Optimized, Lightweight, Defender's *(melee)*, Blocker *(melee)*, Ghost's *(ranged)*, Nimble *(ranged)*, Swift *(ranged)*, Resilient *(ranged)*, Steadfast *(ranged)*, Strength, Perception, Endurance, Charisma, Intelligence, Agility, Luck

### Armor

**Tier 1 (20):** Life Saving, Chameleon, Cloaking, Regenerating, Unyielding, Auto Stim, Overeater's, Bolstering, Vanguard's, Heavyweight, Aristocrat's, Nocturnal, Mutant's, Hunter's, Exterminator's, Ghoul Slayer's, Assassin's, Troubleshooter's, Zealot's, Mutant Slayer's

**Tier 2 (15):** Powered, Warming, Antiseptic, Fireproof, Glutton, Poisoner's, HazMat, Hardy, Strength, Perception, Endurance, Charisma, Intelligence, Agility, Luck

**Tier 3 (19):** Burning, Electrified, Frozen, Toxic, Durability, Doctor's, Sentinel's, Acrobat's, Safecracker's, Dissipating, Adamantium, Secret Agent's, Diver's, Cavalier's, Defender's, Belted, Thru-hiker's, Pack Rat's, Arms Keeper's

### Power Armor

**Tier 1 (17):** Chameleon, Cloaking, Regenerating, Auto Stim, Overeater's, Bolstering, Vanguard's, Aristocrat's, Nocturnal, Mutant's, Hunter's, Exterminator's, Ghoul Slayer's, Assassin's, Troubleshooter's, Zealot's, Mutant Slayer's
_(vs Armor T1: no Life Saving, Unyielding, or Heavyweight.)_

**Tier 2 (15):** Powered, Warming, Antiseptic, Fireproof, Glutton, Poisoner's, HazMat, Hardy, Strength, Perception, Endurance, Charisma, Intelligence, Agility, Luck
_(identical to Armor T2.)_

**Tier 3 (15):** Burning, Electrified, Frozen, Toxic, Durability, Doctor's, Sentinel's, Safecracker's, Dissipating, Cavalier's, Defender's, Belted, Thru-hiker's, Pack Rat's, Arms Keeper's
_(vs Armor T3: no Acrobat's, Adamantium, Secret Agent's, or Diver's.)_

## 5. Notes / caveats

- **PTS rename incoming:** on PTS (Aug 2026) the 2★ resistance mods are renamed — **Warming → Cryologist's** and **Fireproof → Pyro-Technician's** (both Armor and Power Armor). Live (July) names are used above. Same OMOD records, display-name change only.
- Live vs PTS pool membership is otherwise identical (no records added/removed) — verified by diffing `OMOD_Export_July_2026` against `OMOD_Export_PTS_2026-08-05`.
- Some `mod_Legendary_*` records exist but are **not** in her modcol pools, so she can't roll them: 1★ Adrenal, Lucid, Sniper's, Feral's; 2★ Fierce, Elementalist, Rushing, Pain Killer, Pick Pocketer's; 3★ Reflex, Active, Healthy, Glowing, Barbarian. (Plus `_LEGACY` twins like the old Medic's/Explosive.)
- No 4★ anywhere on her list.

## 6. Provenance

Chain, scrip costs, per-rank modcol bindings and full pool membership are now **hard data** from the LGDI export (the `ModPool_Refs` column resolves each modcol to its `mod_Legendary_*` list with FormIDs). The only inference remaining is nil — the earlier "LGDI→pool link not exported" caveat is resolved.
