# Fallout 76 — Legendary Mods: Data Research & Background

_Source: BnB datamine TSV exports — live build `OMOD_Export_July_2026`, cross-checked vs PTS `OMOD_Export_PTS_2026-08-05`. Prep notes for the legendary-mod pages/guides._

## 1. How legendary mods are stored

Legendary mods are **OMOD** (object-modification) records — not their own record type. Key facts:

- **Star tier = attach point.** `AttachPoint_Name` uses `¬` marks: `¬`=1★, `¬¬`=2★, `¬¬¬`=3★, `¬¬¬¬`=4★. Exactly one `¬¬¬¬¬` (5★) record exists — a unique.
- **Editor-ID pattern:** `mod_Legendary_{Weapon|Armor|PowerArmor}{tier}_{Effect}`. Weapon effects often split into `_Melee_` / `_Ranged_` / `_Guns_` variants of the same display name.
- **Category duplication:** the same effect (e.g. *Overeater's*) is a **separate OMOD** for Armor vs Power Armor; weapon effects are their own set.
- **Effect text/numbers are mostly NOT in the OMOD.** Only ~60 of 247 carry inline DESC text. Each OMOD's **Properties** ADD an **Enchantment** (`ench_Legendary…`), a `HasLegendary_*` keyword, and a `FeaturedItem` keyword. The description + magnitudes live on that **ENCH** record.
- **Page pipeline:** OMOD (name/star/category) → OMOD_Properties (Enchantments → ENCH FormID) → ENCH_Export (text + MGEF magnitudes).

## 2. Counts (live July build) — distinct effect names

| Category | 1★ | 2★ | 3★ | 4★ |
|---|--:|--:|--:|--:|
| Weapon | 29 | 13 | 22 | 23 |
| Armor | 24 | 19 | 21 | 14 |
| Power Armor | 19 | 18 | 17 | 24 |

Excluded from the above: deprecated `zzz_` (95), `TEST_`/`DEL_WIP4_` dev leftovers, and roll-machinery rows (`Random Legendary Mod`, `Bounty Legendary Mods`, `…Item Pool`, `<Prefix>…`).

## 3. Full effect lists

### Weapon — 1-Star (29)

| Effect | Weapon type | OMOD inline text |
|---|---|---|
| Adrenal | — | _(on linked ENCH)_ |
| Anti-armor | — | _(on linked ENCH)_ |
| Aristocrat's | — | _(on linked ENCH)_ |
| Assassin's | — | _(on linked ENCH)_ |
| Berserker's | — | _(on linked ENCH)_ |
| Bloodied | — | _(on linked ENCH)_ |
| Executioner's | — | _(on linked ENCH)_ |
| Exterminator's | — | _(on linked ENCH)_ |
| Feral's | Melee | _(on linked ENCH)_ |
| Furious | — | _(on linked ENCH)_ |
| Ghoul Slayer's | — | _(on linked ENCH)_ |
| Gourmand's | — | _(on linked ENCH)_ |
| Hunter's | — | _(on linked ENCH)_ |
| Instigating | — | _(on linked ENCH)_ |
| Juggernaut's | — | _(on linked ENCH)_ |
| Junkie's | — | _(on linked ENCH)_ |
| Lucid | — | _(on linked ENCH)_ |
| Medic's | — | _(on linked ENCH)_ |
| Mutant Slayer's | — | _(on linked ENCH)_ |
| Mutant's | — | _(on linked ENCH)_ |
| Nocturnal | — | _(on linked ENCH)_ |
| Quad | Ranged | _(on linked ENCH)_ |
| Sniper's | Ranged | _(on linked ENCH)_ |
| Stalker's | Ranged | _(on linked ENCH)_ |
| Suppressor's | — | Reduce Your Target's Damage Output by 25% for 5 seconds |
| Troubleshooter's | — | _(on linked ENCH)_ |
| Two Shot | Ranged | _(on linked ENCH)_ |
| Vampire's | — | Restore 2% Health over 2 seconds when you Hit a Target |
| Zealot's | — | _(on linked ENCH)_ |

### Weapon — 2-Star (13)

| Effect | Weapon type | OMOD inline text |
|---|---|---|
| Basher's | Ranged | _(on linked ENCH)_ |
| Crippling | — | _(on linked ENCH)_ |
| Explosive | Ranged | _(on linked ENCH)_ |
| Heavy Hitter's | Melee | _(on linked ENCH)_ |
| Hitman's | Ranged | _(on linked ENCH)_ |
| Inertial | — | _(on linked ENCH)_ |
| Last Shot | Ranged | _(on linked ENCH)_ |
| Pick Pocketer's | Melee | _(on linked ENCH)_ |
| Rapid | Melee, Ranged | _(on linked ENCH)_ |
| Riposting | Melee | _(on linked ENCH)_ |
| Steady | Melee | _(on linked ENCH)_ |
| V.A.T.S. Enhanced | Ranged | _(on linked ENCH)_ |
| Vital | — | _(on linked ENCH)_ |

### Weapon — 3-Star (22)

| Effect | Weapon type | OMOD inline text |
|---|---|---|
| Agility | — | _(on linked ENCH)_ |
| Arms Keeper's | — | _(on linked ENCH)_ |
| Barbarian | Melee | + STR per kill while on a Kill Streak (Max 10). |
| Blocker | Melee | _(on linked ENCH)_ |
| Charisma | — | _(on linked ENCH)_ |
| Defender's | Melee | _(on linked ENCH)_ |
| Durability | — | _(on linked ENCH)_ |
| Endurance | — | _(on linked ENCH)_ |
| Ghost's | Ranged | _(on linked ENCH)_ |
| Glowing | — | _(on linked ENCH)_ |
| Intelligence | — | _(on linked ENCH)_ |
| Lightweight | — | _(on linked ENCH)_ |
| Luck | — | _(on linked ENCH)_ |
| Lucky Hit | — | _(on linked ENCH)_ |
| Nimble | Ranged | _(on linked ENCH)_ |
| Perception | — | _(on linked ENCH)_ |
| Resilient | Ranged | _(on linked ENCH)_ |
| Rugged | — | Breaks 50% slower |
| Steadfast | Ranged | _(on linked ENCH)_ |
| Strength | — | _(on linked ENCH)_ |
| Swift | Ranged | _(on linked ENCH)_ |
| V.A.T.S. Optimized | — | _(on linked ENCH)_ |

### Weapon — 4-Star (23)

| Effect | Weapon type | OMOD inline text |
|---|---|---|
| Brutalist's | Melee | Each Kill Gives +1 Strength For 5 Mins (Max +10) |
| Bully's | — | _(on linked ENCH)_ |
| Charged | Melee | Light Attacks Build Up Charge That Is Released With A Heavy Attack (Max Charges 3) |
| Combo-Breaker's | Melee | _(on linked ENCH)_ |
| Conductor's | — | Critical Hits Restore 10 Health & Action Points instantly and 100 more over 5 seconds for Player & Teammates Within a 100ft Radius |
| Electrician's | Ranged | When Reloading, Emit a Shock Wave That Stuns Nearby Targets for 3s |
| Encircler's | — | +10% Damage For Each Combat Target Around You (up to +50%) |
| Fencer's | Melee | +12.5% Melee Damage. Additional +12.5% for Each Nearby Teammate (up to +50% on Full Team) |
| Fracturer's | — | When Crippling Limbs, They Explode and Deal up to 50 Explosion Damage to Nearby Targets |
| Head Hunter's Legendary Mod | Ranged | Zero Ammo Cost for 10 seconds after a Headshot Kill. |
| Icemen's | Melee | +20% Cryo Damage |
| Locked | Ranged | _(on linked ENCH)_ |
| Pin-Pointer's | Ranged | _(on linked ENCH)_ |
| Polished | — | _(on linked ENCH)_ |
| Pounder's | Melee | _(on linked ENCH)_ |
| Pyromaniac's | — | When a Combat Target is Burning, Deal +50% Bonus Damage |
| Ruiner's | Ranged | Single Shot Weapons Have A 5% Chance To Deal An Additional 500 Damage For Every 5 Luck |
| Satiated | — | _(on linked ENCH)_ |
| Sightseer's | Ranged | +50% Range & Damage When Aiming Down Sights |
| Stabilizer's | Ranged | Improves Weapon Cone of Fire, Recoil, and Stability by 40% |
| Tarnished | — | _(on linked ENCH)_ |
| Thrill-Seeker's | — | Reload Speed & Melee Attack Speed increases based on Killstreak Count |
| Viper's | — | When a Combat Target is Poisoned, Deal +50% Bonus Damage |
### Armor — 1-Star (24)

| Effect | Weapon type | OMOD inline text |
|---|---|---|
| Adrenal | — | +10 Damage and Energy Resistance per kill while on a Kill Streak (Max 10). |
| Aristocrat's | — | _(on linked ENCH)_ |
| Assassin's | — | _(on linked ENCH)_ |
| Auto Stim | — | _(on linked ENCH)_ |
| Bolstering | — | _(on linked ENCH)_ |
| Chameleon | — | _(on linked ENCH)_ |
| Cloaking | Melee | _(on linked ENCH)_ |
| Exterminator's | — | _(on linked ENCH)_ |
| Ghoul Slayer's | — | _(on linked ENCH)_ |
| Heavyweight | — | _(on linked ENCH)_ |
| Hunter's | — | _(on linked ENCH)_ |
| Life Saving | — | _(on linked ENCH)_ |
| Lucid | — | _(on linked ENCH)_ |
| Mutant Slayer's | — | _(on linked ENCH)_ |
| Mutant's | — | _(on linked ENCH)_ |
| Nocturnal | — | _(on linked ENCH)_ |
| Overeater's | — | _(on linked ENCH)_ |
| Powered | — | _(on linked ENCH)_ |
| Punishing | — | _(on linked ENCH)_ |
| Regenerating | — | _(on linked ENCH)_ |
| Troubleshooter's | — | _(on linked ENCH)_ |
| Unyielding | — | _(on linked ENCH)_ |
| Vanguard's | — | _(on linked ENCH)_ |
| Zealot's | — | _(on linked ENCH)_ |

### Armor — 2-Star (19)

| Effect | Weapon type | OMOD inline text |
|---|---|---|
| Agility | — | _(on linked ENCH)_ |
| Antiseptic | — | _(on linked ENCH)_ |
| Charisma | — | _(on linked ENCH)_ |
| Elementalist | — | _(on linked ENCH)_ |
| Endurance | — | _(on linked ENCH)_ |
| Fierce | — | Fortify Limb Resistance based on Kill Streak count. |
| Fireproof | — | _(on linked ENCH)_ |
| Glutton | — | _(on linked ENCH)_ |
| Hardy | — | _(on linked ENCH)_ |
| HazMat | — | _(on linked ENCH)_ |
| Intelligence | — | _(on linked ENCH)_ |
| Luck | — | _(on linked ENCH)_ |
| Pain Killer | — | Gain Health over time while on a Kill Streak. Effect becomes stronger the higher the Kill Streak. |
| Perception | — | _(on linked ENCH)_ |
| Poisoner's | — | _(on linked ENCH)_ |
| Powered | — | _(on linked ENCH)_ |
| Rushing | — | Gain Action Points over time while on a Kill Streak. Effect becomes stronger the higher the Kill Streak. |
| Strength | — | _(on linked ENCH)_ |
| Warming | — | _(on linked ENCH)_ |

### Armor — 3-Star (21)

| Effect | Weapon type | OMOD inline text |
|---|---|---|
| Acrobat's | — | _(on linked ENCH)_ |
| Active | — | _(on linked ENCH)_ |
| Adamantium | — | _(on linked ENCH)_ |
| Belted | — | _(on linked ENCH)_ |
| Burning | — | _(on linked ENCH)_ |
| Cavalier's | — | _(on linked ENCH)_ |
| Defender's | — | _(on linked ENCH)_ |
| Dissipating | — | _(on linked ENCH)_ |
| Diver's | — | Regenerate Health While in Water |
| Doctor's | — | _(on linked ENCH)_ |
| Durability | — | _(on linked ENCH)_ |
| Electrified | — | _(on linked ENCH)_ |
| Frozen | — | _(on linked ENCH)_ |
| Healthy | — | _(on linked ENCH)_ |
| Pack Rat's | — | _(on linked ENCH)_ |
| Reflex | — | _(on linked ENCH)_ |
| Safecracker's | — | _(on linked ENCH)_ |
| Secret Agent's | — | _(on linked ENCH)_ |
| Sentinel's | — | _(on linked ENCH)_ |
| Thru-hiker's | — | _(on linked ENCH)_ |
| Toxic | — | _(on linked ENCH)_ |

### Armor — 4-Star (14)

| Effect | Weapon type | OMOD inline text |
|---|---|---|
| Battle-Loader's | — | 15% Chance to Instantly Reload When Bashing Enemies (up to 75% Chance on Full Stack) |
| Bruiser's | — | Melee Weapons Deal +5% Bonus Damage (up to +25% on Full Stack) |
| Crusaders | — | +1 S.P.E.C.I.A.L. Point For Each Equipped Armor Piece (up to +5 on Full Stack) |
| Hauler's | — | Increases Carry Capacity by 30. |
| Limit-Breaking | — | Each Worn Armor Piece Reduces the Cost of Critical Hits by -10% (up to -50% on Full Stack) |
| Metabolic | — | All Chems, Serums, Bobbleheads & Magazines Buffs Last 5% Longer (up to 25% on Full Stack) |
| Miasma's | — | _(on linked ENCH)_ |
| Raging | — | Upon being hit, deal +3% Damage for 10 seconds. |
| Ranger's | — | Ranged Weapons Deal +5% Bonus Damage (up to +25% on Full Stack) |
| Rebounders | — | Return 10% of Ballistic & Explosive Damage Received From An Enemy Target Back Towards Them (up to 50% on Full Stack) |
| Runner's | — | Sprinting Action Point Cost Reduced by -20% (up to -100% on Full Stack) |
| Sawbones | — | _(on linked ENCH)_ |
| Tanky's | — | +200 Damage Resist for 10s When Standing Still (20s Cooldown) (up to +1000 on Full Stack) |
| Vector | — | Gain 10% Bonus V.A.T.S. Accuracy Against Distant Targets |
### Power Armor — 1-Star (19)

| Effect | Weapon type | OMOD inline text |
|---|---|---|
| Adrenal | — | +10 Damage and Energy Resistance per kill while on a Kill Streak (Max 10). |
| Aristocrat's | — | _(on linked ENCH)_ |
| Assassin's | — | _(on linked ENCH)_ |
| Auto Stim | — | _(on linked ENCH)_ |
| Bolstering | — | _(on linked ENCH)_ |
| Chameleon | — | _(on linked ENCH)_ |
| Cloaking | Melee | _(on linked ENCH)_ |
| Exterminator's | — | _(on linked ENCH)_ |
| Ghoul Slayer's | — | _(on linked ENCH)_ |
| Hunter's | — | _(on linked ENCH)_ |
| Lucid | — | _(on linked ENCH)_ |
| Mutant Slayer's | — | _(on linked ENCH)_ |
| Mutant's | — | _(on linked ENCH)_ |
| Nocturnal | — | _(on linked ENCH)_ |
| Overeater's | — | _(on linked ENCH)_ |
| Regenerating | — | _(on linked ENCH)_ |
| Troubleshooter's | — | _(on linked ENCH)_ |
| Vanguard's | — | _(on linked ENCH)_ |
| Zealot's | — | _(on linked ENCH)_ |

### Power Armor — 2-Star (18)

| Effect | Weapon type | OMOD inline text |
|---|---|---|
| Agility | — | _(on linked ENCH)_ |
| Antiseptic | — | _(on linked ENCH)_ |
| Charisma | — | _(on linked ENCH)_ |
| Elementalist | — | _(on linked ENCH)_ |
| Endurance | — | _(on linked ENCH)_ |
| Fireproof | — | _(on linked ENCH)_ |
| Glutton | — | _(on linked ENCH)_ |
| Hardy | — | _(on linked ENCH)_ |
| HazMat | — | _(on linked ENCH)_ |
| Intelligence | — | _(on linked ENCH)_ |
| Luck | — | _(on linked ENCH)_ |
| Pain Killer | — | Gain Health over time while on a Kill Streak. Effect becomes stronger the higher the Kill Streak. |
| Perception | — | _(on linked ENCH)_ |
| Poisoner's | — | _(on linked ENCH)_ |
| Powered | — | _(on linked ENCH)_ |
| Rushing | — | Gain Action Points over time while on a Kill Streak. Effect becomes stronger the higher the Kill Streak. |
| Strength | — | _(on linked ENCH)_ |
| Warming | — | _(on linked ENCH)_ |

### Power Armor — 3-Star (17)

| Effect | Weapon type | OMOD inline text |
|---|---|---|
| Active | — | _(on linked ENCH)_ |
| Arms Keeper's | — | _(on linked ENCH)_ |
| Belted | — | _(on linked ENCH)_ |
| Burning | — | _(on linked ENCH)_ |
| Cavalier's | — | _(on linked ENCH)_ |
| Defender's | — | _(on linked ENCH)_ |
| Dissipating | — | _(on linked ENCH)_ |
| Doctor's | — | _(on linked ENCH)_ |
| Durability | — | _(on linked ENCH)_ |
| Electrified | — | _(on linked ENCH)_ |
| Frozen | — | _(on linked ENCH)_ |
| Healthy | — | _(on linked ENCH)_ |
| Pack Rat's | — | _(on linked ENCH)_ |
| Safecracker's | — | _(on linked ENCH)_ |
| Sentinel's | — | _(on linked ENCH)_ |
| Thru-hiker's | — | _(on linked ENCH)_ |
| Toxic | — | _(on linked ENCH)_ |

### Power Armor — 4-Star (24)

| Effect | Weapon type | OMOD inline text |
|---|---|---|
| Aegis | — | Fortifies Physical & Energy Resists by +50 and Poison, Cryo & Fire Resists by +20 for Wearer & Teammates Within a 50ft Radius |
| Battle-Loader's | — | 15% Chance to Instantly Reload When Bashing Enemies (up to 75% Chance on Full Stack) |
| Bruiser's | — | Melee Weapons Deal +5% Bonus Damage (up to +25% on Full Stack) |
| Choo-Choo's | — | _(on linked ENCH)_ |
| Crusaders | — | +1 S.P.E.C.I.A.L. Point For Each Equipped Armor Piece (up to +5 on Full Stack) |
| Hauler's | — | Increases Carrying Capacity by 30. |
| Limit-Breaking | — | Each Worn Armor Piece Reduces the Cost of Critical Hits by -10% (up to -50% on Full Stack) |
| Metabolic | — | All Chems, Serums, Bobbleheads & Magazines Buffs Last 5% Longer (up to 25% on Full Stack) |
| Miasma's | — | _(on linked ENCH)_ |
| Over-Loader's | — | 10% Chance To Trigger An EMP Pulse When Hit (up to 50% on Full Stack), Dealing 50 Energy Damage To Nearby Enemies For 5 Seconds |
| Propelling | — | Movement & Sprint Speed Increased by +5% (up to +25% on Full Stack) |
| Radioactive-Powered | — | Gain +2 Action Point Regen at the Cost of RADS (up to +10 Action Point Regen on Full Stack) |
| Raging | — | Upon being hit, deal +3% Damage for 10 seconds. |
| Ranger's | — | Ranged Weapons Deal +5% Bonus Damage (up to +25% on Full Stack) |
| Reflective | — | Return 10% of Damage Received from an Enemy Target Back Towards Them (up to 50% on Full Stack) |
| Rejuvenator's | — | Gain Health and Action Point Regeneration as You Fill Your Hunger and Thirst Meters. |
| Runner's | — | Sprinting Action Point Cost Reduced by -20% (up to -100% on Full Stack) |
| Sawbones | — | _(on linked ENCH)_ |
| Scanner's | — | Action Point Cost of V.A.T.S. Attacks Reduced by -5% (up to -25% on Full Stack) |
| Stagger-Proof | — | 15% Chance to Not Stagger from any Damage Types (up to 80% on Full Stack) |
| Stalwart's | — | Power Armor Breaks 5% Slower For Owner & All Teammates Within A 50ft Radius (up to 25% Slower on Full Stack) |
| Tanky's | — | +200 Damage Resist for 10s When Standing Still (20s Cooldown) (up to +1000 on Full Stack) |
| Vector | — | Gain 10% Bonus VATS Accuracy Against Distant Targets |
| Voltaic | — | Sprinting Builds Up A Charge & Releases An EMP Shockwave When You Stop That Stuns & Deals Energy Damage To Nearby Enemies |

## 4. Unique / item-bound legendary mods

| Effect | Category | Editor ID | Note |
|---|---|---|---|
| Anti-armor Legendary Mod | Armor | `mod_Legendary_CircuitBreaker_AntiArmor` | Anti-armor bound to the unique "Circuit Breaker" weapon |
| Bloodied | Other | `mod_Custom_Legendary_WarBloodied` | Custom Bloodied (unique weapon) |
| Quad | Other | `Bounty_mod_Custom_QuadLegendary2` | Custom Quad (bounty/reward weapon) |
| Executioner's | Weapon | `Burn_Custom_Legendary_Weapon1_Execute` | The only 5★ record; bound to a custom "Burn" weapon |

## 5. Upcoming on PTS (not yet live)

| Effect | Star | Category | Editor ID |
|---|---|---|---|
| Cryologist's | 2 | Weapon | `POST_mod_Legendary_Weapon2_Cryo` |
| Pyro-Technician's | 2 | Weapon | `POST_mod_Legendary_Weapon2_Fire` |
| Poisoner's | 2 | Weapon | `POST_mod_Legendary_Weapon2_Poison` |
| Severing | 4 | Weapon | `SDOW_mod_Legendary_Weapon4_Severing` |
| Icemen's | 4 | Weapon | `mod_Legendary_Weapon4_Icemens (consolidated)` |

Three new **2★ elemental weapon** mods (*Cryologist's*/cryo, *Pyro-Technician's*/fire, *Poisoner's*/poison) plus a new 4★ **Severing**. *Icemen's* moves from a melee-only ID to a general weapon ID.

## 6. Caveats for guide-writing

- **ENCH join required** for real text + per-star magnitudes on ~75% of mods.
- Weapon effects often exist as **separate Melee vs Ranged/Guns OMODs** with the same name — decide merged vs split on the pages.
- Some 4★ IDs carry update tags (`HTO_`,`P62_`,`RA_`,`POST_`,`SDOW_`) marking the drop they arrived in — handy for a 'date added' column, but confirm none are dev duplicates before publishing.
- A few names have `_LEGACY` twins (e.g. *Medic's*) — the non-LEGACY OMOD is the current one.
- Roll-pool rows (`Random Legendary Mod`, `Bounty Legendary Mods`) are machinery — keep out of the effect checklist.
