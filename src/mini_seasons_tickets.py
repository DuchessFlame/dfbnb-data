"""Static overlay for mini season / LTE JSON.

Three kinds of data are NOT present in the Fallout 76 game-file TSV exports
and have to be maintained by hand here:

  1. TICKET_REWARDS    — tickets awarded for completing a mini-season challenge
  2. TICKET_COSTS      — tickets required to buy a mini-season scoreboard reward
  3. CHALLENGE_REWARDS — per-challenge item reward for limited-time events
                         (LTEs don't use tickets; each challenge drops a
                          specific consumable / apparel / decor item)

The TSV exports are the source of truth for everything else (challenge text,
EDIDs, reward item lists, scope, conditions, etc.). This overlay is merged
into the built JSON after TSV parsing, so a rebuild never wipes these values.

Field mapping (matches df-bnb-mini-seasons.js rendering chain):
  TICKET_REWARDS[challenge_edid]    → entry['ticket_reward']   (mini-season path)
  TICKET_COSTS[reward_edid]         → reward['ticket_cost']    (mini-season path)
  CHALLENGE_REWARDS[challenge_edid] → entry['reward']          (LTE path)

EDID lookups are case-sensitive. Use the exact EDID string from the TSV /
dist JSON (e.g. 'ATX_DE2023_Birthday_Challenge_Consume_Alcohol').

Leave a short comment next to each entry noting the source screenshot /
date so future updates can be cross-checked.
"""

# ─────────────────────────────────────────────────────────────
# Tickets awarded for completing a challenge. (Mini seasons only.)
# Key: challenge EDID. Value: integer ticket count.
# ─────────────────────────────────────────────────────────────
TICKET_REWARDS = {
    # ── Sunset Stranger (2025-12-23 → 2026-01-06) ──
    # Source: screenshots 1151340_20251224013100/013103 and 20251231011355/011358
    'ATX_DE2025_SunsetStranger_Week1': 60,  # "Complete all Challenges to Kill an Enemy" (week1 rollup)
    'ATX_DE2025_SunsetStranger_Week2': 60,  # "Complete all Challenges to Kill an Enemy" (week2 rollup — same name/reward as week1)
    'ATX_DE2025_SunsetStranger_Event_Complete': 100,  # Kill 176 Enemies in Burning Springs bonus
    'ATX_DE2025_SunsetStranger_Week1_Challenge_Cripple_Human_Heads': 20,
    'ATX_DE2025_SunsetStranger_Week1_Challenge_Consume_Food_Prewar': 20,
    'ATX_DE2025_SunsetStranger_Week1_Challenge_Kill_Radhog': 20,
    'ATX_DE2025_SunsetStranger_Week1_Challenge_Kill_BurningDeathclaw': 20,
    'ATX_DE2025_SunsetStranger_Week1_Challenge_Kill_Enemy_Region_BurningSprings': 20,
    'ATX_DE2025_SunsetStranger_Week1_Challenge_Kill_Insects': 20,
    'ATX_DE2025_SunsetStranger_Week1_Challenge_Mod_Pistol': 20,
    'ATX_DE2025_SunsetStranger_Week2_Challenge_CAMPS_Build_Turret_Traps': 20,
    'ATX_DE2025_SunsetStranger_Week2_Challenge_Catch_Fish_BurningSprings': 20,
    'ATX_DE2025_SunsetStranger_Week2_Challenge_Collect_Eggs_Deathclaw': 20,
    'ATX_DE2025_SunsetStranger_Week2_Challenge_Kill_BurningScorpion': 20,
    'ATX_DE2025_SunsetStranger_Week2_Challenge_Kill_BurningStingwing': 20,
    'ATX_DE2025_SunsetStranger_Week2_Challenge_Kill_Humans_BurningSprings': 20,
    # (Kill_BurningOgua not yet captured — no screenshot covering it)

    # ── Love Hurts (2026-02-03 → 2026-02-17) ──
    # Source: screenshots 1151340_20260204011732 and 011735
    'ATX_DE2025_LoveHurts_Challenge_Week1_Complete': 60,  # "Complete a Week 1 Challenge"
    'ATX_DE2025_LoveHurts_MiniSeason_Complete': 140,  # "Complete a Weekly Completion Challenge"
    'ATX_DE2025_LoveHurts_Week1_Challenge_Collect_Chlorine': 20,
    'ATX_DE2025_LoveHurts_Week1_Challenge_Collect_MedX': 20,
    'ATX_DE2025_LoveHurts_Week1_Challenge_Craftt_Psycho': 20,
    'ATX_DE2025_LoveHurts_Week1_Challenge_Harvest_ToxicSootFlower': 20,
    'ATX_DE2025_LoveHurts_Week1_Challenge_GainScrap_Steel': 20,
    'ATX_DE2025_LoveHurts_Week1_Challenge_TravelDistance': 20,
    # (Week2 challenges + Week2_Complete not yet captured — screenshot batch was taken on day 1 of the mini season)

    # ── Rip Daring's Weapons Expert Extraordinaire (2026) ──
    # (values pending — fill in from screenshots)
}


# ─────────────────────────────────────────────────────────────
# Tickets required to purchase a reward from the Scoreboard.
# Key: reward EDID (ENTM). Value: integer ticket count.
# ─────────────────────────────────────────────────────────────
TICKET_COSTS = {
    # ── Love Hurts (2026-02-03 → 2026-02-17) ──
    # Source: screenshots 1151340_20260204011752 (page 1/2) and 011759 (page 2/2)
    'SCORE_MiniSeason_LoveHurts_ENTM_Apparel_Headwear_ToughLove': 0,  # Starter helmet — free on page 1
    'SCORE_MiniSeason_LoveHurts_ENTM_PlayerTitles_Suffix_Masochist': 10,
    'SCORE_MiniSeason_LoveHurts_ENTM_PlayerIcon_WoundedHeart': 10,  # Rusted Heart Player Icon
    'SCORE_MiniSeason_LoveHurts_Photomode_Logo_Marquee': 10,  # Love Hurts Marquee Photomode Logo
    'SCORE_MiniSeason_LoveHurts_ENTM_PlayerTitles_Prefix_Gruesome': 20,
    'SCORE_MiniSeason_LoveHurts_ENTM_PlayerTitles_Prefix_Infatuated': 25,
    'SCORE_MiniSeason_LoveHurts_ENTM_CAMP_FloorlDecor_HeartArchRaiders': 40,  # Vandalized Heart Arch
    'SCORE_MiniSeason_Lovehurts_ENTM_CAMP_WallDecor_CarvedPoems': 40,  # Raider Love Poems
    'SCORE_MiniSeason_LoveHurts_ENTM_CAMP_Furniture_Chair_LethalSeat': 80,  # Lethal Loveseat
    'SCORE_MiniSeason_Lovehurts_Weapons_PiercingLove': 100,  # Piercing Love Bow
    # (Dom Pedro Stock Mod Bundle #1/#2 + Sight Mods not added — screenshots show prices 20 and 25
    #  but bundle numbers aren't legible, so can't disambiguate the 3 EDIDs safely.)

    # ── Rip Daring's Weapons Expert Extraordinaire (2026) ──
    'SCORE_MiniSeason_2026_WeaponsExpert_ENTM_CAMP_Furniture_RipBoyStatue': 0,  # Rip Daring Vault Boy Statue — free starter
}


# ─────────────────────────────────────────────────────────────
# Per-challenge item rewards for LTE events (no ticket economy).
# Key: challenge EDID. Value: dict matching the JS `item.reward` shape:
#   {name: str, type?: 'consumable'|'score', description?: str, image_url?: str}
# When `image_url` is omitted, the overlay tries to auto-resolve it by
# matching `name` against the event's rewards[] list (case-insensitive).
# ─────────────────────────────────────────────────────────────
CHALLENGE_REWARDS = {
    # ── Birthday Challenge (2023) ──
    # Source: "Birthday challenges checklist 4.png" (TSJ & Sugarbombs RAD)
    # Week 1 dailies:
    'ATX_DE2023_Birthday_Challenge_Collect_CakePan':         {'name': 'Vault Shelter Poster'},
    'ATX_DE2023_Birthday_Challenge_Collect_MilkBrahmin':     {'name': 'Brahmin Pant Suit'},
    'ATX_DE2023_Birthday_Challenge_Collect_MothmanEgg':      {'name': 'Fallout Birthday Icon'},
    'ATX_DE2023_Birthday_Challenge_Collect_OpposumMeat':     {'name': 'Vault 76 Suit Box'},
    'ATX_DE2023_Birthday_Challenge_Collect_SpicesSugar':     {'name': 'Birthday Hat'},
    'ATX_DE2023_Birthday_Challenge_Craft_CookedMeal':        {'name': 'Birthday Wallpaper'},
    'ATX_DE2023_Birthday_Challenge_Craft_FermentAlcohol':    {'name': 'Birthday Rolling Pin Skin'},
    # Week 2 dailies:
    'ATX_DE2023_Birthday_Challenge_Build_BirthdayCake':      {'name': 'Broken GECK Machine'},
    'ATX_DE2023_Birthday_Challenge_Build_PartyDecorations':  {'name': 'Birthday Photoframe'},
    'ATX_DE2023_Birthday_Challenge_Consume_Alcohol':         {'name': 'Mysterious Wooden Crate'},
    'ATX_DE2023_Birthday_Challenge_Consume_BirthdayCake':    {'name': 'Sugar Bombers Jacket and Jeans'},
    'ATX_DE2023_Birthday_Challenge_Craft_PieCakeCobbler':    {'name': 'Vault Boy Birthday Rug'},
    'ATX_DE2023_Birthday_Challenge_Photo_BirthdaySuit':      {'name': 'Sugar Bombs Fridge'},
    # NOTE: Fast Travel to Vault 76 reward — last word of "Brahmin Short___" was not
    # fully legible in the screenshot. Best reading is "Brahmin Shorts" but confirm.
    'ATX_DE2023_Birthday_Challenge_Visit_Vault76':           {'name': 'Brahmin Shorts'},
    # Weekly completion rewards:
    'ATX_DE2023_Birthday_Challenge_Week1_Complete':          {'name': 'Birthday Cake Resource'},
    'ATX_DE2023_Birthday_Challenge_Week2_Complete':          {'name': 'Nukashine Fermenter'},

    # ── Halloween 2023 ──
    # Source: "Halloween Challenge 2023.png" (Determined by DSJ)
    # Daily S.C.O.R.E. challenges (candy-based):
    'ATX_DE2023_Halloween_SCORE_Challenge_Daily_TrickorTreat': {'name': 'S.C.O.R.E.', 'type': 'score'},
    'ATX_DE2023_Halloween_SCORE_Challenge_Daily_GiveOutCandy': {'name': 'S.C.O.R.E.', 'type': 'score'},
    # Daily Spooky Scorched kill challenges (3 days):
    'ATX_DE2023_Halloween_Challenge_Daily_Scorched_Day1':    {'name': 'Improved Repair Kit x1', 'type': 'consumable'},
    'ATX_DE2023_Halloween_Challenge_Daily_Scorched_Day2':    {'name': 'Lunchbox x1', 'type': 'consumable'},
    'ATX_DE2023_Halloween_Challenge_Daily_Scorched_Day3':    {'name': 'Perfect Bubblegum x3', 'type': 'consumable'},
    'ATX_DE2023_Halloween_Challenge_Weekly_Scorched':        {'name': 'Carry Weight Booster x3', 'type': 'consumable'},
    # Weekly completion rewards:
    'ATX_DE2023_Halloween_Challenge_Event_GiveOutCandy_Complete': {'name': 'Ghost Skeleton Costume'},
    'ATX_DE2023_Halloween_Challenge_Event_TrickorTreat_Complete': {'name': 'Ghost Skull Hood Mask'},
    'ATX_DE2023_Halloween_Challenge_Event_Halloween_Complete':    {'name': 'Mothman Nest'},

    # ── Spread the Love 2023 ──
    # Source: "Spread the love challenge 2023 jpeg.jpg" (theduchessflame.com)
    'ATX_DE2023_Valentines_Challenge_Daily_Collect_AshRoses':       {'name': 'Legendary Module x3', 'type': 'consumable'},
    'ATX_DE2023_Valentines_Challenge_Daily_Collect_SootFlowers':    {'name': '500 S.C.O.R.E.', 'type': 'score'},
    'ATX_DE2023_Valentines_Challenge_Daily_Collect_FeverBlossoms':  {'name': 'Lunchbox x3', 'type': 'consumable'},
    'ATX_DE2023_Valentines_Challenge_Daily_Collect_Ceramic':        {'name': 'Basic Repair Kit x3', 'type': 'consumable'},
    'ATX_DE2023_Valentines_Challenge_Daily_Consume_Alcohol':        {'name': 'Perk Card Pack x3', 'type': 'consumable'},
    'ATX_DE2023_Valentines_Challenge_Daily_Craft_FermentableWine':  {'name': 'Legendary Module x3', 'type': 'consumable'},
    'ATX_DE2023_Valentines_Challenge_Daily_Visit_TOL':              {'name': '500 S.C.O.R.E.', 'type': 'score'},
    # 5/7 Daily completion unlock:
    'ATX_DE2023_Valentines_Challenge_EventDUPLICATE000':            {'name': "Valentine's Flower Bouquet"},

    # ── Science of Love 2025 (Feb 5 – Feb 19) ──
    # Source: "DE2025_SOL_PUBLIC_FINAL_EN.jpg"
    # NOTE: Event is classified as type=mini_season in EVENT_DEFS, but the 2025
    # public flyer shows per-challenge item rewards (LTE-style), not tickets.
    # Duchess: confirm whether this re-run should be reclassified to
    # limited_time_event in EVENT_DEFS. If it stays as mini_season, the
    # frontend will render ticket_reward instead of these item rewards.
    # Week 1 dailies (all while wearing the Doctor Head Mirror):
    'ATX_DE2024_ScienceOfLove_Week1_Challenge_Kill_SuperMutant':        {'name': 'Nuka-Cola Mix Pack x1', 'type': 'consumable'},
    'ATX_DE2024_ScienceOfLove_Week1_Challenge_Kill_FeralGhoul':         {'name': 'Basic Repair Kit x3', 'type': 'consumable'},
    'ATX_DE2024_ScienceOfLove_Week1_Challenge_GainScrap_FiberOptics':   {'name': 'Perk Card Pack x3', 'type': 'consumable'},
    'ATX_DE2024_ScienceOfLove_Week1_Challenge_Collect_NuclearWaste':    {'name': "Scout's Banner"},
    'ATX_DE2024_ScienceOfLove_Week1_Challenge_Collect_Microscope':      {'name': 'Lunchbox x3', 'type': 'consumable'},
    'ATX_DE2024_ScienceOfLove_Week1_Challenge_Collect_HumanSkull':      {'name': 'S.C.O.R.E.', 'type': 'score'},
    'ATX_DE2024_ScienceOfLove_Week1_Challenge_Collect_Autopsy_BoardGame': {'name': 'S.C.O.R.E.', 'type': 'score'},
    # Week 2 dailies:
    'ATX_DE2024_ScienceOfLove_Week2_Challenge_Collect_EnergyAmmo':  {'name': 'Nuka-Cola Mix Pack x1', 'type': 'consumable'},
    'ATX_DE2024_ScienceOfLove_Week2_Challenge_Kill_Robots':         {'name': "Scout's Banner"},
    'ATX_DE2024_ScienceOfLove_Week2_Challenge_Kill_MutantHound':    {'name': 'Perk Card Pack x3', 'type': 'consumable'},
    'ATX_DE2024_ScienceOfLove_Week2_Challenge_Kill_Insect':         {'name': 'Basic Repair Kit x3', 'type': 'consumable'},
    'ATX_DE2024_ScienceOfLove_Week2_Challenge_GainScrap_Glass':     {'name': 'S.C.O.R.E.', 'type': 'score'},
    'ATX_DE2024_ScienceOfLove_Week2_Challenge_Deal_Damage_Energy':  {'name': 'S.C.O.R.E.', 'type': 'score'},
    'ATX_DE2024_ScienceOfLove_Week2_Challenge_Collect_Beakers':     {'name': 'S.C.O.R.E.', 'type': 'score'},
    # Weekly completions:
    'ATX_DE2024_ScienceOfLove_Challenge_Week1_Complete':            {'name': 'Dopamine Wallpaper'},
    'ATX_DE2024_ScienceOfLove_Challenge_Week2_Complete':            {'name': 'Study of Love Chalkboard'},
}


def _resolve_reward_image(reward_spec, event_rewards):
    """If reward_spec lacks image_url, try to find one in event.rewards[] by name.

    Names are matched case-insensitively with whitespace collapsed. Consumables
    (type='consumable') and score rewards (type='score') are skipped — they
    don't have ENTM entries to look up.
    """
    if reward_spec.get('image_url'):
        return reward_spec['image_url']
    if reward_spec.get('type') in ('consumable', 'score'):
        return ''
    target = ' '.join(str(reward_spec.get('name', '')).lower().split())
    if not target:
        return ''
    for r in event_rewards or []:
        name = ' '.join(str(r.get('name', '')).lower().split())
        if name == target and r.get('image_url'):
            return r['image_url']
    return ''


def apply_ticket_overlay(output):
    """Merge static ticket values and per-challenge LTE rewards into output.

    Mutates `output` in place. Safe to call even if the dicts are empty.
    Called from build_mini_seasons_json.main() right before writing JSON.

    Returns a stats dict with counts of each field written.
    """
    chal_ticket_hits = 0
    rew_ticket_hits = 0
    chal_reward_hits = 0

    for ev in output.values():
        # Collect every challenge bucket once (week1–week8 + bonus + cut).
        buckets = []
        for wi in range(1, 9):
            wk = f'week{wi}'
            if wk in ev:
                buckets.append(ev[wk])
        if 'bonus' in ev:
            buckets.append(ev['bonus'])
        if 'cut' in ev:
            buckets.append(ev['cut'])

        event_rewards = ev.get('rewards', []) or []

        for bucket in buckets:
            for chal in bucket:
                edid = chal.get('edid', '')
                # 1) Ticket reward (mini-season path)
                if edid in TICKET_REWARDS:
                    chal['ticket_reward'] = TICKET_REWARDS[edid]
                    chal_ticket_hits += 1
                # 2) Per-challenge item reward (LTE path)
                if edid in CHALLENGE_REWARDS:
                    spec = dict(CHALLENGE_REWARDS[edid])
                    spec.setdefault('description', '')
                    spec.setdefault('image_url', _resolve_reward_image(spec, event_rewards))
                    spec.setdefault('edid', '')
                    chal['reward'] = spec
                    chal_reward_hits += 1

        # 3) Ticket costs on reward items (mini-season path)
        for rew in event_rewards:
            edid = rew.get('edid', '')
            if edid in TICKET_COSTS:
                rew['ticket_cost'] = TICKET_COSTS[edid]
                rew_ticket_hits += 1

    return {
        'challenges_ticket_updated':  chal_ticket_hits,
        'rewards_ticket_updated':     rew_ticket_hits,
        'challenges_reward_updated':  chal_reward_hits,
    }
