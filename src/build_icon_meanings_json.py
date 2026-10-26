#!/usr/bin/env python3
"""
build_icon_meanings_json.py - HUD Icon Meanings + CAMP Icon Meanings
====================================================================
Pages:  /df/data-mining/guide-hud-icon-meanings/   (page key "hud")
        /df/camp/camp-icon-meanings/               (page key "camp")
        /bnb/specials-stats/special-stat-guides/awareness-bar-meaning/  (page key "awareness")
Output: dist/icon_meanings.json      Renderer: assets/df-bnb-icon-meanings.js

The icon art comes from the game's interface files (hudmenu.swf,
newpipboy_statspage.swf, mapmarkerslibrary.swf, vendingiconlibrary.swf),
rendered to .avif by Claude (Oct 2026) and uploaded by Duchess to
  /wp-content/uploads/guide-images/data-mining/hud-icon-meanings/
  /wp-content/uploads/guide-images/camp/camp-icon-meanings/

The words are hand-written below in plain player language (Duchess's rule:
no record names on guide pages). The only thing read from the exports is the
"Examples" list: every effect / item in the newest KYWD_Export_*_Refs.tsv that
carries the icon's keyword, so new chems, mutations, diseases etc. show up on
their own after a patch.

    python src/build_icon_meanings_json.py
"""
import csv, datetime, glob, json, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
TSV = os.path.join(REPO, "tsv")
OUT = os.path.join(REPO, "dist", "icon_meanings.json")

HUD_IMG = "/wp-content/uploads/guide-images/data-mining/hud-icon-meanings/"
CAMP_IMG = "/wp-content/uploads/guide-images/camp/camp-icon-meanings/"
AWR_IMG = "/wp-content/uploads/guide-images/specials-&-stats/awareness-bar-meaning/"

# --------------------------------------------------------------------------
# Examples from the keyword references
# --------------------------------------------------------------------------
def newest(pattern):
    # newest by the date in the file name (tsv_source): in CI every file shares the
    # checkout time, so an mtime sort picks at random
    import tsv_source
    return tsv_source.newest(pattern, required=False)

SKIP_NAME = re.compile(r"^(zzz|cut_|test|del_)|testsmoke|ui text dummy|\bspell\b|^npc |\bself$|^\s*$", re.I)
CAMEL = re.compile(r"^[A-Z][a-z]+[A-Z][A-Za-z]*$|^(?=[A-Z]*\d)[A-Z0-9]{3,5} ")
SKIP_EDID = re.compile(r"^(zzz_|CUT_|Test|DEL_|.*BACKUP$|.*_Babylon)", re.I)
KEEP_SIG = {"SPEL", "ALCH", "MGEF", "PERK"}

def load_refs():
    path = newest("KYWD_Export_*_Refs.tsv")
    refs = {}
    if not path:
        print("WARNING: no KYWD_Export_*_Refs.tsv - examples left empty")
        return refs, None
    with open(path, encoding="utf-8", errors="replace", newline="") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            if row.get("RefSignature") not in KEEP_SIG:
                continue
            name = (row.get("RefName") or "").replace("Â¢", "").replace("¢", "").strip()
            edid = row.get("RefEDID") or ""
            if SKIP_NAME.search(name) or CAMEL.search(name) or SKIP_EDID.match(edid) or "_" in name:
                continue
            refs.setdefault(row["KeywordEDID"], []).append(name)
    return refs, os.path.basename(path)

def examples(refs, *keywords, drop=(), limit=400):
    seen, out = set(), []
    for kw in keywords:
        for n in refs.get(kw, []):
            n = re.sub(r"\s+\d+$", "", n) if re.search(r"(Tales|Life|Grognak|Ops\.|Backwoodsman|Today|Bullets|Hills|Unstoppables|Live & Love|Astoundingly Awesome|Taboo Tattoos|Tesla Science|Guns and Bullets|Wasteland Survival Guide|Lad|Weekly)\s+\d+$", n) else n
            k = n.lower()
            if k in seen or any(d.lower() == k for d in drop):
                continue
            seen.add(k)
            out.append(n)
    return sorted(out, key=str.lower)[:limit]

# --------------------------------------------------------------------------
# Content
# --------------------------------------------------------------------------
def row(id_, name, img, pill, meaning, when=None, ex=None, extra_imgs=None, base=HUD_IMG):
    images = [{"src": base + img + ".avif", "label": ""}]
    for src, label in (extra_imgs or []):
        images.append({"src": base + src + ".avif", "label": label})
    if extra_imgs:
        images[0]["label"] = "Usual look"
    return {"id": id_, "name": name, "images": images, "pill": pill,
            "meaning": meaning, "when": when or [], "examples": ex or []}

def hud_page(refs):
    E = lambda *k, **kw: examples(refs, *k, **kw)
    status = [
        row("chems", "Chems & Buffs", "chems-and-buffs", "Buff",
            "The game's catch-all buff icon. Most timed boosts show this pill: chems, RadAway and Rad-X, "
            "magazines, bobbleheads, Well Rested and other bonus effects.",
            ["A chem, magazine, bobblehead or other buff is running on you.",
             "The square fills up as the effect runs out, then the icon disappears.",
             "A few harmful effects use the same pill in red."],
            E("SURV_Icon_Chems", "ObjectTypeSerum"),
            [("chems-and-buffs-red", "Red (hurting you)")]),
        row("food", "Food", "food", "Buff",
            "You have a buff from something you ate.",
            ["Shows after eating food that gives a bonus (Carry Weight, XP, damage and so on).",
             "Plain food that only fills your hunger bar does not leave an icon."],
            E("ObjectTypeFood", "ObjectTypeCanSpoil")),
        row("drink", "Drink", "drink", "Buff",
            "You have an effect from water, milk or another plain drink.",
            ["Shows after drinking water, milk or a drink with a lasting bonus."],
            E("DrinkIconGeneric")),
        row("nuka-cola", "Nuka-Cola", "nuka-cola", "Buff",
            "You have a buff from a Nuka-Cola drink.",
            ["Shows while a Nuka-Cola flavour's bonus is running."],
            E("DrinkTypeSodaIcon")),
        row("tea", "Tea & Juice", "tea", "Buff",
            "You have a buff from a herbal tea or a juice.",
            ["Shows while a tea or juice bonus is running."],
            E("DrinkTypeTeaIcon")),
        row("alcohol", "Alcohol", "alcohol", "Buff",
            "You have a buff from an alcoholic drink (beer, spirits, cocktails, moonshine).",
            ["Shows while the drink's bonus is running.",
             "Party Boy / Party Girl makes these effects stronger."],
            E("DrinkTypeAlcohol")),
        row("stimpak", "Stimpak", "stimpak", "Buff",
            "You're healing from a Stimpak.",
            ["Shows for the few seconds a Stimpak is still restoring health."],
            E("ObjectTypeStimpak")),
        row("disease", "Disease", "disease", "Debuff",
            "You've caught a disease. Each disease has its own penalty.",
            ["Shows until the disease wears off or you cure it (Disease Cure, Antibiotics, or a herbal remedy).",
             "You can catch diseases from swimming in dirty water, eating raw food, sleeping on the floor and from some enemies' attacks."],
            E("SURV_Icon_Disease", drop=("Plague Walker Damage Aura",))),
        row("mutation", "Mutation", "mutation", "Buff or Debuff",
            "You have a mutation. One icon shows for each mutation you have.",
            ["Mutations stay until RadAway removes them. The Starched Genes perk stops RadAway removing them.",
             "Shown in cream; some mutation side-effects show in red."],
            E("SURV_Icon_Mutation"),
            [("mutation-red", "Red (side-effect)")]),
        row("radiation", "Radiation", "radiation", "Debuff",
            "You're standing in radiation and taking rads.",
            ["Shows while you're in a radiation zone, in irradiated water or in a nuke zone.",
             "Goes away when you leave the area."],
            E("SURV_Icon_Radiation")),
        row("encumbered", "Encumbered / Slowed", "encumbered", "Debuff",
            "You're slowed down: either you're carrying too much, or you've been chilled by cryo.",
            ["Over your carry weight: you walk slowly, running drains Action Points and you can't fast travel.",
             "Hit by cryo (Chilled, Frosted, Frozen): you move slower until it wears off."],
            E("SURV_Icon_Encumbered", drop=("Running drains AP",))),
        row("hunger", "Hunger", "hunger", "Debuff",
            "You're hungry.",
            ["Shows when your hunger bar is low. Being well fed shows no icon.",
             "Eat something to make it go away."]),
        row("thirst", "Thirst", "thirst", "Debuff",
            "You're thirsty.",
            ["Shows when your thirst bar is low. Being hydrated shows no icon.",
             "Drink water or another drink to make it go away."]),
        row("feral", "Feral (Ghouls)", "feral", "Debuff",
            "Ghoul characters only: you're turning feral.",
            ["Shows as your Feral meter builds up. Taking in radiation (irradiated food and drink, Glowing items) brings it back down."],
            E("GHL_SURV_Icon_Feral", "SURV_Icon_Hunger", drop=("Hunger",))),
        row("stealth-boy", "Invisible", "stealth-boy", "Buff",
            "You're invisible (cloaked).",
            ["Shows while a Stealth Boy is running.",
             "Also shows for Chameleon armour, the Chameleon mutation, and legendary effects that cloak you when you're hit or attack."],
            E("SURV_Icon_StealthBoy")),
        row("tracking-dart", "Tracking Dart", "tracking-dart", "Debuff",
            "You've been tagged by a Tracking Dart.",
            ["Shows while the Tracking Dart effect is on you."],
            E("SURV_Icon_TrackingDart")),
        row("generic-effect", "Hazard", "generic-effect", "Debuff",
            "You're standing in a hazard that's hurting you, like live electricity or a gas cloud.",
            ["Shows while you're in electrified water or near live wires (Light, Strong and Fatal Electricity).",
             "Crackler grenades and some boss fights leave electric hazards that show it too.",
             "Hallucinogenic gas shows it in orange."],
            E("SURV_Icon_GenericEffect", "SURV_Icon_ColorOrange")),
        row("bleed", "Bleed", "bleed", "Debuff",
            "You're bleeding and losing health over time.",
            ["Shows while a bleed effect is on you, e.g. from a weapon with a bleed effect.",
             "Stops when the bleed runs out; a Stimpak heals the lost health."]),
        row("healing-from-item", "Retribution", "healing-from-item", "Buff",
            "You have the Retribution buff.",
            ["Shows while the Retribution bonus is running."],
            E("SURV_Icon_HealingFromItem")),
        row("candy-bucket", "Candy Bucket", "candy-bucket", "Buff",
            "You have the Candy Bucket buff from the Halloween event.",
            ["Shows while the Candy Bucket effect is running during the Halloween event."]),
        row("perk", "Perk Effect", "perk", "Buff",
            "A perk card or legendary perk has kicked in.",
            ["Shows when a perk gives you a timed effect: Inspirational, Magnetic Personality, "
             "Four Leaf Clover and many more.",
             "Some foods and event items that copy a perk's effect use it too."],
            E("Icon_Perk")),
        row("adrenaline", "Adrenaline", "adrenaline", "Stacks",
            "Your Adrenaline stacks: extra damage for each kill in a row.",
            ["Each kill adds a stack (the number in the little box beside the icon).",
             "The stacks drop off if you go too long without a kill."],
            ["Adrenaline (perk card)", "Battle Charged (battle banner)"]),
        row("onslaught", "Onslaught", "onslaught", "Stacks",
            "Your Onslaught stacks: extra damage for each hit in a row on the same target.",
            ["Comes from the Furious and Pounder's legendary effects.",
             "The number beside the icon is how many stacks you have."],
            ["Furious (legendary effect)", "Pounder's (legendary effect)", "Fortify Onslaught"]),
        row("secondary-charge-up", "Charge Up", "secondary-charge-up", "Stacks",
            "A stacking charge-up effect, shown with a stack number.",
            ["Builds up as you keep attacking, then counts down when you stop."]),
        row("rage", "Rage", "rage", "Stacks",
            "A stacking rage effect, shown with a stack number.",
            ["Builds up in combat and counts down when it wears off."]),
        row("xp-boost", "XP Boost", "xp-boost", "Buff",
            "You're earning bonus XP.",
            ["Shows while an XP bonus effect is running."]),
        row("sleep", "Sleeping", "sleep", "Info",
            "You're resting.",
            ["Shows while you're sleeping in a bed or sleeping bag."]),
        row("emt", "E.M.T.", "emt", "Cooldown",
            "The E.M.T. perk is on cooldown.",
            ["Shows after E.M.T. triggers (healing a teammate you revive); it can't trigger again until the icon goes."],
            E("Icon_EMT")),
        row("life-saver", "Life Saver", "life-saver", "Cooldown",
            "The Life Saver perk is on cooldown.",
            ["Shows after Life Saver triggers; it can't trigger again until the icon goes."],
            E("Icon_LifeSaver")),
        row("power-armor-reboot", "Power Armor Reboot", "power-armor-reboot", "Cooldown",
            "The Power Armor Reboot legendary perk is on cooldown.",
            ["Shows after Power Armor Reboot brings you back up; it can't trigger again until the icon goes."],
            E("Icon_PowerArmorReboot")),
        row("scouts-courage", "Scout's Courage", "scouts-courage", "Cooldown",
            "Your battle banner's revive is on cooldown.",
            ["Shows after the banner's revive effect has been used; it can't trigger again until the icon goes."],
            E("Icon_ScoutsCourage")),
    ]
    pets = [
        row("pet-buff-cat", "Cat Pet Buffs", "pet-buff-cat", "Buff",
            "A buff from your Cat world pet.", ["Shows in the Pip-Boy Effects list while your cat's buffs are active."],
            E("WorldPets_UI_Icon_CatBuff")),
        row("pet-buff-dog", "Dog Pet Buffs", "pet-buff-dog", "Buff",
            "A buff from your Dog world pet.", ["Shows in the Pip-Boy Effects list while your dog's buffs are active."],
            E("WorldPets_UI_Icon_DogBuff")),
        row("pet-buff-deathclaw", "Deathclaw Pet Buffs", "pet-buff-deathclaw", "Buff",
            "A buff from your Deathclaw world pet.", ["Shows in the Pip-Boy Effects list while your deathclaw's buffs are active."],
            E("WorldPets_UI_Icon_DeathclawBuff")),
        row("pet-buff-radhog", "Radhog Pet Buffs", "pet-buff-radhog", "Buff",
            "A buff from your Radhog world pet. The pet Well Rested buff uses this icon too.",
            ["Shows in the Pip-Boy Effects list while your radhog's buffs are active."],
            E("WorldPets_UI_Icon_RadhogBuff", "WorldPets_UI_Icon_WellRestedBuff")),
        row("pet-prowess", "Pet Prowess", "pet-prowess", "Buff",
            "The combined buff your world pet gives you.", ["Shows in the Pip-Boy Effects list while your pet's buffs are active."],
            E("WorldPets_UI_Icon_PetProwess")),
    ]
    team = [
        row("teammate-downed", "Teammate Downed", "teammate-downed", "Info",
            "This teammate is down and needs reviving.",
            ["Shows on the teammate's name plate while they're down but not dead."]),
        row("teammate-needs-revive", "Teammate Calling for Help", "teammate-needs-revive", "Info",
            "This teammate is down and asking for a revive or a heal.",
            ["The heartbeat line pulses on their name plate. Go to them and revive them before they bleed out."]),
        row("teammate-dead", "Teammate Dead", "teammate-dead", "Info",
            "This teammate has died and is waiting to respawn.",
            ["Shows on the teammate's name plate until they respawn."]),
    ]
    return {
        "title": "Data Mining - HUD Icon Meanings",
        "blurb": "What every icon on your HUD means: the effect squares by your health bar, the icons in "
                 "the Pip-Boy Effects list and the icons on your teammates. Enemy icons (skulls, resistances "
                 "and damage-over-time triangles) are on the Awareness Bar Meaning page. "
                 "Tap an icon to see what it means and when it shows.",
        "groups": [
            {"key": "status", "title": "Status Effect Icons",
             "blurb": "The little squares in the bottom right of your HUD. A cream square is helping you, a red square "
                      "is hurting you. The square slowly fills as the effect runs out, and a number in the small box "
                      "beside it is how many stacks you have. The same icons show in the Pip-Boy under Stat > Effects.",
             "rows": status},
            {"key": "pets", "title": "World Pet Buff Icons",
             "blurb": "These only show in the Pip-Boy under Stat > Effects, not on the HUD.",
             "rows": pets},
            {"key": "team", "title": "Teammate Icons",
             "blurb": "Icons on your teammates' name plates.",
             "rows": team},
        ],
    }


# --------------------------------------------------------------------------
# Awareness Bar (enemy icons) - the V.A.T.S. resistance bar (vatsmenu.swf)
# --------------------------------------------------------------------------
SKULL_SKIP = re.compile(r"^(zzz|DEL_Template|HTO_Template|CUT_|Test)", re.I)

def newest_main_npc():
    """The newest NPC_Export main file (not _PRPS / _Refs)."""
    import tsv_source
    return tsv_source.newest("NPC_Export_*.tsv", exclude=("_PRPS", "_Refs"), required=False)


def encounter_skulls():
    """Enemies the game gives a fixed Encounter Skull (NPC PRPS EncounterSkullIndex 1-3)."""
    prps, npc = newest("NPC_Export_*_PRPS.tsv"), None
    npc = newest_main_npc()
    out = {1: [], 2: [], 3: []}
    if not prps or not npc:
        return out
    names = {}
    with open(npc, encoding="utf-8", errors="replace", newline="") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            names[r["EDID"]] = (r.get("FULL") or "").strip()
    with open(prps, encoding="utf-8", errors="replace", newline="") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if r.get("ActorValue_EDID") != "EncounterSkullIndex":
                continue
            try:
                tier = int(float(r["Value"]))
            except ValueError:
                continue
            nm = names.get(r["NPC_EDID"], "")
            if tier in out and nm and not SKULL_SKIP.match(r["NPC_EDID"]) and not re.match(r"^[A-Z0-9]{2,5} ", nm) and nm not in out[tier]:
                out[tier].append(nm)
    return {k: sorted(v, key=str.lower) for k, v in out.items()}

def awareness_page():
    A = lambda *a, **k: row(*a, base=AWR_IMG, **k)
    sk = encounter_skulls()
    how = [
        A("bar", "The Awareness Bar", "awareness-bar-example", "Info",
          "When you target an enemy in V.A.T.S., the bar under its name shows how tough it is and how well it "
          "resists each type of damage.",
          ["You need at least 10 Perception. With less, the resistance row doesn't show.",
           "The Awareness perk card no longer does this - it now gives extra V.A.T.S. accuracy based on your Perception.",
           "Left to right: the enemy's level (encounter enemies show a skull instead), then Physical, Energy, "
           "Radiation, Fire, Cryo and Poison.",
           "The higher a number, the more of that damage type the enemy shrugs off.",
           "A crossed-out circle instead of a number means the enemy is immune to that damage type.",
           "To hit hardest, use the damage type with the lowest number and avoid the immune ones."]),
    ]
    res = [
        A("resist-physical", "Physical", "resist-physical", "Resistance",
          "How well the enemy resists physical damage: bullets, melee hits, arrows and explosions."),
        A("resist-energy", "Energy", "resist-energy", "Resistance",
          "How well the enemy resists energy damage: laser, plasma, Tesla and other energy weapons."),
        A("resist-radiation", "Radiation", "resist-radiation", "Resistance",
          "How well the enemy resists radiation damage, e.g. from a Gamma Gun or irradiated weapons."),
        A("resist-fire", "Fire", "resist-fire", "Resistance",
          "How well the enemy resists fire damage: flamers, incendiary weapons, Molotovs and burn effects."),
        A("resist-cryo", "Cryo", "resist-cryo", "Resistance",
          "How well the enemy resists cryo damage: the Cryolator, cryo grenades and mines, and cryo effects."),
        A("resist-poison", "Poison", "resist-poison", "Resistance",
          "How well the enemy resists poison damage: poisoned weapons and poison effects."),
        A("resist-immune", "Immune", "resist-immune", "Resistance",
          "Shown instead of a number when the enemy is immune to that damage type - it does nothing to them.",
          ["Robots are usually immune to poison and radiation; many bosses are immune to radiation or poison too."]),
    ]
    enemy = [
        A("enemy-skull", "Encounter Skull", "enemy-skull-1", "Enemy",
          "Shown instead of the level number for encounter enemies (Raids, Infestations, Daily Ops and some events). "
          "The fancier the skull, the more important the enemy.",
          ["Plain skull: a standard or support enemy in the encounter.",
           "Skull with a crown: a tougher elite or mini-boss.",
           "Skull with a spiked crown and flames: the boss of the encounter.",
           "Infestations: support enemies get the plain skull, the mob the crowned skull, the boss the spiked crown. "
           "Daily Ops: the skull follows the difficulty (Medium = crowned, Hard = spiked crown)."],
          ["Boss: " + n for n in sk.get(3, [])] + ["Elite: " + n for n in sk.get(2, [])] + ["Standard: " + n for n in sk.get(1, [])],
          [("enemy-skull-2", "Elite / mini-boss"), ("enemy-skull-3", "Boss")]),
        A("enemy-target", "Encounter Target", "enemy-target-1", "Enemy",
          "A crosshair version of the encounter icon, used to mark a target enemy. Like the skull, it comes in "
          "three levels: plain, one set of flames, two sets of flames.",
          None, None, [("enemy-target-2", "Tougher"), ("enemy-target-3", "Toughest")]),
    ]
    dot = [
        A("dot-fire", "Burning", "dot-fire", "Damage over time",
            "The enemy is on fire and taking fire damage over time.",
            ["Shows beside the enemy's health bar. The triangle drains as the burn runs out."]),
        A("dot-poison", "Poisoned", "dot-poison", "Damage over time",
            "The enemy is poisoned and taking poison damage over time."),
        A("dot-energy", "Shocked", "dot-energy", "Damage over time",
            "The enemy is taking energy damage over time."),
        A("dot-cryo", "Chilled", "dot-cryo", "Damage over time",
            "The enemy is taking cryo damage over time (and is slowed)."),
        A("dot-radiation", "Irradiated", "dot-radiation", "Damage over time",
            "The enemy is taking radiation damage over time."),
        A("dot-bleed", "Bleeding", "dot-bleed", "Damage over time",
            "The enemy is bleeding and taking damage over time."),
    ]


    img = AWR_IMG + "awareness-bar-example.avif"
    sections = [
        {"key": "what", "title": "What Is the Awareness Bar?", "html":
         "<p>When you target an enemy in V.A.T.S., a row of icons and numbers appears under its name and health bar. "
         "This is the Awareness Bar. It tells you how tough the enemy is and how well it stands up to each type of damage, "
         "so you can pick the right weapon before you start shooting.</p>"
         "<p>You need <b>10 Perception</b> to see the resistance part of the bar.</p>"
         "<p>The <b>Awareness</b> perk card used to be what showed enemy resistances. It doesn't any more - it now gives "
         "you extra V.A.T.S. accuracy based on your Perception. The resistances come from having 10 Perception.</p>"},
        {"key": "read", "title": "Reading the Bar", "html":
         "<figure class=\"imSecFig\"><img class=\"imSecImg\" src=\"" + img + "\" alt=\"The Awareness Bar on the Ultracite Terror\" loading=\"lazy\">"
         "<figcaption>The Awareness Bar on the Ultracite Terror, the boss of the Gleaming Depths raid.</figcaption></figure>"
         "<p>Read the bar from left to right:</p>"
         "<ol>"
         "<li><b>Level or skull.</b> Most enemies show their level here. Encounter enemies (raids, Infestations, Daily Ops "
         "and some events) show a skull instead. The fancier the skull, the more important the enemy - see "
         "<i>Encounter Skull Icons</i> below.</li>"
         "<li><b>Physical</b> (shield) - bullets, melee, arrows and explosions.</li>"
         "<li><b>Energy</b> (lightning bolt) - laser, plasma, Tesla and other energy weapons.</li>"
         "<li><b>Radiation</b> (radiation symbol).</li>"
         "<li><b>Fire</b> (flame).</li>"
         "<li><b>Cryo</b> (snowflake).</li>"
         "<li><b>Poison</b> (drop).</li>"
         "</ol>"
         "<p>Each damage type has either a <b>number</b> or a <b>crossed-out circle</b>. In the picture above, the Ultracite "
         "Terror is immune to radiation and poison, and its lowest number is cryo.</p>"},
        {"key": "numbers", "title": "What the Numbers Mean", "html":
         "<p>The number is the enemy's resistance to that damage type. <b>The higher the number, the more of that damage "
         "the enemy shrugs off.</b> A low number is a weak spot.</p>"
         "<p>The number on the bar is not the same as the raw resistance stored in the game files. For example, the "
         "Ultracite Terror's physical resistance is 276 in the files, but the bar shows 183. The game works out the "
         "bar number itself. We have asked the developers for the exact formula and will add it, with a calculator, "
         "once we know.</p>"
         "<p>Even without the exact formula, the bar is easy to use: compare the numbers against each other. The lowest "
         "number is the damage type that will hurt that enemy the most.</p>"
         "<p>A <b>crossed-out circle</b> means the enemy is <b>immune</b>: that damage type does nothing to it. Robots, "
         "for example, are usually immune to poison and radiation. If your weapon only does an immune damage type, "
         "switch weapons.</p>"},
        {"key": "tips", "title": "Using It to Pick Your Damage", "html":
         "<ul>"
         "<li><b>Go for the lowest number.</b> If an enemy's energy number is much lower than its physical number, an "
         "energy weapon will do more damage than a gun of the same power.</li>"
         "<li><b>Never rely on an immune type.</b> Damage of an immune type is wasted. Weapons that mix damage types "
         "(for example plasma, or weapons with fire, cryo or poison effects) still do their other types.</li>"
         "<li><b>Big hits get through resistance better.</b> Resistance cuts a share of each hit, and that share is "
         "smaller when the hit is big compared to the resistance. Against very resistant enemies, a few big hits beat "
         "lots of small ones.</li>"
         "<li><b>Lower the enemy's resistance.</b> Perks and effects that ignore or reduce armour (such as Tank Killer, "
         "Stabilized and Incisor, or armour-piercing mods) help most against enemies with high numbers.</li>"
         "<li><b>Damage over time shows too.</b> When you set an enemy burning, poisoned, bleeding and so on, a small "
         "triangle appears beside its health bar - see <i>Damage Over Time Icons</i> below.</li>"
         "</ul>"},
        {"key": "icons", "title": "Icon Reference", "html":
         "<p>Open any icon below to see what it means.</p>"},
    ]
    return {
        "title": "SPECIALs & Stats - Awareness Bar Meaning",
        "sections": sections,
        "blurb": "What the icons and numbers under an enemy's health bar mean when you target it in V.A.T.S. "
                 "(you need 10 Perception for the resistance row), plus the damage-over-time icons that show "
                 "when you are hurting an enemy over time.",
        "groups": [
            {"key": "resist", "title": "Resistance Icons", "rows": res,
             "blurb": "One icon per damage type, each followed by the enemy's resistance number or the immune symbol."},
            {"key": "enemy", "title": "Encounter Skull Icons", "rows": enemy,
             "blurb": "Encounter enemies show one of these in place of their level, beside their name and on the bar."},
            {"key": "dot", "title": "Damage Over Time Icons", "rows": dot,
             "blurb": "Small triangles beside an enemy's health bar while you are hurting it over time. "
                      "Each triangle drains as its effect runs out."},
        ],
    }

CAMP_ICON_NAMES = {
    "BoSBase": "Brotherhood Base", "BoS": "Brotherhood of Steel", "LastCorpse": "Skull and Crossbones",
    "NukaColaQuantumPlant": "Nuka-Cola Quantum", "SancHills": "Minuteman", "TrainTrack": "Train Track",
    "TopOfTheWorld": "Top of the World", "WhitespringResort": "Whitespring Resort", "CowSpotsCreamery": "Cow Spots Creamery",
    "DefaultCamp": "Tent (default)", "HammerWing": "Hammer and Wing", "PalaceWindingPath": "Palace of the Winding Path",
    "IrishPride": "Four-Leaf Clover", "RaiderSettlement": "Raider", "RailroadFaction": "Lantern",
    "Sentinel": "Sentinel", "Libertalia": "Libertalia", "SkullRing": "Skull Ring",
}
def camp_icon_label(cls):
    base = re.sub(r"Marker$|Mark$", "", cls)
    if base in CAMP_ICON_NAMES:
        return CAMP_ICON_NAMES[base]
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", base)

def camp_slug(cls):
    s = re.sub(r"Marker$|Mark$", "", cls)
    s = re.sub(r"(?<!^)(?=[A-Z])", "-", s).lower().replace("bo-s", "bos")
    return "custom-" + s

# In-game order of the CAMP icon picker (CampBuildsShared.CAMP_SLOTS_ICON_ARRAY).
CAMP_ICON_ORDER = """DefaultCampMarker AgriculturalCenterMarker AirfieldMarker AmusementParkMarker AppalachianAntiquesMarker
ArktosPharmaMarker BloodEagleMarker BoSBaseMarker BoSMarker BunkerMarker CabinMarker CamperMarker CapitalBuildingMarker CarMarker
CastleMarker CaveMarker ChurchMarker CityMarker LastCorpseMarker CountryClubMarker CowSpotsCreameryMarker CultistMarker
CustomHouseMarker DamMarker SkyscraperMarker DriveInMarker ElectricalSubstationMarker ElevatedHighwayMarker EncampmentMarker
ExcavatorMarker FactoryMarker FarmMarker FillingStationMarker FissureMarker ForestedMarker GoodneighborMarker GraveyardMarker
HammerWingMarker HighTechBuildingMarker HospitalMarker HouseTrailerMarker IndustrialDomeMarker IrishPrideMarker JunkyardMarker
LandmarkMarker LegendaryPurveyorMarker LibertaliaMarker LighthouseMarker LookoutTowerMarker LowRiseMarker MansionMarker
MechanistMarker MetroMarker MilitaryBaseMarker MissileSiloMarker MonorailMarker MonumentMarker NukaColaQuantumPlant
ObservatoryMarker OfficeMarker OverlookMarker PalaceWindingPathMarker PierMarker PoliceStationMarker PondLakeMarker
PowerPlantMarker PumpkinMarker QuarryMarker RadioactiveAreaMarker RadioTowerMarker RaiderSettlementMarker RailroadFactionMarker
RailroadMarker SancHillsMarker SatelliteMarker SchoolMarker SentinelMarker SettlementMarker SewerMarker ShipwreckMarker
SkiResortMarker SkullRingMarker SpaceStationMarker SubmarineMarker SwanPondMarker TeapotMarker TopOfTheWorldMarker TownMarker
TownRuinsMarker TrainStationMarker TrainTrackMark UrbanRuinsMarker VaultMarker WhitespringResort WoodShackMarker""".split()

def camp_page():
    C = lambda *a, **k: row(*a, base=CAMP_IMG, **k)
    markers = [
        C("camp-active", "Your CAMP (placed)", "camp-marker-active", "Map",
          "Your CAMP, placed and active. You can fast travel to it for free.",
          ["Gold ring: this CAMP is the one currently placed in the world.",
           "The picture in the middle is the CAMP icon you picked (see Custom CAMP Map Icons below).",
           "A pulsing ring around it means your CAMP is set to public."]),
        C("camp-stored", "Your CAMP (stored)", "camp-marker-stored", "Map",
          "One of your other CAMP slots, not placed right now.",
          ["Dark ring: the CAMP is stored. Pick it from the map to place it.",
           "Teammates' CAMPs show the same gold / dark rings."]),
        C("camp-other", "Another Player's CAMP", "camp-marker-other-player", "Map",
          "A CAMP belonging to another player on your server.",
          ["White ring with that player's chosen icon inside.",
           "Small icons along the bottom tell you what they have (vending, shelter, candy bowl) - see CAMP Map Sub-Icons."]),
        C("camp-tent", "Your Survival Tent", "camp-marker-tent", "Map",
          "Your placed Survival Tent. You can fast travel to it for free.",
          ["Shows where you last placed your Survival Tent."]),
        C("camp-tent-placed", "Survival Tent Marker", "camp-marker-tent-placed", "Map",
          "A placed Survival Tent marker on the map.",
          ["Flag and campfire marking a placed tent."]),
        C("camp-best-builds", "Best Builds CAMP", "camp-marker-best-builds", "Map",
          "A CAMP featured in Best Builds. You can visit it from the map.",
          ["Blue rosette with the builder's chosen icon inside."]),
        C("workshop-public", "Public Workshop", "workshop-marker-public", "Map",
          "A public workshop nobody has claimed.", ["Claim it to build and collect its resources."]),
        C("workshop-owned", "Public Workshop (claimed)", "workshop-marker-owned", "Map",
          "A public workshop that has been claimed.", ["The dashed gold ring shows it is owned."]),
    ]
    subs = [
        C("sub-vending", "Vending", "camp-sub-vending", "Map",
          "This CAMP has a vending machine with items for sale.",
          ["Bright green V: the CAMP is placed and you can go and shop.",
           "Dark V: the CAMP has vending but isn't placed right now."],
          None, [("camp-sub-vending-off", "CAMP not placed")]),
        C("sub-shelter", "Shelter", "camp-sub-shelter", "Map",
          "This CAMP has a Shelter you can visit.",
          ["Red: the CAMP is placed and the Shelter can be visited.",
           "Dark: the CAMP isn't placed right now."],
          None, [("camp-sub-shelter-off", "CAMP not placed")]),
        C("sub-candy-bowl", "Candy Bowl", "camp-sub-candy-bowl", "Map",
          "This CAMP has a Halloween candy bowl out for trick-or-treaters.",
          ["Orange pumpkin: the CAMP is placed and you can trick-or-treat there.",
           "Dark: the CAMP isn't placed right now."],
          None, [("camp-sub-candy-bowl-off", "CAMP not placed")]),
    ]
    machines = [
        C("machine-vendor", "Vending Machine", "machine-vendor", "Menu",
          "A vending machine. Items put in it are for sale to other players.",
          ["Shows at the top of the menu when you open a vending machine, and on vending items in the trade lists."]),
        C("machine-display", "Display Case & Mannequin", "machine-display", "Menu",
          "A display case, rack or mannequin. Items put in it are shown off, not sold.",
          ["Shows on display cases (weapons, armour, food, books and more) and on mannequins / armour racks."]),
        C("machine-fermenter", "Fermenter", "machine-fermenter", "Menu",
          "A fermenter. Put in fermentable drinks to age them into stronger versions."),
        C("machine-fridge", "Fridge & Freezer", "machine-fridge", "Menu",
          "A fridge or freezer. Food and drinks put in it spoil more slowly."),
        C("machine-recharger", "Recharger", "machine-recharger", "Menu",
          "A recharger that charges items placed in it."),
        C("machine-dispenser", "Dispenser", "machine-dispenser", "Menu",
          "A CAMP dispenser that hands out its contents to other players.",
          ["Used by the aid, ammo and junk dispensers."]),
        C("machine-ally", "Ally", "machine-ally", "Menu",
          "Your CAMP ally's inventory.", ["Shows when you trade with an ally living at your CAMP."]),
        C("machine-pet", "Pet", "machine-pet", "Menu",
          "Your CAMP pet's inventory.", ["Shows when you trade with a pet living at your CAMP."]),
    ]
    objects = [
        C("object-repair", "Repairing", "camp-object-repair", "HUD",
          "This CAMP object is repairing itself.",
          ["Shows on a damaged CAMP object's health bar while it is being repaired automatically.",
           "Looking at a damaged object without the wrench means you'll need to repair it yourself."]),
    ]
    custom_imgs = [{"src": CAMP_IMG + camp_slug(c) + ".avif", "label": camp_icon_label(c)} for c in CAMP_ICON_ORDER]
    custom = [{
        "id": "custom-icons", "name": "Custom CAMP Map Icons", "pill": "Map", "gallery": True,
        "images": custom_imgs,
        "meaning": "The picture in the middle of your CAMP's map marker. You pick it yourself for each CAMP slot.",
        "when": ["Open the map, select your CAMP and choose Change Icon (or do it from the CAMP slots menu).",
                 "There are %d icons to pick from, listed below in the same order as the game." % len(custom_imgs),
                 "Other players see your choice inside the white ring on their map."],
        "examples": [],
    }]
    return {
        "title": "CAMP - CAMP Icon Meanings",
        "blurb": "What the CAMP icons mean: the CAMP markers on your map, the little icons that show what a CAMP "
                 "has, the icons on CAMP machines, and every icon you can pick for your own CAMP.",
        "groups": [
            {"key": "markers", "title": "CAMP Map Markers", "rows": markers,
             "blurb": "The main markers you'll see for CAMPs and workshops on the map."},
            {"key": "subs", "title": "CAMP Map Sub-Icons", "rows": subs,
             "blurb": "Small icons along the bottom of a CAMP marker that tell you what the CAMP has. "
                      "Bright means the CAMP is placed; dark means it isn't."},
            {"key": "machines", "title": "CAMP Machine Icons", "rows": machines,
             "blurb": "Shown at the top of the menu when you open one of these CAMP items, and beside "
                      "items in vending and trade lists."},
            {"key": "objects", "title": "CAMP Object Icons", "rows": objects,
             "blurb": "Icons on your CAMP objects' health bars."},
            {"key": "custom", "title": "Custom CAMP Map Icons", "rows": custom,
             "blurb": "Every icon you can choose for your CAMP's map marker."},
        ],
    }

def main():
    refs, src = load_refs()
    data = {"version": 1,
            "generated": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": src,
            "pages": {"hud": hud_page(refs), "camp": camp_page(), "awareness": awareness_page()}}
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=1)
    for k, p in data["pages"].items():
        n = sum(len(g["rows"]) for g in p["groups"])
        print("%s: %d groups, %d rows" % (k, len(p["groups"]), n))
    print("Wrote", OUT)

if __name__ == "__main__":
    main()
