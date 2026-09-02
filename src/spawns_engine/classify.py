#!/usr/bin/env python3
r"""
spawns_engine.classify — source_type classifiers for the DF/BNB "{Item} Spawn
Locations" pipeline.

classify() is intentionally PLUGGABLE per item family: the structural engine
(closure walk, geo, region grouping) is shared, but the EDID-keyword vocabulary
that routes a placed source into a renderer expand differs slightly between
families (drinks have ice/soda machines + mystery machines + collectron pods;
the farming/egg sets have nests + dispensers). Each family passes its own
classifier into engine.sources.get_sources, so outputs stay byte-identical to
the pre-refactor per-category builds.

Both classifiers are EDID-keyword driven — NO hardcoded FormIDs.
"""


def nuka_classify(sig, edid, via_edid):
    """Nuka-Cola / drink 12-case router (spawn-guide skill §9k).

    Buckets -> expand (routed in df-bnb-spawns.js):
      direct / machine      -> Fixed Spawn Locations (guaranteed world source)
      container / loot-list  -> Containers            (chance loot)
      npc                    -> Creatures             (chance death drop)
      vendor                 -> Vendors               (merchant stock; incl. dedicated)
      collectron             -> Collectrons           (ATX/Season, no map coords)
      resource-generator     -> Resource Generators   (ATX dispenser, no map coords)
      quest-reward           -> excluded from the map (guaranteed but not a location)

    Order matters: check the most specific EDID keyword first. A vendor's stock
    chest can also read as a "vending machine" (e.g. NWOT_VendingMachine_
    VendorChest_NukaCola) — VendorChest wins so dedicated vendors are never
    mislabelled as machines.
    """
    e = (edid or "").lower() + " " + (via_edid or "").lower()
    if "collectron" in e:
        return "collectron"
    if "vendorchest" in e:
        return "vendor"
    if "mysterymachine" in e:
        return "resource-generator"
    if "icemachine" in e or "sodamachine" in e:
        return "machine"
    if "vendingmachine" in e or "dispenser" in e:
        return "resource-generator"
    if "vendor" in e:
        return "vendor"
    if "questreward" in e or "quest_reward" in e or "_reward" in e:
        return "quest-reward"
    if sig == "NPC_":
        return "npc"
    if sig == "REFR":
        return "direct"
    if sig == "CONT":
        return "container"
    return "loot-list"


def weapon_classify(sig, edid, via_edid):
    """Weapon router (the Chainsaw family, and any WEAP page that follows it).

    A weapon is never a nest, a dispenser or a harvestable, so the vocabulary is the
    drink router's minus those cases — and it keeps the drink router's ordering rule:
    the most specific keyword first, VendorChest beating the VendingMachine token so a
    merchant chest is never mislabelled as a machine.

    The one weapon-specific case is the world **display rack / weapon rack** (EDID
    carries `WeaponRack` / `GunRack`): a rack is a fixed world point that hands you the
    weapon, so it belongs in Fixed Spawn Locations alongside `direct`, not in
    Containers. Everything else routes exactly as the drink router does.
    """
    e = (edid or "").lower() + " " + (via_edid or "").lower()
    if "collectron" in e:
        return "collectron"
    if "vendorchest" in e:
        return "vendor"
    if "mysterymachine" in e:
        return "resource-generator"
    if "weaponrack" in e or "gunrack" in e:
        return "machine"          # a fixed, always-there world point (Fixed Spawn)
    if "vendingmachine" in e or "dispenser" in e:
        return "resource-generator"
    if "vendor" in e:
        return "vendor"
    if "questreward" in e or "quest_reward" in e or "_reward" in e:
        return "quest-reward"
    if sig == "NPC_":
        return "npc"
    if sig == "REFR":
        return "direct"
    if sig == "CONT":
        return "container"
    return "loot-list"


def farming_classify(sig, edid, via_edid):
    """Farming / egg router (Cream + the egg sets). Keeps the exact buckets the
    farming pipeline shipped: collectron/slowroaster, vendor, nest, dispenser,
    quest-reward, npc, direct, container, loot-list."""
    e = (edid or "").lower() + " " + (via_edid or "").lower()
    if "collectron" in e or "slowroaster" in e:
        return "collectron"
    if "vend" in e or "vendor" in e:
        return "vendor"
    if "nest" in e:
        return "nest"
    if "dispenser" in e:
        return "dispenser"
    if "questreward" in e or "quest_reward" in e or "_reward" in e:
        return "quest-reward"
    if sig == "NPC_":
        return "npc"
    if sig == "REFR":
        return "direct"
    if sig == "CONT":
        return "container"
    return "loot-list"
