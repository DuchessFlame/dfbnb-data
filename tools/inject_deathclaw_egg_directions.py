#!/usr/bin/env python3
r"""
inject_deathclaw_egg_directions.py — one-off: port the DF farming-guide directions
onto the BNB Deathclaw Egg fixed-spawn markers.

The directions are hand-authored content (like photos), so they live in the dist
JSON and are preserved across rebuilds by spawns_engine.build.load_existing:
  - MULTI-spawn markers  -> marker-level `directions` (renders as "Getting there"),
                            preserved by (region, marker).
  - SINGLE-spawn markers -> the one spawn's `directions` (renders via the single
                            layout's fallback), preserved by ref (survives the
                            Excelsior -> Ella Ames' Bunker rename).

Idempotent: re-running overwrites the same fields with the same text. Applies to
dist/farming_spawns/ and dist/pts/farming_spawns/ if present.

Photos are intentionally left blank — the user moves those over separately.
"""
import json, os, sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Marker-level "Getting there" directions for the MULTI-spawn markers.
MARKER_DIRECTIONS = {
    "Tunnel of Love": (
        "These locations are accessible whether the Nuka-World on Tour event is "
        "running or not. After entering the Tunnel of Love, face north and follow "
        "the train tracks until you reach the area with the pink heart lights. "
        "Turn left, pass the orange forklift, and stop at the second set of "
        "tracks. Turn left again and walk forward into a small cave — there are "
        "three nests in the pile on your right. From there, turn around and follow "
        "the tracks to the end. Turn right and walk along the road wall until you "
        "reach some scaffolding with a wooden ramp leading up. Follow the ramp to "
        "the top, turn left, and follow the path until you see a break in the "
        "fence and a small cave on your left — inside is one more nest. Five nests "
        "spawn in this instance in total; check each of the numbered nest blocks "
        "below."
    ),
    "Deathclaw Island": (
        "Check the white suitcase on the side of the Deathclaw nest — it can "
        "sometimes spawn additional Deathclaw eggs. The eggs can be hidden under "
        "the dirt when the Deathclaw pops out, so lead the Deathclaw away from the "
        "nest before killing it, so you can loot the nest without the body getting "
        "in the way or covering the loot."
    ),
    "Hopewell Cave": (
        "From the fast travel point, run straight across the road and enter the "
        "cave. The first Deathclaw nest is on the left-hand side of the cave, just "
        "past the small pond of water near the entrance — a Deathclaw Egg can "
        "spawn on top of this nest. From the nest, face east and walk to the back "
        "of the cave until you reach a rock face, then turn left; a Deathclaw Egg "
        "can spawn behind the Firecracker Berry bush. Next, face west and follow "
        "the edge of the pond as it curves around the cave until you see a small "
        "campfire and a sleeping bag. Behind the sleeping bag, against the rock "
        "face and to the left of a Firecracker Berry bush, is the final Deathclaw "
        "Egg spawn for this area."
    ),
    "Abandoned Waste Dump": (
        "External approach. From the fast travel point, walk forward and jump over "
        "the gate. Head toward the back of the truck, keeping the trailer on your "
        "left. On your right you'll see a dead brahmin and a radiation barrel — "
        "the first Deathclaw nest is behind the brahmin. Continue following the "
        "trailer until you reach the cab of the truck; in front of you is a dead "
        "Yao Guai, and behind it is a Deathclaw nest and a Deathclaw egg."
    ),
    "Cavern": (
        "This is the internal cavern beneath the Abandoned Waste Dump (it requires "
        "a load screen to enter). Once inside, walk past the rad barrels and stop "
        "at the bottom of the ramp. Turn left and keep the wall on your left-hand "
        "side the whole time you are in the cavern — it will lead you to the three "
        "Deathclaw nests in turn. The last Deathclaw nest has a fixed-spawn "
        "Deathclaw egg next to it."
    ),
    "Dino Peaks Mini Golf": (
        "From the spawn-in location, face north and run forward, stopping just "
        "past the Welcome to Dino Peak entrance sign. Turn left and walk to the "
        "Hole 1 sign, then line yourself up with the Hole 1 putting green and "
        "follow it to the end. The nests run in sequence along the course (Holes "
        "1–6): several sit on top of the putting greens and stairs and can be "
        "partly covered by dirt depending on how the Deathclaw exits, so wave your "
        "cursor around to find the loot window. A Deathclaw Egg can also spawn on "
        "top of several of these nests."
    ),
}

# Single-spawn markers -> directions applied to that marker's ONE spawn.
SPAWN_DIRECTIONS = {
    "Ash Cave": (
        "From the spawn-in location, line yourself up with the Ash Cave map marker "
        "and run straight up the hill. Continue forward and stop at the top of the "
        "stairs. Veer right and follow the path straight (north-ish) until you "
        "reach the pond. Turn right and enter the small cave — the Deathclaw Egg "
        "is against the north wall, behind the Brain Fungus and near the "
        "bobblehead spawn."
    ),
    "The Rust Kingdom": (
        "From the spawn-in location, face southwest and run straight until you "
        "reach the cliff wall in the distance. At the cliff face, turn left and "
        "use the machinery in front of you to climb to the rock ledge above. On "
        "the ledge, face north — the Deathclaw Egg is directly in front of you."
    ),
    "Arktos Pharma": (
        "From the fast travel point, run straight through the doorway to the "
        "bottom of the stairs. Turn left, pass the Nuka-Cola and Medical Supplies "
        "machines, and run up the stairs in front of you. Follow the corridor "
        "around, pass the Arktos Pharma sign, and enter the doorway on your left. "
        "Continue straight and enter the second doorway on your left. Head "
        "slightly right and walk through the gap in the wall directly opposite. "
        "Jump down to ground level, turn around, and enter the doorway marked "
        "Protein Sequencing. Turn right and walk down the stairs — the Deathclaw "
        "Egg is on the metal shelves in front of you."
    ),
    "Monorail Elevator": (
        "Note: you need the Marsupial mutation to reach this spot. From the fast "
        "travel point, follow the road until you reach the main pillar with the "
        "elevator at the bottom. Enter the elevator and select floor 2 (pressing "
        "the wrong floor takes you elsewhere). Exit and follow the path to your "
        "left, continuing until you reach a building. Jump onto the roof and "
        "follow it around, keeping to the left and away from the edge — the "
        "Deathclaw Egg is in the far corner as part of a picnic scene."
    ),
    "Atrium Upper Level": (
        "Vault 63 — Atrium Upper Level. One Deathclaw Egg spawns on the atrium's "
        "upper level."
    ),
    "Thunder Mt. Power Plant Yard": (
        "From the fast travel point, follow the road south until you reach the end "
        "of the limo. Turn left, pass the tank, and enter the workshop yard. "
        "Continue straight along the road until you reach the green shipping "
        "containers near the water's edge. Jump over the railing and keep the "
        "concrete wall on your right as you follow it along. At the end of the "
        "wall, line yourself up between south and the Braxson's Quality Medical "
        "Supplies map icon, then walk forward along the riverbank. As the bank "
        "begins to curve, look to your right for the Deathclaw nest."
    ),
    "Ella Ames' Bunker": (
        "Stand at the door of Ella Ames' Bunker and look toward the blue ute in "
        "the water ahead of you. Behind the ute are four trees arranged in a loose "
        "semicircle. Starting from the left, head toward the third tree; stand "
        "behind it and look for a tree stump and a light-coloured tree with red "
        "leaves in front of you. Run to the red-leafed tree, then turn south and "
        "walk to the river's edge. Turn left and follow the riverbank — just past "
        "the curve in the bank, the Deathclaw nest is on your right."
    ),
    # Highway Town Interior — not on the DF page, but present on BNB. Include it
    # with an honest placeholder note so it is treated like every other marker.
    "Highway Town Interior": (
        "Highway Town Interior is a Burning Springs interior (it requires a load "
        "screen to enter). One loose Deathclaw Egg spawns inside. Step-by-step "
        "directions and photos are coming soon."
    ),
    # Enclave Research Facility — placed in the game files but the cell has no
    # accessible entrance in normal gameplay, so the egg can't actually be looted.
    "Enclave Research Facility": (
        "Heads-up: the Enclave Research Facility interior cannot be entered in "
        "normal gameplay — there is no accessible door or load-door into this "
        "cell. A loose Deathclaw Egg is placed here in the game files (ref "
        "5DD49E), but because the location can't be reached, the egg cannot "
        "actually be collected. It is listed here for completeness only."
    ),
}


def apply(path):
    if not os.path.exists(path):
        return None
    d = json.load(open(path, encoding="utf-8"))
    hits = {"marker": [], "spawn": []}
    for reg in d.get("regions", []):
        for loc in reg.get("locations", []):
            mk = loc.get("marker", "")
            if mk in MARKER_DIRECTIONS:
                loc["directions"] = MARKER_DIRECTIONS[mk]
                hits["marker"].append(mk)
            if mk in SPAWN_DIRECTIONS:
                spawns = loc.get("spawns") or []
                if spawns:
                    spawns[0]["directions"] = SPAWN_DIRECTIONS[mk]
                    hits["spawn"].append(mk)
                else:  # no spawns array — fall back to marker level
                    loc["directions"] = SPAWN_DIRECTIONS[mk]
                    hits["marker"].append(mk)
    json.dump(d, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    return hits


def main():
    targets = [
        os.path.join(REPO, "dist", "farming_spawns", "deathclaw-egg_spawns.json"),
        os.path.join(REPO, "dist", "pts", "farming_spawns", "deathclaw-egg_spawns.json"),
    ]
    for p in targets:
        hits = apply(p)
        if hits is None:
            print(f"  [skip] {os.path.relpath(p, REPO)} (not present)")
            continue
        print(f"  {os.path.relpath(p, REPO)}")
        print(f"    marker-level: {len(hits['marker'])}  ->  {sorted(hits['marker'])}")
        print(f"    spawn-level : {len(hits['spawn'])}  ->  {sorted(hits['spawn'])}")


if __name__ == "__main__":
    main()
